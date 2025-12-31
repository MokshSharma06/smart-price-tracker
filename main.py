from dotenv import load_dotenv
from pyspark.sql import functions as F

from src.utils import get_spark_session, adls_path
from src.logger import get_logger

from src.fetch_prices import run_flipkart_scraper
from src.fetch_price_ajio import run_ajio_scraper

from src.process_data import (
    clean_prices,
    filter_valid_data,
    add_final_mrp,
    calculate_disc,
)

from src.logic import generate_signals
from src.data_loader import write_processed_data
from src.gold_loader import write_gold_alerts


def main():
    # --------------------------------------------------
    # 1. Spark + Logger
    # --------------------------------------------------
    load_dotenv()
    spark, config = get_spark_session()
    app_logger = get_logger(spark, "smart-price-tracker")

    app_logger.info("Starting Smart Price Tracker pipeline")

    # --------------------------------------------------
    # 2. SCRAPING (IN-MEMORY ONLY)
    # --------------------------------------------------
    app_logger.info("Running Flipkart scraper")
    flipkart_df = run_flipkart_scraper(spark)

    app_logger.info("Running Ajio scraper")
    ajio_df = run_ajio_scraper(spark)

    if flipkart_df is None and ajio_df is None:
        raise Exception("No data scraped from any source")

    # --------------------------------------------------
    # 3. STAGING (UNION IN-MEMORY)
    # --------------------------------------------------
    app_logger.info("Unioning scraped data in staging")

    raw_staging_df = (
        flipkart_df.unionByName(ajio_df)
        if flipkart_df is not None and ajio_df is not None
        else flipkart_df or ajio_df
    )

    app_logger.info(f"Staging record count = {raw_staging_df.count():,}")

    # --------------------------------------------------
    # 4. CLEAN & NORMALIZE 
    # --------------------------------------------------
    app_logger.info("Cleaning and normalizing raw data")
    raw_clean_df = clean_prices(raw_staging_df)

    # Drop records where selling price could not be fetched
    invalid_price_df = raw_clean_df.filter(F.col("selling_price").isNull())
    valid_price_df = raw_clean_df.filter(F.col("selling_price").isNotNull())

    invalid_count = invalid_price_df.count()
    if invalid_count > 0:
        app_logger.warn(
            f"Dropping {invalid_count} records due to missing selling price"
        )

    raw_clean_df = valid_price_df
    app_logger.info("RAW data validation passed")

    # --------------------------------------------------
    # 5. WRITE RAW (VALIDATED ONLY)
    # --------------------------------------------------
    raw_path = adls_path("raw")
    silver_path = adls_path("processed")

    app_logger.info("Writing validated RAW data to ADLS")
    raw_clean_df.write.mode("append").json(raw_path)
    app_logger.info("RAW write completed")

    # --------------------------------------------------
    # 6. SILVER TRANSFORMATION (BUSINESS TRUTH)
    # --------------------------------------------------
    app_logger.info("Filtering valid records")
    df_filtered = filter_valid_data(raw_clean_df)

    app_logger.info("Adding final MRP")
    df_with_mrp = add_final_mrp(df_filtered)

    app_logger.info("Calculating discount percentages")
    silver_df = calculate_disc(df_with_mrp)

    app_logger.info(f"Silver record count = {silver_df.count():,}")

    app_logger.info("Writing Silver layer idempotently")
    write_processed_data(silver_df, silver_path)

    # --------------------------------------------------
    # 7. GOLD SIGNAL GENERATION (IDEMPOTENT)
    # --------------------------------------------------
    gold_path = adls_path("gold_alerts")

    app_logger.info("Generating BUY signals")
    signal_df = generate_signals(silver_df)

    signal_count = signal_df.count()

    if signal_count > 0:
        app_logger.info(f"Found {signal_count} BUY signals")
        write_gold_alerts(signal_df, gold_path)
        app_logger.info("Gold alerts written successfully")
    else:
        app_logger.info("No BUY signals generated today")

    app_logger.info("Pipeline execution finished successfully")


if __name__ == "__main__":
    main()
