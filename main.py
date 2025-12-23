from src.utils import get_spark_session, adls_path
from src.logger import get_logger
from src.process_data import write_processed_data
from src.fetch_prices import run_flipkart_scraper
from src.fetch_price_ajio import run_ajio_scraper
from src.data_loader import load_combined_data,BASE_ADLS_PATH
from src.process_data import (
    clean_prices,
    filter_valid_data,
    add_final_mrp,
    calculate_disc,
)
from src.logic import generate_signals
from src.delta_loader import delta_loader


def main():
    # --------------------------------------------------
    # 1. Spark + Application Logger
    # --------------------------------------------------
    spark, config = get_spark_session()
    app_logger = get_logger(spark, "smart-price-tracker")

    app_logger.info("Starting Spark application")

    # --------------------------------------------------
    # 2. Ingestion Phase
    # --------------------------------------------------
    app_logger.info("Starting Flipkart and Ajio scrapers")

    run_flipkart_scraper(spark)
    run_ajio_scraper(spark)

    app_logger.info("Scraping completed successfully")

    # --------------------------------------------------
    # 3. Silver Layer (Cleaning & Transformation)
    # --------------------------------------------------
    raw_path = adls_path("raw")
    silver_path = adls_path("processed")
    delta_path = adls_path("delta_path")

    app_logger.info("Loading raw data from ADLS")
    raw_df = load_combined_data(spark, app_logger, BASE_ADLS_PATH)

    raw_count = raw_df.count()
    app_logger.info(f"Raw record count = {raw_count:,}")

    # ---------- Cleaning ----------
    app_logger.info("Starting data cleaning")
    df_cleaned = clean_prices(raw_df)

    cleaned_count = df_cleaned.count()
    app_logger.info(
        f"Cleaning completed | Before = {raw_count:,} | After = {cleaned_count:,}"
    )

    # ---------- Filtering ----------
    app_logger.info("Applying validity filters")
    df_filtered = filter_valid_data(df_cleaned)

    filtered_count = df_filtered.count()
    app_logger.info(
        f"Filtering completed | Dropped = {cleaned_count - filtered_count:,} | "
        f"Remaining = {filtered_count:,}"
    )

    # ---------- Enrichment (MRP) ----------
    app_logger.info("Adding final MRP column")
    df_with_mrp = add_final_mrp(df_filtered)

    app_logger.info(
        f"MRP enrichment completed | Columns = {len(df_with_mrp.columns)}"
    )

    # ---------- Discount Calculation ----------
    app_logger.info("Calculating discount percentages")
    df_final_silver = calculate_disc(df_with_mrp)

    final_silver_count = df_final_silver.count()
    app_logger.info(
        f"Silver transformation completed | Final record count = {final_silver_count:,}"
    )

    # ---------- Write Silver ----------
    app_logger.info("Writing silver data to processed path")
    write_processed_data(df_final_silver, silver_path)

    # --------------------------------------------------
    # 4. Curated Layer (Signals)
    # --------------------------------------------------
    app_logger.info("Calculating buying signals and moving averages")
    final_df = generate_signals(df_final_silver)

    app_logger.info("Signal generation completed")

    # --------------------------------------------------
    # 5. Delta Load
    # --------------------------------------------------
    app_logger.info("Starting Delta incremental load")
    delta_loader(spark, silver_path, delta_path)

    app_logger.info("Pipeline execution finished successfully")


if __name__ == "__main__":
    main()
