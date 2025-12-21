from src.utils import get_spark_session
from src.logger import Log4j
from src.data_loader import *
from src.process_data import *
from src.delta_loader import *
from src.fetch_prices import *
from src.fetch_price_ajio import *
from src.logic import *

# Configuration
raw_path = adls_path("raw")
silver_path = adls_path("processed")
delta_path = adls_path("delta_path")

def main():
    spark, config = get_spark_session()
    logger = Log4j(spark)

    logger.info("Starting Spark application")

    # 1. Ingestion Phase
    logger.info("Starting Flipkart and Ajio scrapers...")
    run_flipkart_scraper()
    run_ajio_scraper()
    logger.info("Scraping completed successfully.")

    # 2. Silver Layer: Cleaning & Transformation
    raw_df = load_combined_data(spark, logger, BASE_ADLS_PATH)
    logger.info("Combining data sources and beginning transformation.")

    df_cleaned = clean_prices(raw_df)
    df_filtered = filter_valid_data(df_cleaned)
    df_with_mrp = add_final_mrp(df_filtered)
    df_final_silver = calculate_disc(df_with_mrp)
    
    logger.info("Silver transformation complete. Writing to processed path.")
    write_processed_data(df_final_silver, silver_path)

    # 3. Curated Layer: Buy/Wait Signals
    logger.info("Calculating Buying Signals and Moving Averages...")
    final_df = generate_signals(df_final_silver)

    # 4. Delta Load with Schema Evolution
    logger.info(f"Syncing enriched data to Curated path: {delta_path}")
    final_df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(delta_path)

    logger.info("Pipeline Execution Finished Successfully")

if __name__ == "__main__":
    main()