from src.utils import get_spark_session
from src.logger import get_logger
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max

spark = get_spark_session()


def delta_loader(
    spark,
    silver_path: str = "data/processed",
    delta_path: str = "data/delta_data/curated_data",
):
    logger = get_logger(spark, "delta_loader")
    logger.info("Starting Delta_Loader")
    logger.info(f"silver source: {silver_path} to Delta sink: {delta_path}")

    # Load silver Data
    silver_df = spark.read.parquet(silver_path)
    total_silver_records = silver_df.count()
    logger.info(f"Loaded {total_silver_records:,} silver records")

    current_watermark = None

    try:
        # Determine Current Watermark from Existing Delta Table
        delta_df = spark.read.format("delta").load(delta_path)
        # Find the max 'timestamp' from the existing data
        max_ts_row = delta_df.agg(spark_max("timestamp").alias("max_ts")).collect()[0]
        current_watermark = max_ts_row["max_ts"]

        logger.info(
            f"Existing Delta table found. Current watermark: {current_watermark}"
        )

    except Exception:
        # Initial Load (Table Not Found)
        logger.info("Delta table not found. Performing initial load.")
        pass  # current_watermark remains None

    # Filter silver Data based on Watermark
    if current_watermark is None:
        # Initial Load: take all records
        new_df = silver_df
        logger.info("Initial load: All silver records will be loaded.")
    else:
        # Incremental Load: filter against the last processed timestamp
        # Filter for records *newer* than the watermark
        new_df = silver_df.filter(col("timestamp") > lit(current_watermark))
        logger.info(f"Filtering silver data using watermark: {current_watermark}")

    new_count = new_df.count()
    logger.info(f"Found {new_count:,} new records to append")

    if new_count == 0:
        logger.info("No new data to append. Exiting.")
        return

    # Append New Data to Delta Table
    logger.info(f"Appending {new_count:,} records to Delta table...")
    new_df.write.format("delta").mode("append").save(delta_path)
    logger.info("Data has been appended successfully.")
