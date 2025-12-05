from src.utils import get_spark_session
from src.logger import get_logger
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, max as max_

spark = get_spark_session()


def delta_loader(
    spark,
    silver_path: str = "data/processed",
    delta_path: str = "data/delta_data/silver_products",
    watermark_path: str = "data/watermark/products",
):
    logger = get_logger(spark, "delta_loader")
    logger.info("Starting Delta_Loader")
    logger.info(f"Silver:{silver_path} to Delta: {delta_path}")

    silver_df = spark.read.parquet(silver_path)
    total_records = silver_df.count()
    logger.info(f"Loaded {total_records:,}silver records")
    try:
        delta_table = DeltaTable.forPath(spark, delta_path)
        max_ts_df = delta_table.toDF().agg(max_("timestamp").alias("max_ts"))
        current_watermark = max_ts_df.collect()[0]["max_ts"]

        if current_watermark:
            logger.info(f"Watermark Present : {current_watermark}")
        else:
            current_watermark = "None"
            logger.info("Delta table is empty.")

    except Exception as e:
        # If Delta table does not exist, this is the first run.
        logger.info("Delta table not found. Performing initial load.")
        current_watermark = "None"

    new_df = silver_df.filter(col("timestamp") > current_timestamp)
    new_count = new_df.count()

    logger.info(f"Found {new_count= }new records to append")
    if new_count == 0:
        logger.info("No new data to append")
        return
    logger.info(f"Appending {new_count:,} records to Delta table...")
    new_df.write.format("delta").mode("append").save(delta_path)
    logger.info("Data has been appended successfully ")

    # logic for watermark updating
