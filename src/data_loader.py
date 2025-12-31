from pyspark.sql import SparkSession
from src.utils import get_spark_session, load_config
import yaml
from src.logger import Log4j
from pyspark.sql.functions import col, coalesce, regexp_replace, to_timestamp, lit
from pyspark.sql.types import (
    DoubleType,
    StructType,
    StructField,
    StringType,
    IntegerType,
)

config = load_config()
container_name = config["azure"]["container_name"]
storage_account_name = config["azure"]["account_name"]

BASE_ADLS_PATH = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Data/"
)


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def write_processed_data(df, silver_path):
    """
    defining silver grain and writing to idempotent silver layer
    """
    spark = df.sparkSession
    
    # 1. Add Date and Window definition
    df_with_date = df.withColumn("date", F.to_date("timestamp"))

    latest_window = (
        Window
        .partitionBy("url", "website", "date")
        .orderBy(F.col("timestamp").desc())
    )

    # 2. Daily Metrics
    daily_metrics = (
        df_with_date
        .groupBy("url", "website", "date")
        .agg(
            F.min("final_price").alias("min_price_day"),
            F.max("final_price").alias("max_price_day"),
            F.avg("final_price").alias("avg_price_day"),
            F.avg("Discount_Percentage").alias("avg_discount_day"),
            F.count("*").alias("num_scrapes_day"),
            F.first("brand").alias("brand"),
            F.first("product_name").alias("product_name"),
            F.first("mrp_final").alias("mrp_final")
        )
    )


    latest_per_day = (
        df_with_date
        .withColumn("rn", F.row_number().over(latest_window))
        .filter(F.col("rn") == 1)
        .select(
            "url", "website", "date", 
            F.col("final_price").alias("closing_price"),
            F.col("timestamp").alias("last_scrape_time"),
            "status"
        )
    )

    daily_silver = (
        daily_metrics.alias("m")
        .join(
            latest_per_day.alias("l"),
            on=["url", "website", "date"],
            how="inner"
        )
    )

    if not DeltaTable.isDeltaTable(spark, silver_path):
        (
            daily_silver.write
            .format("delta")
            .mode("overwrite")
            .save(silver_path)
        )
    else:
        silver_delta = DeltaTable.forPath(spark, silver_path)
        (
            silver_delta.alias("t")
            .merge(
                daily_silver.alias("s"),
                "t.url = s.url AND t.website = s.website AND t.date = s.date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return daily_silver