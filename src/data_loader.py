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


def load_combined_data(spark, logger, base_adls_path):
    FLIPKART_RAW_PATH = f"{base_adls_path}raw/flipkart/"
    AJIO_RAW_PATH = f"{base_adls_path}raw/ajio/"

    # 1. Load Flipkart Data
    df = (
        spark.read.option("mode", "DropMalformed")
        .option("multiline", "True")
        .json(FLIPKART_RAW_PATH)
    )
    logger.info("Reading from Flipkart raw data")
    rc = df.count()
    logger.info(f"Flipkart record count = {rc}")

    # 2. Load Ajio Data
    df2 = (
        spark.read.option("mode", "DropMalformed")
        .option("multiline", "true")
        .json(AJIO_RAW_PATH)
    )
    logger.info("Reading from Ajio raw data")
    df2 = (
        df2.drop("error")
        .withColumnRenamed("mrp", "mrp_price")
        .withColumnRenamed("price", "selling_price")
    )

    # 3. Combine DataFrames
    logger.info("Combining dataframes using unionByName...")
    combined_df = df.unionByName(df2, allowMissingColumns=True)
    count = combined_df.count()
    logger.info(f"Combined record count (Flipkart + Ajio) = {count}")

    return combined_df
