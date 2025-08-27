from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.configloader import get_spark_conf

def get_spark_session():
    """
    Initialize SparkSession with Delta support using configs.
    """
    spark_conf = get_spark_conf()

    builder = SparkSession.builder.appName("smart-price-tracker")

    # Apply settings from conf/spark.conf
    for key, value in spark_conf.items():
        builder = builder.config(key, value)

    # Add Delta support
    builder = (
        builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:conf/log4j.properties")
        )

    return configure_spark_with_delta_pip(builder).getOrCreate()
