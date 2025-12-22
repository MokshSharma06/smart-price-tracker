import pytest
from src.utils import get_spark_session
from pyspark.sql import SparkSession
@pytest.fixture(scope="session")
def spark():
    spark,config = get_spark_session()
    yield spark
    spark.stop()
def test_spark_session_created(spark):
    """Check that SparkSession is created successfully."""
    assert isinstance(spark, SparkSession)
    assert spark.sparkContext.appName == "smart-price-tracker"