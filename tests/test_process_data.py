import sys
print("sys.path:", sys.path)

from src.process_data import *
import pytest
from src.utils import get_spark_session
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = get_spark_session()
    yield spark
    spark.stop()
@pytest.fixture
def sample_df(spark):
        data = [
        ("iPhone 14", "Apple", "₹79,900", "₹69,900", "2024-01-10 10:00:00", "In Stock"),
        ("iPhone 14", "Apple", "₹79,900", "₹72,900", "2025-01-02 12:00:00", "In Stock"),
        ("iPhone 14", "Apple", None, "₹65,000", "2025-01-01 15:00:00", "Out of Stock"),
        ("Samsung S23", "Samsung", "", "Out of Stock", "2025-01-01 09:00:00", None),
        ("iPhone 14", "Apple", None, "₹69,900", "2025-12-1 08:00:00", "In Stock"),  # before first MRP
    ]
        return spark.createDataFrame(data, 
        ["product_name", "brand", "mrp_price", "selling_price", "timestamp", "status"])

class Testing_Process:
        def test_clean_prices(self, spark , sample_df):
                result =clean_prices(sample_df)
                rows = result.orderBy("timestamp").collect()

                # Verify first row cleaning (₹79,900 → 79900.0)
                assert rows[0]["mrp_price"] == 79900.0
                assert rows[0]["selling_price"] == 69900.0
                assert result.schema["mrp_price"].dataType.simpleString() == "double"
                assert result.schema["selling_price"].dataType.simpleString() == "double"
                print("Row Cleaning Done")

                # Verify row with empty MRP ("" → null)
                empty_mrp_row = result.filter(col("mrp_price").isNull()).count()
                assert empty_mrp_row >= 3
                print("Null values are dropped and verified")

