import sys
print("sys.path:", sys.path)

from src.process_data import *
from pyspark.sql.functions import col
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
        ("iPhone 14", "Apple", None, "₹69,900", "2025-12-1 08:00:00", "In Stock"),
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

        def test_final_mrp(self, spark):
              sample_data_2 = [
        (100, "A", "Nike", "Running Shoe", 100.0, None, "In Stock", "nike.com"),
        (101, "A", "Nike", "Running Shoe", 100.0, None, "In Stock", "nike.com"),
        (102, "A", "Nike", "Running Shoe", 80.0, 100.0, "In Stock", "nike.com"),
        (103, "A", "Nike", "Running Shoe", 75.0, None, "In Stock", "nike.com"),
        (104, "A", "Nike", "Running Shoe", 80.0, None, "In Stock", "nike.com"),
        (110, "B", "Adidas", "Training Pant", 250.0, None, "In Stock", "adidas.com"),
        (111, "B", "Adidas", "Training Pant", 140.0, 250.0, "In Stock", "adidas.com"),
        (112, "B", "Adidas", "Training Pant", 130.0, None, "In Stock", "adidas.com"),
    ]
              sample_df_2 = spark.createDataFrame(
        sample_data_2,
        [
            "timestamp",
            "URL",
            "Brand",
            "product_name",
            "selling_price",
            "mrp_price",
            "status",
            "website",
        ],
    )
              expected_data = [
        (100, "A", "Nike", "Running Shoe", 100.0, None, "In Stock", "nike.com", None, 100),
        (101, "A", "Nike", "Running Shoe", 100.0, None, "In Stock", "nike.com", None, 100),
        (102, "A", "Nike", "Running Shoe", 80.0, 100.0, "In Stock", "nike.com", 100, 100),
        (103, "A", "Nike", "Running Shoe", 75.0, None, "In Stock", "nike.com", 100, 100),
        (104, "A", "Nike", "Running Shoe", 80.0, None, "In Stock", "nike.com", 100, 100),
        (110, "B", "Adidas", "Training Pant", 250.0, None, "In Stock", "adidas.com", None, 250),
        (111, "B", "Adidas", "Training Pant", 140.0, 250.0, "In Stock", "adidas.com", 250, 250),
        (112, "B", "Adidas", "Training Pant", 130.0, None, "In Stock", "adidas.com", 250, 250),
    ]
              expected_df = spark.createDataFrame(
        expected_data,
        [
            "timestamp",
            "URL",
            "Brand",
            "product_name",
            "selling_price",
            "mrp_price",
            "status",
            "website",
            "mrp_ffill",
            "mrp_final",
        ],
    )
              result_df = add_final_mrp(sample_df_2)
              assert result_df.collect() == expected_df.collect()

        def test_discount_percentage(self,spark):
                             sample_data_3 = [
        (100, "A", "Nike", "Running Shoe", 100.0, None, "In Stock", "nike.com",None,100),
        (101, "A", "Nike", "Running Shoe", 100.0, None, "In Stock", "nike.com",None,100),
        (102, "A", "Nike", "Running Shoe", 80.0, 100.0, "In Stock", "nike.com",100, 100),
    ]
                             

                             sample_df_3 = spark.createDataFrame(
        sample_data_3,
        [
            "timestamp",
            "URL",
            "Brand",
            "product_name",
            "selling_price",
            "mrp_price",
            "status",
            "website",
            "mrp_ffill",
            "mrp_final",
        ],
    )
                             expected_data = [
                                     (100, "A", "Nike", "Running Shoe", 100.0, None, "In Stock", "nike.com", None, 100, 0),
                                     (101, "A", "Nike", "Running Shoe", 100.0, None, "In Stock", "nike.com", None, 100, 0),
                                     (102, "A", "Nike", "Running Shoe", 80.0, 100.0, "In Stock", "nike.com", 100, 100, 20),
                             ]
                             expected_df_2 = spark.createDataFrame(
                                     expected_data,
                                     [
                                             "timestamp",
                                             "URL",
                                             "Brand",
                                             "product_name",
                                             "selling_price",
                                             "mrp_price",
                                             "status",
                                             "website",
                                             "mrp_ffill",
                                             "mrp_final",
                                             "discount_percentage"
                                             ],
                             )
                             result_df = calculate_disc(sample_df_3)

                             print("Result schema:")
                             result_df.printSchema()
                             print("Expected schema:")
                             expected_df_2.printSchema()

                             assert result_df.collect() == expected_df_2.collect()


               

    


               

