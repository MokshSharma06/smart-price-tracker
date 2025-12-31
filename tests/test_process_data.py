import pytest
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

from src.process_data import (
    clean_prices,
    add_final_mrp,
    calculate_disc,
    process_data
)
from src.data_loader import write_processed_data
from src.utils import get_spark_session


# --------------------------------------------------
# Spark Fixture
# --------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    spark, _ = get_spark_session()
    yield spark
    spark.stop()


# --------------------------------------------------
# Sample RAW dataframe fixture
# --------------------------------------------------
@pytest.fixture
def sample_df(spark):
    data = [
        ("iPhone 14", "Apple", "₹79,900", "₹69,900", "2024-01-10 10:00:00", "In Stock"),
        ("iPhone 14", "Apple", "₹79,900", "₹72,900", "2025-01-02 12:00:00", "In Stock"),
        ("iPhone 14", "Apple", None, "₹65,000", "2025-01-01 15:00:00", "Out of Stock"),
        ("Samsung S23", "Samsung", "", "₹70,000", "2025-01-01 09:00:00", "In Stock"),
        ("iPhone 14", "Apple", None, "₹69,900", "2025-12-01 08:00:00", "In Stock"),
    ]

    return spark.createDataFrame(
        data,
        ["product_name", "brand", "mrp", "selling_price", "timestamp", "status"]
    )


# ==================================================
# PROCESS DATA TESTS
# ==================================================
class TestProcessData:

    # --------------------------------------------------
    # CLEAN PRICES
    # --------------------------------------------------
    def test_clean_prices(self, spark, sample_df):
        result = clean_prices(sample_df)

        assert result.schema["mrp"].dataType.simpleString() == "double"
        assert result.schema["selling_price"].dataType.simpleString() == "double"

        cleaned = result.orderBy("timestamp").collect()
        assert cleaned[0]["mrp"] == 79900.0
        assert cleaned[0]["selling_price"] == 69900.0

        # NULL mrp rows are allowed in RAW
        assert result.filter(col("mrp").isNull()).count() >= 2


    # --------------------------------------------------
    # MRP FORWARD FILL
    # --------------------------------------------------
    def test_add_final_mrp_forward_fill(self, spark):
        data = [
            ("2025-01-01 10:00:00", "urlA", "Nike", "Shoe", 100.0, None, "In Stock", "siteA"),
            ("2025-01-01 12:00:00", "urlA", "Nike", "Shoe", 90.0, 120.0, "In Stock", "siteA"),
            ("2025-01-01 15:00:00", "urlA", "Nike", "Shoe", 85.0, None, "In Stock", "siteA"),
        ]

        df = spark.createDataFrame(
            data,
            [
                "timestamp", "url", "brand", "product_name",
                "selling_price", "mrp", "status", "website"
            ]
        ).withColumn("timestamp", col("timestamp").cast("timestamp"))

        result = add_final_mrp(df).orderBy("timestamp").collect()

        assert result[0]["mrp_final"] is None
        assert result[1]["mrp_final"] == 120.0
        assert result[2]["mrp_final"] == 120.0


    # --------------------------------------------------
    # DISCOUNT CALCULATION
    # --------------------------------------------------
def test_calculate_discount_percentage(spark):
    data = [
        (
            "2025-01-01 10:00:00",
            "urlA",
            "Nike",
            "Shoe",
            80.0,     # final_price
            100.0,    # mrp_final
            "siteA"
        ),
    ]

    df = spark.createDataFrame(
        data,
        [
            "timestamp",
            "url",
            "brand",
            "product_name",
            "final_price",
            "mrp_final",
            "website",
        ]
    ).withColumn("timestamp", col("timestamp").cast("timestamp"))

    result = calculate_disc(df).collect()[0]

    assert result["Discount_Percentage"] == 20.0




    # --------------------------------------------------
    # END-TO-END PROCESS TEST
    # --------------------------------------------------
    def test_process_data_end_to_end(self, spark):
        ts = datetime.strptime("2025-11-27 11:00:28", "%Y-%m-%d %H:%M:%S")

        data = [
            (ts, "urlA", "Nike", "Shoe", 100.0, None, "In Stock", "siteA"),
            (ts, "urlA", "Nike", "Shoe", 80.0, 120.0, "In Stock", "siteA"),
        ]

        df = spark.createDataFrame(
            data,
            [
                "timestamp", "url", "brand", "product_name",
                "selling_price", "mrp", "status", "website"
            ]
        )

        result = process_data(df)

        assert "mrp_final" in result.columns
        assert "final_price" in result.columns
        assert "Discount_Percentage" in result.columns


# ==================================================
# SILVER IDEMPOTENCY TEST
# ==================================================
def test_silver_idempotency(spark, tmp_path):
    data = [
        (
            "2025-01-10 10:00:00",
            "url1",
            "Nike",
            "Shoe",
            100.0,   # final_price
            120.0,   # mrp_final
            16.67,
            "In Stock",
            "siteA"
        ),
        (
            "2025-01-10 15:00:00",
            "url1",
            "Nike",
            "Shoe",
            90.0,
            120.0,
            25.0,
            "In Stock",
            "siteA"
        ),
    ]

    df = spark.createDataFrame(
        data,
        [
            "timestamp",
            "url",
            "brand",
            "product_name",
            "final_price",
            "mrp_final",
            "Discount_Percentage",
            "status",
            "website",
        ]
    ).withColumn("timestamp", col("timestamp").cast("timestamp"))

    silver_path = str(tmp_path / "silver")

    write_processed_data(df, silver_path)
    write_processed_data(df, silver_path)

    silver_df = spark.read.format("delta").load(silver_path)

    dupes = (
        silver_df
        .groupBy("url", "website", "date")
        .count()
        .filter("count > 1")
        .count()
    )

    assert dupes == 0
