import os
import shutil
import pytest
from datetime import datetime
from pyspark.sql.functions import max as spark_max
from src.delta_loader import delta_loader
from src.utils import get_spark_session
from src.process_data import *
from delta.tables import DeltaTable

TEST_BASE_DIR = "data/delta_test"
silver_path = f"{TEST_BASE_DIR}/processed"
delta_path = f"{TEST_BASE_DIR}/delta_data/curated_data"


@pytest.fixture(scope="session")
def spark_session():
    spark = get_spark_session()

    yield spark
    print("Stopping Spark Session.")
    spark.stop()


@pytest.fixture(scope="module", autouse=True)
def setup_test_environment():
    if os.path.exists(TEST_BASE_DIR):
        print(f"\n[SETUP] Deleting existing test directory: {TEST_BASE_DIR}")
        shutil.rmtree(TEST_BASE_DIR)

    os.makedirs(silver_path, exist_ok=True)
    os.makedirs(f"{TEST_BASE_DIR}/delta_data", exist_ok=True)
    print(f"Created new directories: {silver_path} and {TEST_BASE_DIR}/delta_data")
    yield
    if os.path.exists(TEST_BASE_DIR):
        print(f"Cleaning up test directory: {TEST_BASE_DIR}")
        shutil.rmtree(TEST_BASE_DIR)
    else:
        print(f"Test directory {TEST_BASE_DIR} was already removed.")


def create_mock_data(spark, date_str: str, value: int):
    """Creates a DataFrame with a specific timestamp and value."""
    ts = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    data = [
        (1, f"ProductA_{value}", ts),
        (2, f"ProductB_{value}", ts),
    ]
    schema = ["id", "name", "timestamp"]
    return spark.createDataFrame(data, schema)


def test_delta_loader_full_and_incremental_load(spark_session):
    """
    Tests initial load, incremental load, and idempotency using
    watermarking logic against the Delta table itself.
    """
    spark = spark_session

    # Initial Load Test (No Delta Table Exists)
    T1_TS_STR = "2025-01-01 10:00:00"
    T1_TS_DT = datetime.strptime(T1_TS_STR, "%Y-%m-%d %H:%M:%S")
    initial_df = create_mock_data(spark, T1_TS_STR, 1)
    initial_df.write.parquet(silver_path, mode="append")

    delta_loader(spark, silver_path=silver_path, delta_path=delta_path)

    # Assertion : Check total records after initial load
    result_df_1 = spark.read.format("delta").load(delta_path)
    assert result_df_1.count() == 2, "Initial load failed: expected 2 records."

    # Assertion : Check the watermark (max timestamp) after initial load
    max_ts_1 = result_df_1.agg(spark_max("timestamp")).collect()[0][0]
    assert (
        max_ts_1 == T1_TS_DT
    ), f"Initial load failed: expected watermark {T1_TS_DT}, got {max_ts_1}."

    # Incremental Load Test (New Data Only)
    T2_TS_STR = "2025-01-02 11:00:00"
    T2_TS_DT = datetime.strptime(T2_TS_STR, "%Y-%m-%d %H:%M:%S")
    incremental_df_new = create_mock_data(spark, T2_TS_STR, 2)
    incremental_df_new.write.parquet(silver_path, mode="append")

    # Run the loader again. It should filter T1 data and only append T2 data.
    delta_loader(spark, silver_path=silver_path, delta_path=delta_path)

    # Assertion: Check total records after incremental load (2 from T1 + 2 from T2 = 4)
    result_df_2 = spark.read.format("delta").load(delta_path)
    assert (
        result_df_2.count() == 4
    ), "Incremental load failed: expected 4 records (2 initial + 2 new)."

    # Assertion: Check the new watermark
    max_ts_2 = result_df_2.agg(spark_max("timestamp")).collect()[0][0]
    assert (
        max_ts_2 == T2_TS_DT
    ), f"Incremental load failed: expected new watermark {T2_TS_DT}, got {max_ts_2}."

    # choosing an odler date to test
    T1_5_TS_STR = "2025-01-01 10:30:00"
    incremental_df_old = create_mock_data(spark, T1_5_TS_STR, 3)
    incremental_df_old.write.parquet(silver_path, mode="append")

    delta_loader(spark, silver_path=silver_path, delta_path=delta_path)
    result_df_3 = spark.read.format("delta").load(delta_path)
    assert result_df_3.count() == 4, "test failed"

    # Assertion: Check the watermark (should not have changed, still T2)
    max_ts_3 = result_df_3.agg(spark_max("timestamp")).collect()[0][0]
    assert max_ts_3 == T2_TS_DT, "test failed watermark was wrongly updated."
