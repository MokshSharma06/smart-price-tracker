from pyspark.sql.functions import (
    col,
    regexp_replace,
    round,
    current_date,
    lit,
    coalesce,
    when,
    to_date,
    to_timestamp,
    trim,
    expr,
)
from src.utils import get_spark_session
from src.logger import Log4j
from src.data_loader import *
from pyspark.sql import functions as F, Window




# lets remove the Ruppee symbol and , from the prices and cast them to double for calculations, using try_cast because some mrp fiels contain literal MRP or MRP 500
def clean_prices(df):
    spark, config = get_spark_session("Process_data")
    logger = Log4j(spark)
    df = df.withColumn(
        "mrp_price",
        trim(regexp_replace(col("mrp_price"), "[₹,]", ""))
    ).withColumn(
        "mrp_price", 
        expr("try_cast(mrp_price AS double)")
    ).withColumn(
        "selling_price",
        expr("try_cast(regexp_replace(selling_price, '[₹,]', '') AS double)")
    ).withColumn(
        "timestamp", 
        col("timestamp").cast("timestamp")
    )
    
    return df


def filter_valid_data(df):
    return df.filter(
        ~(
            col("status").isNull()
            | (col("status") == "Out of Stock")
            # col("brand").isNull() &
            # col("mrp_price").isNull()
            # col("product_name").isNull()
        )
    )


def add_final_mrp(df):
    w = (
        Window.partitionBy("url")
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # 1) Forward-fill true MRP
    df = df.withColumn("mrp_ffill", F.last("mrp_price", ignorenulls=True).over(w))

    # 2) For rows before first MRP (where mrp_ffill is still null),
    # treat selling_price as MRP
    df = df.withColumn(
        "mrp_final",
        F.when(F.col("mrp_ffill").isNull(), F.col("selling_price")).otherwise(
            F.col("mrp_ffill")
        ),
    )
    return df


def calculate_disc(df):
    return df.withColumn(
        "Discount_Percentage",
        round(
            ((F.col("mrp_final") - F.col("selling_price")) / F.col("mrp_final")) * 100,
            2,
        ),
    )


def process_data(df):
    df1 = clean_prices(df)
    df2 = filter_valid_data(df1)
    df3 = add_final_mrp(df2)
    df4 = calculate_disc(df3)
    return df4


from pathlib import Path


def write_processed_data(df, full_path):
    df.write.mode("overwrite").parquet(f"{full_path}")

    print(f" Data has been written to {full_path}")
    return df
