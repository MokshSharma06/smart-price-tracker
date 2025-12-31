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
    df = df.withColumn(
        "mrp",
        trim(regexp_replace(col("mrp"), "[₹,]", ""))
    ).withColumn(
        "mrp", 
        expr("try_cast(mrp AS double)")
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
        (F.col("status") == "In Stock") & 
        (F.col("mrp").isNotNull() | F.col("selling_price").isNotNull())
    )

def add_final_mrp(df):
    w = (
        Window.partitionBy("url")
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
# Forward-fill to find the mrp
    df = df.withColumn("mrp_ffill", F.last("mrp", ignorenulls=True).over(w))

    #  Pick the first non-null value
    df = df.withColumn(
        "mrp_final",
        F.last("mrp", ignorenulls=True).over(w)
    )
    
    # selling price logic and ensuring not null
    df = df.withColumn(
        "final_price",
        F.coalesce(F.col("selling_price"), F.col("mrp_final"))
    )
    
    return df


def calculate_disc(df):
    return df.withColumn(
        "Discount_Percentage",
        round(
            ((F.col("mrp_final") - F.col("final_price")) / F.col("mrp_final")) * 100,
            2,
        ),
    )

def process_data(df):
    df1 = clean_prices(df)
    df2 = filter_valid_data(df1)
    df3 = add_final_mrp(df2)
    df4 = calculate_disc(df3)
    return df4

