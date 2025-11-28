from pyspark.sql.functions import col, regexp_replace, round, current_date, lit , coalesce, when ,to_date,to_timestamp,trim
from src.utils import get_spark_session
from src.logger import Log4j
from src.data_loader import combined_df
from pyspark.sql import functions as F, Window

spark = get_spark_session()
logger = Log4j(spark)

# lets remove the Ruppee symbol and , from the prices and cast them to double for calculations
def clean_prices(df):
    return (
        df.withColumn(
            "mrp_price",
            trim(regexp_replace(col("mrp_price"), "[₹,]", "")).cast("double")
        )
        .withColumn(
            "selling_price",
            trim(regexp_replace(col("selling_price"), "[₹,]", "")).cast("double")
        )
        .withColumn(
            "timestamp",
            col("timestamp").cast("timestamp")
        )
    )

def filter_valid_data(df):
    return df.filter(
        ~(
            col("status").isNull() | (col("status") == "Out of Stock")
            # col("brand").isNull() &
            # col("mrp_price").isNull() &
            # col("product_name").isNull()
        )
    )

def add_final_mrp(df):
    w = Window.partitionBy("url").orderBy("timestamp") \
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # 1) Forward-fill true MRP
    df = df.withColumn(
        "mrp_ffill",
        F.last("mrp_price", ignorenulls=True).over(w)
    )

    # 2) For rows before first MRP (where mrp_ffill is still null),
    # treat selling_price as MRP
    df = df.withColumn(
        "mrp_final",
        F.when(F.col("mrp_ffill").isNull(), F.col("selling_price"))
         .otherwise(F.col("mrp_ffill"))
    )
    return df

def calculate_disc(df):
    return df.withColumn(
        "Discount Percentage",
        round(
            ((F.col("mrp_final") - F.col("selling_price")) / F.col("mrp_final")) * 100,
            2
        )
    )


def process_data(df):
    """
    High-level pipeline you can call from main and tests.
    """
    df1 = clean_prices(df)
    df2 = filter_valid_data(df1)
    df3 = add_final_mrp(df2)
    df4 = calculate_disc(df3)
    return df4