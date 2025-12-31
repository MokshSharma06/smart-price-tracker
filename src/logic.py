from src.utils import get_spark_session
from src.logger import get_logger
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
from src.utils import*


from pyspark.sql import functions as F
from pyspark.sql.window import Window
"""
Gold layer business logic.

Consumes daily silver data and generates
price-drop alert signals.
"""

def generate_signals(df):
    df = df.withColumn("date", F.to_date("timestamp"))

    
    window_spec = Window.partitionBy("url","website").orderBy("date").rowsBetween(-7, -1)
    
    
    df = df.withColumn("avg_price_7d", F.round(F.avg("selling_price").over(window_spec), 2))
    df = df.filter(F.col("date") == F.current_date())

    final_df = df.filter(
        ((F.col("avg_price_7d").isNotNull()) & (F.col("selling_price") <= F.col("avg_price_7d") * 0.9)) |
        (F.col("Discount_Percentage") >= 10)
    ).withColumn("signal", F.lit("BUY"))

    return final_df

