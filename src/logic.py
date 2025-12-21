from src.utils import get_spark_session
from src.logger import get_logger
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
from src.utils import*


from pyspark.sql import functions as F
from pyspark.sql.window import Window

def generate_signals(df):

    window_spec = Window.partitionBy("product_name").orderBy("timestamp").rowsBetween(-6, 0)
    

    final_df = df.withColumn("avg_price_7d", F.round(F.avg("selling_price").over(window_spec), 2)) \
                    .withColumn("signal", F.when(F.col("selling_price") <= (F.col("avg_price_7d") * 0.9), "BUY")
                                          .otherwise("WAIT"))
    return final_df


# spark ,config = get_spark_session()
# silver_path =adls_path("processed")
# delta_path=adls_path("delta_path")

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS curated_data
#     USING DELTA
#     LOCATION '{delta_path}'
# """)
# spark.sql("""
# DESCRIBE curated_data;

# """).show(truncate=True)
# spark.sql("""
# select * from curated_data
#           """)
