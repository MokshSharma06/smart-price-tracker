from pyspark.sql.functions import col, regexp_replace, round, current_date, lit , coalesce, when
from src.utils import get_spark_session
from src.logger import Log4j
from src.data_loader import df, df2 ,combined_df
spark = get_spark_session()
logger = Log4j(spark)

df_transformed = combined_df.withColumn(
    "mrp_price",
    when(
        (col("status") == "In Stock"),
        coalesce(col("mrp_price"), col("selling_price"))
    ).otherwise(col("mrp_price"))
)
df_transformed.show(n=25)

print("Row count:", df_transformed.count())