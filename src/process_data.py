from pyspark.sql.functions import col, regexp_replace, round, current_date, lit , coalesce, when
from src.utils import get_spark_session
from src.logger import Log4j

spark = get_spark_session()
logger = Log4j(spark)

df = spark.read.option("multiline", "true")\
    .json(["data/raw/ajio_products.json", "data/raw/flipkart_products.json"])
df.printSchema()
df_combined = df.withColumn("MaxPrice", coalesce(col("mrp"), col("mrp_price")))\
    .drop("mrp").drop("mrp_price")

df_transformed = df_combined.withColumn(
    "selling_price",
    when(
        (col("MaxPrice").isNotNull()) & 
        (col("selling_price").isNull()) & 
        (col("status") == "In Stock"),
        col("MaxPrice")
    ).otherwise(col("selling_price"))
)

df_transformed.select("brand", "MaxPrice", "selling_price", "product_name", "status", "timestamp", "website")\
    .show(df_transformed.count(), truncate=False)

print("Row count:", df_transformed.count())
