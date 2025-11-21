from pyspark.sql import SparkSession
from src.utils import get_spark_session
from src.logger import Log4j
from pyspark.sql.functions import col, coalesce, regexp_replace, to_timestamp ,lit
from pyspark.sql.types import DoubleType , StructType, StructField,StringType,IntegerType

spark = get_spark_session()

df = spark.read\
    .option("mode", "DropMalformed")\
    .option("multiline","True")\
    .json("data/raw/flipkart_products.json")\
    
df.printSchema()
# df.show(truncate=True)
rc=df.count()
print(f"count is = {rc}")

df2 = spark.read\
     .option("mode","DropMalformed")\
    .option("multiline","true")\
    .json("data/raw/ajio_products.json")\

df2 = df2.drop("error") \
         .withColumnRenamed("mrp", "mrp_price") \
         .withColumnRenamed("price", "selling_price")

df2.printSchema()
# df2.show()
rc2=df2.count()
print(f"count is = {rc2}")

# combining the data frames in order to gather data at one place for the downstream consumption
combined_df = df.unionByName(df2)
count = combined_df.count()
combined_df.show(n = combined_df.count(), truncate= 20)
print(f"count is = file1 + file 2= {count}")