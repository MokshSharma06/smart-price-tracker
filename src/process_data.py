from pyspark.sql.functions import col, regexp_replace, round, current_date, lit

from src.utils import get_spark_session
from src.logger import Log4j

spark = get_spark_session()
logger = Log4j(spark)

df = spark.read.option("multiline", "true")\
    .json(["data/raw/ajio_products.json", "data/raw/flipkart_products.json"])

df.printSchema()
df.show()
print("Row count:", df.count())

