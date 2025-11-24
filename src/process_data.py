from pyspark.sql.functions import col, regexp_replace, round, current_date, lit , coalesce, when ,to_date,to_timestamp,trim
from src.utils import get_spark_session
from src.logger import Log4j
from src.data_loader import df, df2 ,combined_df
spark = get_spark_session()
logger = Log4j(spark)



#initially when i made project i didnt add the col for Product availability and later i added however there are some records with availability = null
date = "2025-09-10 23:59:59"
df_transformed = combined_df.withColumn(
    "status",
    when(
        (col("status").isNull()) &
        (col("timestamp").cast("timestamp") <= (lit(date).cast("timestamp"))) &
        (col("selling_price").isNotNull() | col("mrp_price").isNotNull()),
        lit("In Stock") 
    ).otherwise(col("status"))
)

# so some products were being sold at mrp price and hence selling price was same as mrp but mrp returned null , so used coalesce function
df_transformed_2 = df_transformed.withColumn(
    "mrp_price",
    when(
        (col("status") == "In Stock"),
        coalesce(col("mrp_price"), col("selling_price"))
    ).otherwise(col("mrp_price"))
)
# lets remove the Ruppee symbol and , from the prices and cast them to double for calculations
df_cleaned = df_transformed_2.withColumn(
    "mrp_price",
    trim(regexp_replace(col("mrp_price"), "[₹,]", "")).cast("double")
).withColumn(
    "selling_price",
    trim(regexp_replace(col("selling_price"), "[₹,]", "")).cast("double")
)


df_cleaned.show(n=50)

print("Row count:", df_cleaned.count())