from src.utils import get_spark_session
from src.logger import get_logger
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
from src.utils import*

spark ,config = get_spark_session()
silver_path =adls_path("processed")
delta_path=adls_path("delta_path")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS curated_data
    USING DELTA
    LOCATION '{delta_path}'
""")
spark.sql("""
SELECT
    COUNT(*)        AS total_rows,
    MIN(timestamp)  AS min_ts
FROM curated_data;



""").show(truncate=True)
