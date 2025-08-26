from src.utils import get_spark_session
from src.logger import Log4j
def main():
    spark = get_spark_session()
    logger = Log4j(spark)

    logger.info("Starting Spark application")

    data = [(1, "Alice", 28), (2, "Bob", 35), (3, "Charlie", 23)]
    columns = ["id", "name", "age"]

    df = spark.createDataFrame(data, columns)
    logger.info("Created DataFrame")

    df.show()

if __name__ == "__main__":
    main()
