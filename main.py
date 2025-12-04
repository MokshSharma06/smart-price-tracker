from src.utils import get_spark_session
from src.logger import Log4j
from src.data_loader import *
from src.process_data import *


def main():
    spark = get_spark_session()
    logger = Log4j(spark)

    logger.info("Starting Spark application")

    df = clean_prices(combined_df)
    logger.info("DataFrame has been cleaned")

    df2 = filter_valid_data(df)
    logger.info("data has been filtered")

    df3 = add_final_mrp(df2)
    logger.info("calculated mrp col has been added")

    df4 = calculate_disc(df3)
    logger.info("Discount has been calculated (mrp - SP /mrp)*100 ")

    # lets write down the final data frame to our processed location as it has been duly cleaned , transformed (Raw(Bronze)-------> Processed(Silver))

    df5 = write_processed_data(df4, "./data/processed/processed_data")
    logger.info("Data has successfully been written from raw to processed data")


if __name__ == "__main__":
    main()
