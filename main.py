from src.utils import get_spark_session
from src.logger import Log4j
from src.data_loader import *
from src.process_data import *
from src.delta_loader import *
from src.fetch_prices import *
from src.fetch_price_ajio import *


raw_path= adls_path("raw")
silver_path =adls_path("processed")
delta_path=adls_path("delta_path")

def main():
    spark,config = get_spark_session()
    logger = Log4j(spark)

    logger.info("Starting Spark application")

    run_flipkart_scraper()
    logger.info("starting the flipkart scraper and ajio scraper")

    run_ajio_scraper()
    logger.info("scraping done ! ")


    raw_df = load_combined_data(spark,logger,BASE_ADLS_PATH)
    logger.info("combining the data of two sources into one")

  
    df = clean_prices(raw_df)
    logger.info("DataFrame has been cleaned")

    df2 = filter_valid_data(df)
    logger.info("data has been filtered")

    df3 = add_final_mrp(df2)
    logger.info("calculated mrp col has been added")

    df4 = calculate_disc(df3)
    logger.info("Discount has been calculated (mrp - SP /mrp)*100 ")

    # lets write down the final data frame to our processed location as it has been duly cleaned , transformed (Raw(Bronze)-------> Processed(Silver))

    write_processed_data(df4, silver_path)
    logger.info(f"Data successfully written to Silver path: {silver_path}")
    logger.info("Starting Delta Loader: Silver -> Curated")
    delta_loader(spark, silver_path, delta_path)
    logger.info("Pipeline Execution Finished Successfully")


if __name__ == "__main__":
    main()
