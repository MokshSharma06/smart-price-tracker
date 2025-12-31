import requests
import logging
from src.logger import get_logger
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
import sys, os
from src.utils import get_spark_session


site_name = "Flipkart"

def scrape_flipkart_product(url):
    spark,config = get_spark_session()
    logger = get_logger(spark, "flipkart_scraper")
    logger.info("Starting Flipkart scraper")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        sold_out_tag = soup.find(
            "div", class_="VkYRUs"
        )  # keep check if flipkart changes the classes
        if sold_out_tag and "Sold Out" in sold_out_tag.get_text():
            name_tag = soup.find("span", class_="LMizgS")
            brand_tag = soup.find("span", class_="qbKhdn")
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.warn(f"Product is out of stock {name_tag.text.strip()}")

            return {
                "product_name": name_tag.text.strip() if name_tag else None,
                "selling_price": None,
                "status": "Out of Stock",
                "mrp": None,
                "brand": brand_tag.text.strip() if brand_tag else None,
                "website": site_name,
                "timestamp": timestamp,
                "url": url,
            }

        # Selling price with fallbacks

        price_tag = soup.find("div", class_=["hZ3P6w", "bnqy13"])
        if not price_tag:
            price_tag = soup.find(string=lambda x: x and "₹" in x)
        if not price_tag:
            match = re.search(r"₹\s?([\d,]+)", soup.get_text())
            if match:
                price_tag = match.group(0)

        # MRP tag
        mrp_tag = soup.find("div", class_=["kRYCnD", "yHYOcc"])
        # Name & brand tags
        name_tag = soup.find("span", class_="LMizgS")
        brand_tag = soup.find("span", class_="qbKhdn")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if price_tag and name_tag and brand_tag:
            product_data = {
                "product_name": name_tag.text.strip(),
                "selling_price": (
                    price_tag.get_text(strip=True)
                    if hasattr(price_tag, "get_text")
                    else price_tag
                ),
                "status": "In Stock",
                "mrp": mrp_tag.get_text(strip=True) if mrp_tag else None,
                "brand": brand_tag.text.strip(),
                "website": site_name,
                "timestamp": timestamp,
                "url": url,
            }
            logger.info(f"Scraped {site_name} | Product: {name_tag.text.strip()}")
            return product_data
        else:
            logger.warn(f"Price or product details not found on {url}")
            return None

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None


flipkart_urls = [
    "https://www.flipkart.com/nike-downshifter-13-running-shoes-women/p/itm672e266097def?pid=SHOGYHFQGABKUUBK&lid=LSTSHOGYHFQGABKUUBK2GVOX2&marketplace=FLIPKART&q=nike+downshifter+13+men&store=osp%2Fiko&srno=s_1_1&otracker=AS_QueryStore_OrganicAutoSuggest_1_9_na_na_ps&otracker1=AS_QueryStore_OrganicAutoSuggest_1_9_na_na_ps&fm=search-autosuggest&iid=e4c88bac-a1fa-4e65-9bd2-986fad928192.SHOGYHFQGABKUUBK.SEARCH&ppt=sp&ppn=sp&ssid=9dyl2pok2n9bfzeo1760435674061&qH=a550c8f3d4f26f1e",
    "https://www.flipkart.com/nike-c1ty-sneakers-men/p/itm32724af11099d?pid=SHOHDH84NG2GVZ3Y&lid=LSTSHOHDH84NG2GVZ3YSEWE9H&marketplace=FLIPKART&fm=factBasedRecommendation%2FrecentlyViewed&iid=R%3Arv%3Bpt%3App%3Buid%3A769247bf-8d7a-11f0-9307-258b510dc13d%3B.SHOHDH84NG2GVZ3Y&ppt=pp&ppn=pp&ssid=wsgzyrrbo4ucs2kg1756394844172&otracker=pp_reco_Recently%2BViewed_4_38.productCard.RECENTLY_VIEWED_NIKE%2BC1TY%2BSneakers%2BFor%2BMen_SHOHDH84NG2GVZ3Y_factBasedRecommendation%2FrecentlyViewed_3&otracker1=pp_reco_PINNED_factBasedRecommendation%2FrecentlyViewed_Recently%2BViewed_DESKTOP_HORIZONTAL_productCard_cc_4_NA_view-all&cid=SHOHDH84NG2GVZ3Y",
    "https://www.flipkart.com/nike-sneakers-women/p/itme185d74ecb493?pid=SHOHD2VWANZNDNGC&lid=LSTSHOHD2VWANZNDNGCNXDEQG&marketplace=FLIPKART&fm=productRecommendation%2Fsimilar&iid=R%3As%3Bp%3ASHOHD2VWGVXMGPKH%3Bl%3ALSTSHOHD2VWGVXMGPKHUC9QA1%3Bpt%3App%3Buid%3A14532b20-a8e4-11f0-8979-abc00ee45ccf%3B.SHOHD2VWANZNDNGC&ppt=pp&ppn=pp&ssid=6njh5pjyvco55la81760435731055&otracker=pp_reco_Similar%2BProducts_1_32.productCard.PMU_HORIZONTAL_NIKE%2BSneakers%2BFor%2BWomen_SHOHD2VWANZNDNGC_productRecommendation%2Fsimilar_0&otracker1=pp_reco_PINNED_productRecommendation%2Fsimilar_Similar%2BProducts_GRID_productCard_cc_1_NA_view-all&cid=SHOHD2VWANZNDNGC",
    "https://www.flipkart.com/nike-killshot-2-leather-sneakers-men/p/itm28a181bebad18?pid=SHOHFF25UUMSAKZY&lid=LSTSHOHFF25UUMSAKZYGFDTPW&marketplace=FLIPKART&q=killshot+2&store=osp%2Fcil%2Fe1f&srno=s_1_1&otracker=search&otracker1=search&fm=Search&iid=565272a9-a421-4a4d-a80b-bb66db86d404.SHOHFF25UUMSAKZY.SEARCH&ppt=sp&ppn=sp&ssid=dscy7gk029gx5iww1757505100423&qH=5a1643cee092c11e",
]

# Saving the data to cloud using abfss protocol to adls
from src.utils import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def run_flipkart_scraper(spark=None, urls=None):
    if urls is None: urls = flipkart_urls
    if spark is None:
        from src.utils import get_spark_session
        spark, _ = get_spark_session()
    
    logger = get_logger(spark, "flipkart_scraper")
    all_products = []

    for url in urls:
        data = scrape_flipkart_product(url)
        if data:
            all_products.append(data)

    if not all_products:
        logger.warn("No data scraped successfully.")
        return None

    # --- DEBUG SECTION ---
    first_row_keys = all_products[0].keys()
    print(f"DEBUG: Keys found in first product: {list(first_row_keys)}")
    
    if "mrp" in first_row_keys:
        print("CRITICAL: Your scraper is STILL returning 'mrp'!")
    # ---------------------

    custom_schema = StructType([
        StructField("product_name", StringType(), True),
        StructField("selling_price", StringType(), True),
        StructField("status", StringType(), True),
        StructField("mrp", StringType(), True), 
        StructField("brand", StringType(), True),
        StructField("website", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("url", StringType(), True),
    ])

    return spark.createDataFrame(all_products, custom_schema)

 


# testing
# from dotenv import load_dotenv

# if __name__ == "__main__":
#     load_dotenv()

#     from .utils import get_spark_session

#     spark,config= get_spark_session()
#     run_flipkart_scraper(spark, flipkart_urls)
#     spark.stop()
