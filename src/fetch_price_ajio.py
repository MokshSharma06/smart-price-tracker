from selenium import webdriver
import tempfile
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime
import sys, os, logging, json
from src.logger import get_logger
from src.utils import get_spark_session
from pyvirtualdisplay import Display
import time
import random
import shutil


# -------------------------------------------------
def fetch_ajio_product(url):
    spark,config = get_spark_session()
    logger = get_logger(spark, "ajio_scraper")
    logger.info("starting ajio scraper")
    with Display(visible=0, size=(1920, 1080)) as disp:
        logger.info(f"Fetching product from {url}")
        
        # Creates a temporary directory for Chrome user data to avoid conflicts
        user_data_dir = tempfile.mkdtemp(prefix="chrome_user_data_")
        
        try:
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument(f"--user-data-dir={user_data_dir}")
            
            # Anti-bot and stealth settings
            options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/118.0.5993.118 Safari/537.36"
            )
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)

            # 3. Perform the Scraping
            driver.get(url)

            # Wait for the product name to load
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.prod-name")))

            soup = BeautifulSoup(driver.page_source, "html.parser")

            # ---- Extract Data ----
            product_name_elem = soup.find("h1", class_="prod-name")
            product_name = product_name_elem.get_text(strip=True) if product_name_elem else None
            
            brand_elem = soup.find("h2", class_="brand-name")
            brand = brand_elem.get_text(strip=True) if brand_elem else None

            prod_price_div = soup.find("div", class_="prod-price-sec") or soup.find("div", class_="prod-price")
            sp_price_div = soup.find("div", class_="prod-sp")
            add_to_bag_button = soup.find("div", class_="pdp-addtocart-button")

            mrp = None
            price = None
            status = "In Stock"

            # Check Availability
            if add_to_bag_button:
                button_text = add_to_bag_button.get_text(strip=True).lower()
                if "out of stock" in button_text or "sold out" in button_text:
                    status = "Out of Stock"
            else:
                status = "Out of Stock"

            # Price Extraction Logic
            if prod_price_div and sp_price_div:
                mrp_tag = prod_price_div.find("span", class_="prod-cp") or prod_price_div.find("span", class_="prod-mrp")
                mrp = mrp_tag.get_text(strip=True) if mrp_tag else None
                price = sp_price_div.get_text(strip=True)
            elif prod_price_div:
                mrp_tag = prod_price_div.find("span", class_="prod-cp") or prod_price_div.find("span", class_="prod-mrp")
                mrp = mrp_tag.get_text(strip=True) if mrp_tag else None
                price = mrp
            elif sp_price_div:
                price = sp_price_div.get_text(strip=True)
                mrp = price

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            data = {
                "product_name": product_name,
                "mrp": mrp,
                "status": status,
                "selling_price": price,
                "brand": brand,
                "website": "ajio",
                "timestamp": timestamp,
                "url": url,
            }

            logger.info(f"Scraped: {product_name} | Price: {price} | Status: {status}")
            return data

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return {"error": str(e), "url": url}

        finally:
            # 4. Clean up the driver before exiting the 'with' block
            if 'driver' in locals():
                driver.quit()
            shutil.rmtree(user_data_dir, ignore_errors=True)


ajio_urls = [
    "https://www.ajio.com/nike-men-c1ty-low-top-lace-up-basket-ball-shoes/p/469695776_green",
    "https://www.ajio.com/nike-downshifter-13-running-shoes/p/469581864_black?",
    "https://www.ajio.com/nike-field-general-running-shoes/p/469763433_blackgrey?",
    "https://www.ajio.com/nike-men-killshot-2-leather-lace-up-tennis-shoes/p/469759270_white?",
]


from src.utils import *
from pyspark.sql.types import StructType, StructField, StringType


def run_ajio_scraper(spark=None, urls=None) -> str:
    if urls is None:
        urls = ajio_urls
    # 2. Fallback to existing Spark session if none provided
    if spark is None:
        from src.utils import get_spark_session
        spark,config = get_spark_session()
        logger = get_logger(spark, "ajio_scraper")

    all_products = []
    for url in urls:
        data = fetch_ajio_product(url)
        if data:
            all_products.append(data)
        sleep_time = random.uniform(5, 12)
        print(f"Waiting for {sleep_time:.2f} seconds to avoid bot detection...")
        time.sleep(sleep_time)

    if not all_products:
        logger.warn("No data scraped successfully. Skipping write to cloud.")
        return ""

    # Defining custom schema to string so doesnt raises any type erro
    custom_schema = StructType(
        [
            StructField("product_name", StringType(), True),
            StructField("selling_price", StringType(), True),
            StructField("status", StringType(), True),
            StructField("mrp", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("website", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("url", StringType(), True),
        ]
    )

    raw_df = spark.createDataFrame(all_products, custom_schema)

    return raw_df

