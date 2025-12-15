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
import shutil

# ---------------- LOGGER SETUP ----------------
os.makedirs("logs", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)

logging.getLogger("WDM").setLevel(logging.WARNING)
logging.getLogger("webdriver_manager").setLevel(logging.CRITICAL)

file_handler = logging.FileHandler(
    "/home/moksh/Desktop/smart-price-tracker/logs/ajio_scraper.log"
)
console_handler = logging.StreamHandler(sys.stdout)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])
logger = logging.getLogger("ajio_scraper")


# -------------------------------------------------
def fetch_ajio_product(url):
    logger.info(f"Fetching product from {url}")
    options = Options()
    user_data_dir = tempfile.mkdtemp(prefix="chrome_user_data_")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0.5993.118 Safari/537.36"
    )
    options.add_argument("--headless=new")
    options.add_argument(f"--user-data-dir={user_data_dir}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.headless = True  # Uncomment for silent scraping
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

    try:
        driver.get(url)

        # Wait for product name to load (up to 20 seconds)
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.prod-name")))

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # ---- Extract Data ----
        product_name = (
            soup.find("h1", class_="prod-name").get_text(strip=True)
            if soup.find("h1", class_="prod-name")
            else None
        )
        brand = (
            soup.find("h2", class_="brand-name").get_text(strip=True)
            if soup.find("h2", class_="brand-name")
            else None
        )

        prod_price_div = soup.find("div", class_="prod-price-sec") or soup.find(
            "div", class_="prod-price"
        )
        sp_price_div = soup.find("div", class_="prod-sp")

        add_to_bag_button = soup.find("div", class_="pdp-addtocart-button")

        mrp = None
        price = None
        status = "In Stock"

        if add_to_bag_button:
            button_text = add_to_bag_button.get_text(strip=True).lower()
            if "out of stock" in button_text or "sold out" in button_text:
                status = "Out of Stock"
                logger.warning(f"Product is out of stock {product_name.text.strip()}")
        elif add_to_bag_button is None:
            status = "Out of Stock"
            logger.warning(f"Product is out of stock {product_name.text.strip()}")

        if prod_price_div and sp_price_div:
            mrp_tag = prod_price_div.find(
                "span", class_="prod-cp"
            ) or prod_price_div.find("span", class_="prod-mrp")
            mrp = mrp_tag.get_text(strip=True) if mrp_tag else None
            price = sp_price_div.get_text(strip=True)
        elif prod_price_div:
            mrp_tag = prod_price_div.find(
                "span", class_="prod-cp"
            ) or prod_price_div.find("span", class_="prod-mrp")
            mrp = mrp_tag.get_text(strip=True) if mrp_tag else None
            price = mrp
        elif sp_price_div:
            price = sp_price_div.get_text(strip=True)
            mrp = price
        else:
            logger.warning(f"Price not found for {url}")

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if not product_name:
            logger.warning(f"Product name not found for {url}")

        data = {
            "product_name": product_name,
            "mrp": mrp,
            "status": status,
            "price": price,
            "brand": brand,
            "website": "ajio",
            "timestamp": timestamp,
            "url": url,
        }

        logger.info(
            f"Scraped Ajio Product: {product_name or 'Unknown'} | Price: {price}"
        )
        return data

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return {"error": str(e), "url": url}

    finally:
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


def run_ajio_scraper(spark: SparkSession, urls: list) -> str:
    all_products = []

    # 1. Scrape data into a Python list (This replaces your 'for url in ...' loop)
    for url in urls:
        data = fetch_ajio_product(url)
        if data:
            all_products.append(data)

    if not all_products:
        logger.warning("No data scraped successfully. Skipping write to cloud.")
        return ""

    # Define the schema where all fields are strings
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

    # Inside run_flipkart_scraper:
    # raw_df = spark.createDataFrame(all_products, schema=custom_schema)

    # 2. Convert the list of dictionaries into a Spark DataFrame
    # Note: Spark can infer the schema from the list of dicts.
    raw_df = spark.createDataFrame(all_products, custom_schema)

    # 3. Construct the ADLS Gen2 Path for the RAW layer
    from pyspark.sql.utils import AnalysisException

    # --- IN ajio.py (inside run_ajio_scraper) ---
    # ... (Previous code to sanitize data and create raw_df - the NEW data) ...
    base_raw_path = adls_path("raw")
    site_folder = "ajio"  # <-- UNIQUE FOLDER NAME for Ajio data

    # Define the final output path as a DIRECTORY: abfss://.../Data/raw/ajio/
    full_output_path = f"{base_raw_path}{site_folder}/"
    print(f"--- Starting Scalable APPEND Write for Ajio ---")
    print(f"Appending new records to ADLS Gen2 directory: {full_output_path}")

    # Use the standard Data Lake method: Append new data to the directory.
    raw_df.write.mode("append").format("json").save(full_output_path)

    print(f"--- Write Complete: New files added to the 'ajio' folder ---")


if __name__ == "__main__":
    # You need to pass the URLs list (defined earlier in the file)
    from .utils import get_spark_session

    # 1. Create the session
    spark, config = get_spark_session("DirectScraperTest")

    # 2. Run the main scraping function (using the global ajio_urls list)
    run_ajio_scraper(spark, ajio_urls)

# 3. Stop the session
spark.stop()
