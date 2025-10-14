from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime
import sys, os, logging, json

# ---------------- LOGGER SETUP ----------------
os.makedirs("logs", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)

logging.getLogger("WDM").setLevel(logging.WARNING)
logging.getLogger("webdriver_manager").setLevel(logging.CRITICAL)

file_handler = logging.FileHandler("logs/ajio_scraper.log", mode="a", encoding="utf-8")
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
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0.5993.118 Safari/537.36"
    )
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    # options.headless = True  # Uncomment for silent scraping
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
            mrp_tag = prod_price_div.find("span", class_="prod-cp") or prod_price_div.find(
                "span", class_="prod-mrp"
            )
            mrp = mrp_tag.get_text(strip=True) if mrp_tag else None
            price = sp_price_div.get_text(strip=True)
        elif prod_price_div:
            mrp_tag = prod_price_div.find("span", class_="prod-cp") or prod_price_div.find(
                "span", class_="prod-mrp"
            )
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
            "status":status,
            "price": price,
            "brand": brand,
            "website": "ajio",
            "timestamp": timestamp,
            "url": url,
        }

        logger.info(f"Scraped Ajio Product: {product_name or 'Unknown'} | Price: {price}")
        return data

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return {"error": str(e), "url": url}

    finally:
        driver.quit()


############################# Main Method ########################
if __name__ == "__main__":
    ajio_urls = [
        "https://www.ajio.com/nike-men-c1ty-low-top-lace-up-basket-ball-shoes/p/469695776_green",
        "https://www.ajio.com/nike-downshifter-13-running-shoes/p/469581864_black?",
        "https://www.ajio.com/nike-field-general-running-shoes/p/469763433_blackgrey?",
        "https://www.ajio.com/nike-men-killshot-2-leather-lace-up-tennis-shoes/p/469759270_white?"
    ]

    file_path = "data/raw/ajio_products.json"

    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                all_products = json.load(f)
        except json.JSONDecodeError:
            all_products = []
    else:
        all_products = []

    for url in ajio_urls:
        data = fetch_ajio_product(url)
        if data:
            all_products.append(data)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(all_products, f, ensure_ascii=False, indent=4)

    logger.info(f"Saved {len(all_products)} products to {file_path}")
    print(f"Saved {len(all_products)} products to {file_path}")
