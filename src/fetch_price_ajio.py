from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime
import sys, os, logging
import json

logging.getLogger("WDM").setLevel(logging.WARNING)
logging.getLogger("webdriver_manager").setLevel(logging.CRITICAL)

site_name = "ajio"
# lets set up the logger
# Create handlers
file_handler = logging.FileHandler("logs/ajio_scraper.log")
console_handler = logging.StreamHandler(sys.stdout)

# Define common format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Use basicConfig with handlers
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger("ajio_scraper")

def fetch_ajio_product(url):
    options = Options()
    options.headless = True
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    try:
        driver.get(url)
        
        # Wait until the price container appears (max 10 sec)
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "prod-sp")))

        # Parse the page
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Product name
        name_tag = soup.find("h1", class_="prod-name")
        product_name = name_tag.get_text(strip=True) if name_tag else None
        brand_tag = soup.find("h2",class_="brand-name")
        brand=brand_tag.get_text(strip=True) if brand_tag else None
        
        # Price / MRP / Discount
        prod_price_div = soup.find("div", class_="prod-price-sec") or soup.find("div", class_="prod-price")
        sp_price_div = soup.find("div", class_="prod-sp")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        
        mrp = None
        price = None


        if prod_price_div and sp_price_div:
            mrp_tag = prod_price_div.find("span", class_="prod-cp")
            mrp = mrp_tag.get_text(strip=True) if mrp_tag else None
            price = sp_price_div.get_text(strip=True)
            product_name = name_tag.text.strip() if name_tag else None

            logger.info(f"Scraped {site_name} | Product: {product_name}")

            return {
        "product_name": product_name,
        "mrp": mrp,
        "price": price,
        "brand": brand,
        "website": site_name,
        "timestamp": timestamp,
        "url": url
    }
        else:
            logger.warning(f"Price or product details not found on {url}")
            return None

    except Exception as e:
        return {"error": str(e), "url": url}
    
    finally:
        driver.quit()


# Example usage
if __name__ == "__main__":
    ajio_urls = [
        "https://www.ajio.com/nike-men-c1ty-low-top-lace-up-basket-ball-shoes/p/469695776_green",
        "https://www.ajio.com/nike-downshifter-13-running-shoes/p/469581864_black?"
    ]
#Saving the data to JSON file in the data/Raw folder
file_path = f"data/raw/ajio_products.json"

if os.path.exists(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            all_products = json.load(f)   # read old array
        except json.JSONDecodeError:
            all_products = []  # file empty or broken → reset
else:
    all_products = []  # first run → start empty

for url in ajio_urls:
    data = fetch_ajio_product(url)
    if data:
        all_products.append(data)



with open(file_path, "w", encoding="utf-8") as f:
    json.dump(all_products, f, ensure_ascii=False, indent=4)

print(f"Saved {len(all_products)} products to {file_path}")