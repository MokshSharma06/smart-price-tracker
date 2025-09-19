from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time

def fetch_ajio_product(url):
    """
    Fetch product details from an Ajio product page.
    
    Returns a dictionary:
    {
        "name": str,
        "mrp": str,
        "price": str,
        "discount": str,
        "url": str
    }
    """
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
        name = name_tag.get_text(strip=True) if name_tag else None
        
        # Price / MRP / Discount
        prod_price_div = soup.find("div", class_="prod-price-sec") or soup.find("div", class_="prod-price")
        sp_price_div = soup.find("div", class_="prod-sp")
        
        mrp = None
        discount = None
        price = None

        if prod_price_div:
            mrp_tag = prod_price_div.find("span", class_="prod-cp")
            discount_tag = prod_price_div.find("span", class_="prod-discount")
            mrp = mrp_tag.get_text(strip=True) if mrp_tag else None
            discount = discount_tag.get_text(strip=True) if discount_tag else None

        if sp_price_div:
            price = sp_price_div.get_text(strip=True)
        
        return {
            "name": name,
            "mrp": mrp,
            "price": price,
            "discount": discount,
            "url": url
        }
    
    except Exception as e:
        return {"error": str(e), "url": url}
    
    finally:
        driver.quit()


# Example usage
if __name__ == "__main__":
    url = "https://www.ajio.com/nike-men-c1ty-low-top-lace-up-basket-ball-shoes/p/469695776_green"
    product = fetch_ajio_product(url)
    print(product)
