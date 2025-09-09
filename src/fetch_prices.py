import requests
import logging
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
import sys, os

site_name = "Flipkart"

# Create handlers
file_handler = logging.FileHandler("logs/flipkart_scraper.log")
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

logger = logging.getLogger("flipkart_scraper")


url = "https://www.flipkart.com/puma-softride-alexandria-wns-running-shoes-women/p/itm73a37010820b3?pid=SHOH33SDBRFDK5XX&lid=LSTSHOH33SDBRFDK5XXPJLTBA&marketplace=FLIPKART&q=shoes&store=osp&srno=s_1_1&otracker=search&otracker1=search&fm=Search&iid=127aeba1-ea89-4082-bc13-762f5cb0ebad.SHOH33SDBRFDK5XX.SEARCH&ppt=sp&ppn=sp&ssid=wsgzyrrbo4ucs2kg1756394844172&qH=b0a8b6f820479900"  # example product
def scrape_flipkart_product(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/126.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        # Selling price with fallbacks
        price_tag = soup.find("div", class_=["Nx9bqj", "CxhGGd"])
        if not price_tag:
            price_tag = soup.find(string=lambda x: x and "₹" in x)
        if not price_tag:
            match = re.search(r"₹\s?([\d,]+)", soup.get_text())
            if match:
                price_tag = match.group(0)

        # MRP tag
        mrp_tag = soup.find("div", class_=["yRaY8j", "_3I9_wc"])
        # Name & brand tags
        name_tag = soup.find("span", class_="VU-ZEz")
        brand_tag = soup.find("span", class_="mEh187")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if price_tag and name_tag and brand_tag:
            product_data = {
                "product_name": name_tag.text.strip(),
                "selling_price": price_tag.get_text(strip=True) if hasattr(price_tag, "get_text") else price_tag,
                "mrp_price": mrp_tag.get_text(strip=True) if mrp_tag else None,
                "brand": brand_tag.text.strip(),
                "website": site_name,
                "timestamp": timestamp,
                "url": url
            }
            logger.info(f"Scraped {site_name} | Product: {name_tag.text.strip()}")
            return product_data
        else:
            logger.warning(f"Price or product details not found on {url}")
            return None

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None
    

flipkart_urls =[
"https://www.flipkart.com/puma-softride-alexandria-wns-running-shoes-women/p/itm73a37010820b3?pid=SHOH33SDBRFDK5XX&lid=LSTSHOH33SDBRFDK5XXPJLTBA&marketplace=FLIPKART&q=shoes&store=osp&srno=s_1_1&otracker=search&otracker1=search&fm=Search&iid=127aeba1-ea89-4082-bc13-762f5cb0ebad.SHOH33SDBRFDK5XX.SEARCH&ppt=sp&ppn=sp&ssid=wsgzyrrbo4ucs2kg1756394844172&qH=b0a8b6f820479900",
"https://www.flipkart.com/nike-c1ty-sneakers-men/p/itm32724af11099d?pid=SHOHDH84NG2GVZ3Y&lid=LSTSHOHDH84NG2GVZ3YSEWE9H&marketplace=FLIPKART&fm=factBasedRecommendation%2FrecentlyViewed&iid=R%3Arv%3Bpt%3App%3Buid%3A769247bf-8d7a-11f0-9307-258b510dc13d%3B.SHOHDH84NG2GVZ3Y&ppt=pp&ppn=pp&ssid=wsgzyrrbo4ucs2kg1756394844172&otracker=pp_reco_Recently%2BViewed_4_38.productCard.RECENTLY_VIEWED_NIKE%2BC1TY%2BSneakers%2BFor%2BMen_SHOHDH84NG2GVZ3Y_factBasedRecommendation%2FrecentlyViewed_3&otracker1=pp_reco_PINNED_factBasedRecommendation%2FrecentlyViewed_Recently%2BViewed_DESKTOP_HORIZONTAL_productCard_cc_4_NA_view-all&cid=SHOHDH84NG2GVZ3Y"
]


all_products = []
for url in flipkart_urls:
    data = scrape_flipkart_product(url)
    if data:
        all_products.append(data)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
file_path = f"data/raw/flipkart_products_{timestamp}.json"

with open(file_path, "w", encoding="utf-8") as f:
    json.dump(all_products, f, ensure_ascii=False, indent=4)

print(f"Saved {len(all_products)} products to {file_path}")