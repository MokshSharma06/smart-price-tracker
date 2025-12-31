# 🛒 Smart Price Tracker

Production-style Data Engineering Pipeline (Spark + Delta + ADLS)

Smart Price Tracker is an end-to-end data engineering pipeline that scrapes product prices from multiple e-commerce platforms, cleans and normalizes them, builds idempotent Silver aggregates, and generates Gold BUY alerts based on price-drop logic.

## 🏗 Architecture

![Smart Price Tracker Architecture](docs/architecture.png)

The project is built using Medallion Architecture (Bronze → Silver → Gold) with strong emphasis on schema contracts, canonical pricing, idempotency, and testability.

# Medallion Architecture of the project
```
Scrapers (Flipkart, Ajio)
        │
        ▼
In-Memory Staging (Spark DataFrame)
        │
        ▼
🥉 Bronze / Raw (ADLS)
• Cleaned numeric prices
• Validated schema
• Replay-safe storage
        │
        ▼
🥈 Silver / Processed (Delta)
• Canonical final_price
• Forward-filled MRP
• Daily aggregates
• Idempotent merges
        │
        ▼
🥇 Gold / Alerts (Delta)
• BUY signals
• Minimal alert-focused schema


```

# How to run project Locally
```
git clone <repo>
cd smart-price-tracker
conda env create -f environment.yml
conda activate smart-price-tracker
python main.py
pytest -v
```


# 📂 Project Structure
```
smart-price-tracker/
├── artifacts/                     # Build / pipeline artifacts
│
├── conf/
│   ├── config.yaml                # Azure & pipeline config
│   ├── spark.conf                 # Spark runtime config
│   ├── log4j.properties           # Logging (prod)
│   └── log4j-ci.properties        # Logging (CI)
│
├── data/                          # Local data (gitignored)
│   ├── raw/                       # Bronze layer
│   │   ├── ajio_products.json
│   │   └── flipkart_products.json
│   │
│   ├── processed/                # Silver outputs
│   │   ├── processed_data/
│   │   └── test_data/
│   │
│   ├── delta/                    # Delta tables
│   └── delta_data/
│       └── curated_data/
│           └── _delta_log/
│
├── logs/                          # Application & scraper logs
│   ├── application/
│   ├── flipkart.log*
│   └── ajio.log*
│
├── src/
│   ├── configloader.py            # Configuration loader
│   ├── data_loader.py             # Silver layer writer (idempotent)
│   ├── fetch_prices.py            # Flipkart scraper
│   ├── fetch_price_ajio.py        # Ajio scraper
│   ├── process_data.py            # Cleaning & transformations
│   ├── logic.py                   # Gold signal logic
│   ├── gold_loader.py             # Gold Delta writer
│   ├── logger.py                  # Centralized logging
│   ├── utils.py                   # Shared utilities
│   └── delta_loader.py            # (Deprecated)
│
├── tests/
│   ├── conftest.py
│   ├── test_process_data.py       # Transformation tests
│   ├── test_utils.py              # Utility tests
│   └── test_delta_loader.py       # (Deprecated)
│
├── main.py                        # Pipeline entrypoint
├── Dockerfile                     # package and Containerization  
├── docker-compose.yml
├── environment.yml                # Conda environment
├── run.sh                         # Local runner
└── README.md

```
⚙️ Tech Stack

Python 3.9+
PySpark – batch data processing
Delta Lake – idempotency
YAML configs – for flexible pipeline settings
Unit Testing (pytest) – test-driven modules
Logging (log4j + Python logger)
Github Actions – CI/CD automation

🔑 Features

Fetch product price data (simulated for 5–7 products).
Store raw → processed → delta layers.
Config-driven pipeline (no hardcoded values).
Unit tested modules for reliability.
Scalable design – can later be deployed to cloud.

👨‍💻 Author
Moksh Sharma – Aspiring Data Engineer | BCA Graduate | Azure & PySpark Enthusiast | DP- 900 
