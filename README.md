# Smart Price Tracker

A batch data pipeline to fetch, clean, and track product prices over time.  
This project will first run locally, then later be deployed to the cloud.

📂 Project Structure
smart-price-tracker/
├── conf/                 # Config files
│   ├── config.yaml       # Pipeline configuration
│   └── spark.conf        # Spark session configs
├── data/                 # Data layers (ignored in Git)
│   ├── raw/              # Raw data fetched from source
│   ├── processed/        # Cleaned & transformed data
│   └── delta/            # Delta tables for tracking history
├── environment.yml       # Conda environment file
├── JenkinsFile           # CI/CD pipeline (Jenkins)
├── log4j.properties      # Logging configuration
├── main.py               # Pipeline entrypoint
├── src/                  # Source code modules
│   ├── configloader.py   # Config reader
│   ├── delta_loader.py   # Delta lake writer/reader
│   ├── fetch_prices.py   # Fetch product prices
│   ├── logger.py         # Central logging
│   ├── process_data.py   # Data cleaning & transformations
│   └── utils.py          # Utility functions
└── tests/                # Unit tests
    ├── test_delta_loader.py
    ├── test_fetch_prices.py
    ├── test_process_data.py
    └── test_utils.py

⚙️ Tech Stack

Python 3.9+
PySpark – batch data processing
Delta Lake – maintain historical product prices
YAML configs – for flexible pipeline settings
Unit Testing (pytest) – test-driven modules
Logging (log4j + Python logger)
Jenkins – CI/CD automation

🔑 Features

Fetch product price data (simulated for 5–7 products).
Store raw → processed → delta layers.
Config-driven pipeline (no hardcoded values).
Unit tested modules for reliability.
Scalable design – can later be deployed to cloud.

👨‍💻 Author
Moksh Sharma – Aspiring Data Engineer | BCA Graduate | Azure & PySpark Enthusiast
