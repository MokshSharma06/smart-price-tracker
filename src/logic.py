from pyspark.sql import functions as F
from pyspark.sql.window import Window

def generate_signals(silver_daily):
    """
    Generates BUY alerts from DAILY SNAPSHOT silver data.
    One row per (url, website, date).
    """

    window_spec = (
        Window
        .partitionBy("url", "website")
        .orderBy("date")
        .rowsBetween(-7, -1)
    )

    df = silver_daily.withColumn(
        "avg_price_7d",
        F.round(F.avg("closing_price").over(window_spec), 2)
    )

    df = df.withColumn(
        "signal",
        F.when(
            (
                F.col("avg_price_7d").isNotNull() &
                F.col("closing_price").isNotNull() &
                (F.col("closing_price") <= F.col("avg_price_7d") * 0.9)
            ) |
            (
                F.col("avg_discount_day").isNotNull() &
                (F.col("avg_discount_day") >= 10)
            ),
            "BUY"
        ).otherwise("NO_BUY")
    )

    return df.filter(
        (F.col("date") == F.current_date()) &
        (F.col("signal") == "BUY")
    )

import requests
import json
import os


def send_alert(row):
    LOGIC_APP_URL = os.getenv("LOGIC_APP_ALERT_URL")
    if not LOGIC_APP_URL:
        raise RuntimeError("logic_app_url not found or set")

    data_to_send = {
        "url": row.url,
        "product_name": row.product_name,
        "brand": row.brand,
        "closing_price": row.closing_price,
        "mrp_final": row.mrp_final,
        "avg_discount_day": row.avg_discount_day,
        "num_scrapes_day": row.num_scrapes_day,
        "signal": row.signal,
        "date": str(row.date)
    }

    response = requests.post(
        LOGIC_APP_URL,
        headers={"Content-Type": "application/json"},
        json=data_to_send
    )

    response.raise_for_status()
