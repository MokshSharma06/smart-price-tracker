from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, coalesce

def write_gold_alerts(signal_df, gold_path):
    spark = signal_df.sparkSession

    new_data = signal_df.dropDuplicates(["url", "website", "date"])

    if DeltaTable.isDeltaTable(spark, gold_path):
        delta_table = DeltaTable.forPath(spark, gold_path)

        (
            delta_table.alias("t")
            .merge(
                new_data.alias("s"),
                "t.url = s.url AND t.website = s.website AND t.date = s.date"
            )
            .whenMatchedUpdate(set={
                "closing_price": "s.closing_price",
                "min_price_day": "s.min_price_day",
                "max_price_day": "s.max_price_day",
                "avg_price_day": "s.avg_price_day",
                "avg_discount_day": "s.avg_discount_day",
                "num_scrapes_day": "s.num_scrapes_day", 
                "avg_price_7d": "s.avg_price_7d",
                "signal": "s.signal",
                "last_scrape_time": "s.last_scrape_time",
                "status": "s.status"
            })
            .whenNotMatchedInsert(values={
                "url": "s.url",
                "website": "s.website",
                "date": "s.date",
                "brand": "s.brand",
                "product_name": "s.product_name",
                "closing_price": "s.closing_price",
                "min_price_day": "s.min_price_day",
                "max_price_day": "s.max_price_day",
                "avg_price_day": "s.avg_price_day",
                "avg_discount_day": "s.avg_discount_day",
                "num_scrapes_day": "s.num_scrapes_day",
                "mrp_final": "s.mrp_final",
                "avg_price_7d": "s.avg_price_7d",
                "signal": "s.signal",
                "last_scrape_time": "s.last_scrape_time",
                "status": "s.status"
            })
            .execute()
        )
    else:
        (
            new_data
            .withColumn(
                "num_scrapes_day",
                coalesce(col("num_scrapes_day"), lit(1))
            )
            .write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )
