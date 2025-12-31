from delta.tables import DeltaTable

def write_gold_alerts(signal_df, gold_path):
    spark = signal_df.sparkSession

    # 1. Deduplicate incoming batch
    new_data = signal_df.dropDuplicates(["url", "website", "date"])

    # 2. Idempotent write
    if DeltaTable.isDeltaTable(spark, gold_path):
        delta_table = DeltaTable.forPath(spark, gold_path)

        (
            delta_table.alias("t")
            .merge(
                new_data.alias("s"),
                "t.url = s.url AND t.website = s.website AND t.date = s.date"
            )
            .whenNotMatchedInsert(values={
                "url": "s.url",
                "website": "s.website",
                "date": "s.date",
                "brand": "s.brand",
                "product_name": "s.product_name",
                "final_price": "s.final_price",
                "avg_price_7d": "s.avg_price_7d",
                "Discount_Percentage": "s.Discount_Percentage",
                "signal": "s.signal"
            })
            .execute()
        )

    else:
        # First-time table creation
        new_data.write.format("delta").mode("overwrite").save(gold_path)
