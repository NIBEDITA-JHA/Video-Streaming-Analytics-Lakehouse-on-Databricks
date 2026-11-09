"""Read source CSV files and persist raw Bronze Delta tables."""
from pyspark.sql import functions as F

SOURCE_FILES = {
    "users": "users.csv",
    "videos": "videos.csv",
    "watch_events": "watch_events.csv",
    "subscriptions": "subscriptions.csv",
}

def ingest_bronze(spark, source_path: str, bronze_path: str):
    results = {}
    for table_name, filename in SOURCE_FILES.items():
        df = (spark.read
              .option("header", True)
              .option("inferSchema", False)
              .csv(f"{source_path.rstrip('/')}/{filename}")
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.input_file_name()))
        target = f"{bronze_path.rstrip('/')}/{table_name}"
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target))
        results[table_name] = target
    return results
