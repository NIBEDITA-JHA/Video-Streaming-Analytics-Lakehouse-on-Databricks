"""Entry point for the batch Medallion pipeline."""
from src.ingestion.bronze_ingestion import ingest_bronze
from src.transformations.silver_transformations import transform_silver
from src.analytics.gold_aggregations import build_gold

def run_pipeline(spark, source_path: str, base_path: str):
    base_path = base_path.rstrip("/")
    bronze_path = f"{base_path}/bronze"
    silver_path = f"{base_path}/silver"
    gold_path = f"{base_path}/gold"

    print("Starting Bronze ingestion...")
    bronze = ingest_bronze(spark, source_path, bronze_path)
    print("Bronze complete:", bronze)

    print("Starting Silver transformations...")
    silver = transform_silver(spark, bronze_path, silver_path)
    print("Silver complete:", silver)

    print("Building Gold analytics...")
    gold = build_gold(spark, silver_path, gold_path)
    print("Gold complete:", gold)

    return {"bronze": bronze, "silver": silver, "gold": gold}
