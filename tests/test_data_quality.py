"""Small unit tests for reusable data quality helpers."""
from pyspark.sql import SparkSession
from src.utils.data_quality import add_quality_columns, split_valid_invalid

def test_required_field_validation():
    spark = SparkSession.builder.master("local[1]").appName("dq-test").getOrCreate()
    try:
        df = spark.createDataFrame([("U1", "IN"), (None, "US")], ["user_id", "country"])
        checked = add_quality_columns(df, ["user_id", "country"])
        valid, invalid = split_valid_invalid(checked)
        assert valid.count() == 1
        assert invalid.count() == 1
        assert invalid.first()["dq_error_reason"] == "user_id"
    finally:
        spark.stop()
