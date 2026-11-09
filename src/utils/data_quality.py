"""Reusable PySpark data quality helpers."""
from pyspark.sql import functions as F

def add_quality_columns(df, required_columns):
    """Add a boolean flag and readable reason for missing required values."""
    missing_checks = [
        F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        for c in required_columns
    ]
    any_missing = missing_checks[0]
    for check in missing_checks[1:]:
        any_missing = any_missing | check

    reason = F.concat_ws(
        ", ",
        *[
            F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), F.lit(c))
            for c in required_columns
        ]
    )
    return (df.withColumn("is_valid", ~any_missing)
              .withColumn("dq_error_reason", F.when(any_missing, reason)))

def split_valid_invalid(df):
    return df.filter(F.col("is_valid")), df.filter(~F.col("is_valid"))
