"""Clean, type, validate and deduplicate Bronze data into Silver Delta tables."""
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from src.utils.data_quality import add_quality_columns, split_valid_invalid

TABLE_KEYS = {
    "users": ["user_id"],
    "videos": ["video_id"],
    "watch_events": ["event_id"],
    "subscriptions": ["subscription_event_id"],
}

def _read_delta(spark, path):
    return spark.read.format("delta").load(path)

def transform_silver(spark, bronze_path: str, silver_path: str):
    outputs = {}

    users = _read_delta(spark, f"{bronze_path}/users")
    users = (users
        .withColumn("user_id", F.trim("user_id"))
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("plan_type", F.lower(F.trim("plan_type")))
        .withColumn("signup_date", F.to_date("signup_date")))
    users = add_quality_columns(users, ["user_id", "country", "plan_type", "signup_date"])
    valid, invalid = split_valid_invalid(users)
    valid = valid.dropDuplicates(TABLE_KEYS["users"])
    valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/users")
    invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/quarantine_users")
    outputs["users"] = f"{silver_path}/users"

    videos = _read_delta(spark, f"{bronze_path}/videos")
    videos = (videos
        .withColumn("video_id", F.trim("video_id"))
        .withColumn("title", F.trim("title"))
        .withColumn("category", F.initcap(F.trim("category")))
        .withColumn("duration_seconds", F.col("duration_seconds").cast(IntegerType()))
        .withColumn("release_year", F.col("release_year").cast(IntegerType())))
    videos = add_quality_columns(videos, ["video_id", "title", "category", "duration_seconds", "release_year"])
    videos = videos.withColumn("is_valid", F.col("is_valid") & (F.col("duration_seconds") > 0))
    valid, invalid = split_valid_invalid(videos)
    valid.dropDuplicates(TABLE_KEYS["videos"]).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/videos")
    invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/quarantine_videos")
    outputs["videos"] = f"{silver_path}/videos"

    events = _read_delta(spark, f"{bronze_path}/watch_events")
    events = (events
        .withColumn("event_id", F.trim("event_id"))
        .withColumn("user_id", F.trim("user_id"))
        .withColumn("video_id", F.trim("video_id"))
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("watch_duration_seconds", F.col("watch_duration_seconds").cast(IntegerType()))
        .withColumn("device_type", F.lower(F.trim("device_type"))))
    events = add_quality_columns(events, ["event_id", "user_id", "video_id", "event_timestamp", "watch_duration_seconds"])
    events = events.withColumn("is_valid", F.col("is_valid") & (F.col("watch_duration_seconds") > 0))
    valid, invalid = split_valid_invalid(events)
    valid.dropDuplicates(TABLE_KEYS["watch_events"]).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/watch_events")
    invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/quarantine_watch_events")
    outputs["watch_events"] = f"{silver_path}/watch_events"

    subs = _read_delta(spark, f"{bronze_path}/subscriptions")
    subs = (subs
        .withColumn("subscription_event_id", F.trim("subscription_event_id"))
        .withColumn("user_id", F.trim("user_id"))
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("plan_type", F.lower(F.trim("plan_type")))
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp")))
    subs = add_quality_columns(subs, ["subscription_event_id", "user_id", "event_type", "event_timestamp"])
    subs = subs.withColumn("is_valid", F.col("is_valid") & F.col("event_type").isin("started", "cancelled"))
    valid, invalid = split_valid_invalid(subs)
    valid.dropDuplicates(TABLE_KEYS["subscriptions"]).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/subscriptions")
    invalid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/quarantine_subscriptions")
    outputs["subscriptions"] = f"{silver_path}/subscriptions"

    return outputs
