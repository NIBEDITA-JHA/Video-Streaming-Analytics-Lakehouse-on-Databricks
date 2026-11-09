"""Generate deterministic synthetic source data for the lakehouse project."""
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

def write_csv(path, rows, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

def generate(output_dir: Path, seed: int = 42):
    random.seed(seed)
    now = datetime(2026, 10, 31, 23, 0, 0)
    users = []
    for i in range(1, 301):
        signup = now - timedelta(days=random.randint(1, 180))
        users.append({
            "user_id": f"U{i:05d}",
            "country": random.choice(["IN", "US", "GB", "CA", "AU"]),
            "plan_type": random.choice(["free", "basic", "premium"]),
            "signup_date": signup.date().isoformat()
        })

    videos = []
    categories = ["Drama", "Comedy", "Documentary", "Action", "Sci-Fi", "Kids"]
    for i in range(1, 81):
        videos.append({
            "video_id": f"V{i:04d}",
            "title": f"Streaming Title {i}",
            "category": random.choice(categories),
            "duration_seconds": random.randint(300, 7200),
            "release_year": random.randint(2015, 2026)
        })

    watch_events = []
    for i in range(1, 5001):
        user = random.choice(users)
        video = random.choice(videos)
        event_time = now - timedelta(days=random.randint(0, 59), hours=random.randint(0, 23),
                                     minutes=random.randint(0, 59))
        watch_events.append({
            "event_id": f"E{i:07d}",
            "user_id": user["user_id"],
            "video_id": video["video_id"],
            "event_timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "watch_duration_seconds": random.randint(10, min(video["duration_seconds"], 5400)),
            "device_type": random.choice(["mobile", "web", "tv", "tablet"])
        })

    subscriptions = []
    for i in range(1, 501):
        user = random.choice(users)
        event_time = now - timedelta(days=random.randint(0, 120), hours=random.randint(0, 23))
        event_type = random.choices(["started", "cancelled"], weights=[0.72, 0.28])[0]
        subscriptions.append({
            "subscription_event_id": f"S{i:06d}",
            "user_id": user["user_id"],
            "event_type": event_type,
            "plan_type": random.choice(["basic", "premium"]),
            "event_timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S")
        })

    write_csv(output_dir / "users.csv", users, list(users[0]))
    write_csv(output_dir / "videos.csv", videos, list(videos[0]))
    write_csv(output_dir / "watch_events.csv", watch_events, list(watch_events[0]))
    write_csv(output_dir / "subscriptions.csv", subscriptions, list(subscriptions[0]))
    print(f"Generated 4 CSV files in {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/sample")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate(Path(args.output_dir), args.seed)
