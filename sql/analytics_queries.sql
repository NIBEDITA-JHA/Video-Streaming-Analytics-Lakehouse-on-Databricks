-- Update the base path to your Delta table locations before running.

-- DAU trend
SELECT event_date, daily_active_users
FROM delta.`<base_path>/gold/daily_active_users`
ORDER BY event_date;

-- Watch time trend
SELECT event_date, total_watch_hours, avg_watch_seconds, total_views
FROM delta.`<base_path>/gold/daily_watch_time`
ORDER BY event_date;

-- Top 10 content by views
SELECT video_id, title, category, total_views, unique_viewers, total_watch_seconds
FROM delta.`<base_path>/gold/content_popularity`
ORDER BY total_views DESC
LIMIT 10;

-- Subscription growth
SELECT event_date, subscriptions_started, subscriptions_cancelled, net_subscription_change
FROM delta.`<base_path>/gold/subscription_growth`
ORDER BY event_date;

-- Cohort retention
SELECT cohort_date, days_since_signup, cohort_size, active_users, retention_rate
FROM delta.`<base_path>/gold/viewer_retention`
ORDER BY cohort_date, days_since_signup;
