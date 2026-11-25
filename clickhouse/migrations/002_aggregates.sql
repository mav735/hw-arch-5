CREATE TABLE IF NOT EXISTS cinema.daily_metrics
(
    metric_date Date,
    metric_name LowCardinality(String),
    dimension String,
    value Float64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, metric_name, dimension);

CREATE TABLE IF NOT EXISTS cinema.daily_top_movies
(
    metric_date Date,
    movie_id String,
    views UInt64,
    unique_viewers UInt64,
    rank UInt32,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, movie_id);

CREATE TABLE IF NOT EXISTS cinema.cohort_retention
(
    cohort_date Date,
    day_number UInt8,
    cohort_size UInt64,
    retained_users UInt64,
    retention_rate Float64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (cohort_date, day_number);
