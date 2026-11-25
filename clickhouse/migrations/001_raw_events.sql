CREATE DATABASE IF NOT EXISTS cinema;

CREATE TABLE IF NOT EXISTS cinema.movie_events_queue
(
    event_id String,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 3,
    kafka_handle_error_mode = 'stream';

-- ReplacingMergeTree collapses duplicates from at-least-once delivery (same event_id).
CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id UUID,
    user_id String,
    movie_id LowCardinality(String),
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(timestamp),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Nullable(UInt32),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv TO cinema.movie_events AS
SELECT
    toUUID(event_id) AS event_id,
    user_id,
    movie_id,
    event_type,
    timestamp,
    device_type,
    session_id,
    CAST(progress_seconds, 'Nullable(UInt32)') AS progress_seconds
FROM cinema.movie_events_queue
WHERE length(_error) = 0;

CREATE TABLE IF NOT EXISTS cinema.movie_events_errors
(
    topic String,
    partition UInt64,
    offset UInt64,
    raw String,
    error String,
    received_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY received_at
TTL received_at + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_errors_mv TO cinema.movie_events_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset, _raw_message AS raw, _error AS error
FROM cinema.movie_events_queue
WHERE length(_error) > 0;
