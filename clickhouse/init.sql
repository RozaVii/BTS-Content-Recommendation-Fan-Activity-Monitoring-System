CREATE DATABASE IF NOT EXISTS bts;
SHOW DATABASES;

CREATE TABLE IF NOT EXISTS bts.fan_events
(
    event_time DateTime,
    event_id String,
    user_id UInt32,
    content_id UInt32,
    content_type LowCardinality(String),
    member_bias LowCardinality(String),
    event_type LowCardinality(String),
    watch_time_sec UInt16,
    source LowCardinality(String),
    mode LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (event_time, member_bias);

DESCRIBE TABLE bts.fan_events;
