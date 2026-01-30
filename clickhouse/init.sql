CREATE DATABASE IF NOT EXISTS bts;

CREATE TABLE IF NOT EXISTS bts.fan_events (
    event_time DateTime,
    user_id UInt64,
    content_id UInt32,
    content_type String,
    member_bias String,
    event_type String,
    watch_time_sec UInt16,
    source String,
    mode String
) ENGINE = MergeTree
ORDER BY event_time;