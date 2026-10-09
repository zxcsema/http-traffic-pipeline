CREATE DATABASE IF NOT EXISTS traffic;

CREATE TABLE IF NOT EXISTS traffic.raw_http_logs
(
    source_date Date,
    request_id String,
    timestamp String,
    ip String,
    method LowCardinality(String),
    path String,
    status_code LowCardinality(String),
    response_ms String,
    user_agent LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(source_date)
ORDER BY (source_date, path, request_id);

CREATE TABLE IF NOT EXISTS traffic.traffic_daily_etl
(
    event_date Date,
    path String,
    requests UInt64,
    unique_ips UInt64,
    errors_4xx UInt64,
    errors_5xx UInt64,
    error_rate Float64,
    avg_response_ms Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, path);

CREATE TABLE IF NOT EXISTS traffic.traffic_daily_elt
(
    event_date Date,
    path String,
    requests UInt64,
    unique_ips UInt64,
    errors_4xx UInt64,
    errors_5xx UInt64,
    error_rate Float64,
    avg_response_ms Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, path);
