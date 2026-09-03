CREATE DATABASE IF NOT EXISTS Monitoring;

CREATE TABLE IF NOT EXISTS  Monitoring.api_performance_logs
(
    timestamp DateTime64(6) CODEC(Delta, ZSTD),
    user_id UUID CODEC(ZSTD),
    request_id UUID CODEC(ZSTD),
    response_time_ms Float32 CODEC(Gorilla, ZSTD),
    -- spark_latency_ms Float32 CODEC(Gorilla, ZSTD),
    status LowCardinality(String) CODEC(ZSTD),
    -- http_status_code UInt8 CODEC(ZSTD),
    final_price Float32 CODEC(ZSTD),
    request_size Int32 CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (timestamp, status);