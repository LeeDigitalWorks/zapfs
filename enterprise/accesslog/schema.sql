-- ZapFS ClickHouse Schema for Access Logging
-- This schema creates the database and table for storing S3 access logs.
--
-- This file is the single source of truth for the ClickHouse schema.
-- It is embedded in Go code and also used by Docker init.

CREATE DATABASE IF NOT EXISTS zapfs;

CREATE TABLE IF NOT EXISTS zapfs.access_logs (
    event_time DateTime64(3) CODEC(Delta, ZSTD(1)),
    request_id String CODEC(ZSTD(1)),
    bucket LowCardinality(String),
    object_key String CODEC(ZSTD(1)),
    owner_id LowCardinality(String),
    requester_id String CODEC(ZSTD(1)),
    remote_ip IPv4,
    operation LowCardinality(String),
    http_method LowCardinality(String),
    http_status UInt16,
    bytes_sent UInt64,
    object_size UInt64,
    total_time_ms UInt32,
    turn_around_time_ms UInt32,
    signature_version LowCardinality(String),
    tls_version LowCardinality(String),
    auth_type LowCardinality(String),
    user_agent String CODEC(ZSTD(1)),
    referer String CODEC(ZSTD(1)),
    host_header String CODEC(ZSTD(1)),
    request_uri String CODEC(ZSTD(1)),
    error_code LowCardinality(String),
    version_id String CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (bucket, event_time, request_id)
TTL event_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
