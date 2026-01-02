-- Bucket logging configuration
-- Stores which buckets have access logging enabled and where to deliver logs

CREATE TABLE IF NOT EXISTS bucket_logging (
    id VARCHAR(36) PRIMARY KEY,
    source_bucket VARCHAR(255) NOT NULL,
    target_bucket VARCHAR(255) NOT NULL,
    target_prefix VARCHAR(255) DEFAULT '',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

-- Each source bucket can only have one logging configuration
CREATE UNIQUE INDEX IF NOT EXISTS idx_bucket_logging_source ON bucket_logging(source_bucket);

-- Find all buckets logging to a specific target
CREATE INDEX IF NOT EXISTS idx_bucket_logging_target ON bucket_logging(target_bucket);

-- Track last export time per bucket for S3 delivery
CREATE TABLE IF NOT EXISTS bucket_logging_exports (
    source_bucket VARCHAR(255) PRIMARY KEY,
    last_export_time TIMESTAMPTZ(3) NOT NULL,
    last_export_key VARCHAR(1024)
);

CREATE INDEX IF NOT EXISTS idx_bucket_logging_exports_time ON bucket_logging_exports(last_export_time);
