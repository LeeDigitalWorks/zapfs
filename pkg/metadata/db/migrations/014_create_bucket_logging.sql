-- Bucket logging configuration
-- Stores which buckets have access logging enabled and where to deliver logs

CREATE TABLE IF NOT EXISTS bucket_logging (
    id VARCHAR(36) PRIMARY KEY,
    source_bucket VARCHAR(255) NOT NULL,
    target_bucket VARCHAR(255) NOT NULL,
    target_prefix VARCHAR(255) DEFAULT '',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,

    -- Each source bucket can only have one logging configuration
    UNIQUE INDEX idx_source_bucket (source_bucket),

    -- Find all buckets logging to a specific target
    INDEX idx_target_bucket (target_bucket)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Track last export time per bucket for S3 delivery
CREATE TABLE IF NOT EXISTS bucket_logging_exports (
    source_bucket VARCHAR(255) PRIMARY KEY,
    last_export_time DATETIME(3) NOT NULL,
    last_export_key VARCHAR(1024),

    INDEX idx_last_export (last_export_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
