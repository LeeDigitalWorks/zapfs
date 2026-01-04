-- Create bucket_intelligent_tiering table
-- Stores S3 Intelligent-Tiering configurations per bucket
-- Each bucket can have up to 1000 configurations (per AWS limit)

CREATE TABLE IF NOT EXISTS bucket_intelligent_tiering (
    id VARCHAR(36) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    config_id VARCHAR(64) NOT NULL COMMENT 'User-provided configuration ID',
    config_json JSON NOT NULL COMMENT 'IntelligentTieringConfiguration as JSON',
    created_at BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (id),
    UNIQUE INDEX idx_bucket_config (bucket, config_id),
    INDEX idx_bucket (bucket)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
