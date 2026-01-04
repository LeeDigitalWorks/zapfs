-- Create bucket_intelligent_tiering table
-- Stores S3 Intelligent-Tiering configurations per bucket
-- Each bucket can have up to 1000 configurations (per AWS limit)

CREATE TABLE IF NOT EXISTS bucket_intelligent_tiering (
    id VARCHAR(36) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    config_id VARCHAR(64) NOT NULL, -- User-provided configuration ID
    config_json JSONB NOT NULL, -- IntelligentTieringConfiguration as JSON
    created_at BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (id),
    CONSTRAINT idx_bucket_config UNIQUE (bucket, config_id)
);

CREATE INDEX IF NOT EXISTS idx_intelligent_tiering_bucket ON bucket_intelligent_tiering(bucket);
