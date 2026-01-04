-- Migration: Create bucket_replication table for cross-region replication configurations
-- Stores S3 ReplicationConfiguration for each bucket (Enterprise feature)

CREATE TABLE IF NOT EXISTS bucket_replication (
    bucket VARCHAR(255) NOT NULL PRIMARY KEY,
    config JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_replication_bucket FOREIGN KEY (bucket)
        REFERENCES buckets(name) ON DELETE CASCADE
);
