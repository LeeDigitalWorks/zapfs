-- Migration: Create buckets table
CREATE TABLE IF NOT EXISTS buckets (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    owner_id VARCHAR(255) NOT NULL,
    region VARCHAR(64),
    created_at BIGINT NOT NULL,
    default_profile_id VARCHAR(36),
    versioning VARCHAR(16) DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_buckets_owner ON buckets(owner_id);
CREATE INDEX IF NOT EXISTS idx_buckets_name ON buckets(name);
