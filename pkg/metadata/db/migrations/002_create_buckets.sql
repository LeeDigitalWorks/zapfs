-- Migration: Create buckets table
CREATE TABLE IF NOT EXISTS buckets (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    owner_id VARCHAR(255) NOT NULL,
    region VARCHAR(64),
    created_at BIGINT NOT NULL,
    default_profile_id VARCHAR(36),
    versioning VARCHAR(16) DEFAULT '',
    
    INDEX idx_buckets_owner (owner_id),
    INDEX idx_buckets_name (name)
);
