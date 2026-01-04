-- Migration: Create federation_configs table for S3 bucket federation
-- Stores external S3 connection details and migration tracking for federated buckets

CREATE TABLE IF NOT EXISTS federation_configs (
    bucket VARCHAR(255) NOT NULL PRIMARY KEY,

    -- External S3 connection
    endpoint VARCHAR(512) NOT NULL,
    region VARCHAR(64) NOT NULL,
    access_key_id VARCHAR(128) NOT NULL,
    secret_access_key_encrypted VARBINARY(512) NOT NULL,
    external_bucket VARCHAR(255) NOT NULL,
    path_style BOOLEAN NOT NULL DEFAULT false,

    -- Migration tracking
    migration_started_at BIGINT DEFAULT 0,
    migration_paused BOOLEAN NOT NULL DEFAULT false,
    objects_discovered BIGINT DEFAULT 0,
    objects_synced BIGINT DEFAULT 0,
    bytes_synced BIGINT DEFAULT 0,
    last_sync_key VARCHAR(1024) DEFAULT '',

    -- Dual-write control (OFF by default)
    dual_write_enabled BOOLEAN NOT NULL DEFAULT false,

    -- Timestamps
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,

    CONSTRAINT fk_federation_bucket FOREIGN KEY (bucket)
        REFERENCES buckets(name) ON DELETE CASCADE
);

CREATE INDEX idx_federation_migration ON federation_configs(migration_started_at);
CREATE INDEX idx_federation_paused ON federation_configs(migration_paused);
