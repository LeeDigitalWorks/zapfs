-- Migration: Create objects table
-- Supports object versioning: each version is a separate row with unique ID
-- is_latest=true indicates the current version for GetObject without version ID
CREATE TABLE IF NOT EXISTS objects (
    id VARCHAR(36) PRIMARY KEY,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    size BIGINT NOT NULL,
    version BIGINT DEFAULT 1,
    etag VARCHAR(64) NOT NULL,
    content_type VARCHAR(255) NOT NULL DEFAULT 'application/octet-stream',
    created_at BIGINT NOT NULL,
    deleted_at BIGINT DEFAULT 0,
    ttl INT DEFAULT 0,
    profile_id VARCHAR(36),
    storage_class VARCHAR(32) DEFAULT 'STANDARD',
    transitioned_at BIGINT NOT NULL DEFAULT 0,
    transitioned_ref VARCHAR(512) NOT NULL DEFAULT '',
    chunk_refs JSONB,
    ec_group_ids JSONB,
    is_latest BOOLEAN NOT NULL DEFAULT TRUE,

    -- Encryption metadata
    sse_algorithm VARCHAR(20) DEFAULT '',
    sse_customer_key_md5 VARCHAR(32) DEFAULT '',
    sse_kms_key_id VARCHAR(255) DEFAULT '',
    sse_kms_context TEXT,

    -- Restore tracking for archive tiers (GLACIER, DEEP_ARCHIVE)
    restore_status VARCHAR(20) NOT NULL DEFAULT '',
    restore_expiry_date BIGINT NOT NULL DEFAULT 0,
    restore_tier VARCHAR(20) NOT NULL DEFAULT '',
    restore_requested_at BIGINT NOT NULL DEFAULT 0,

    -- Intelligent tiering access tracking
    last_accessed_at BIGINT NOT NULL DEFAULT 0,

    CONSTRAINT fk_objects_bucket FOREIGN KEY (bucket)
        REFERENCES buckets(name) ON DELETE CASCADE
);

-- Note: PostgreSQL doesn't support prefix indexes, using full column
CREATE INDEX IF NOT EXISTS idx_objects_bucket_key_latest ON objects(bucket, object_key, is_latest);
CREATE INDEX IF NOT EXISTS idx_objects_bucket_key_id ON objects(bucket, object_key, id);
CREATE INDEX IF NOT EXISTS idx_objects_deleted ON objects(deleted_at);
CREATE INDEX IF NOT EXISTS idx_objects_bucket ON objects(bucket);
CREATE INDEX IF NOT EXISTS idx_objects_storage_class ON objects(storage_class);
CREATE INDEX IF NOT EXISTS idx_objects_restore_expiry ON objects(restore_status, restore_expiry_date);
CREATE INDEX IF NOT EXISTS idx_objects_access_pattern ON objects(storage_class, last_accessed_at);

COMMENT ON COLUMN objects.content_type IS 'MIME type of the object';
COMMENT ON COLUMN objects.transitioned_at IS 'Unix nano when object was transitioned, 0 if not transitioned';
COMMENT ON COLUMN objects.transitioned_ref IS 'Remote object key in tier backend (e.g., ab/cd/uuid for S3)';
COMMENT ON COLUMN objects.restore_status IS 'Restore status: empty, pending, in_progress, completed';
COMMENT ON COLUMN objects.restore_expiry_date IS 'Unix nano when restored copy expires';
COMMENT ON COLUMN objects.restore_tier IS 'Retrieval tier: Expedited, Standard, Bulk';
COMMENT ON COLUMN objects.restore_requested_at IS 'Unix nano when restore was requested';
COMMENT ON COLUMN objects.last_accessed_at IS 'Unix nano of last GET request, 0 if never accessed (use created_at)';
