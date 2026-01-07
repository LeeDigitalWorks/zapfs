-- Migration: Create objects table
-- Supports object versioning: each version is a separate row with unique ID
-- is_latest=1 indicates the current version for GetObject without version ID
CREATE TABLE IF NOT EXISTS objects (
    id VARCHAR(36) PRIMARY KEY,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    size BIGINT UNSIGNED NOT NULL,
    version BIGINT UNSIGNED DEFAULT 1,
    etag VARCHAR(64) NOT NULL,
    content_type VARCHAR(255) NOT NULL DEFAULT 'application/octet-stream' COMMENT 'MIME type of the object',
    created_at BIGINT NOT NULL,
    deleted_at BIGINT DEFAULT 0,
    ttl INT UNSIGNED DEFAULT 0,
    profile_id VARCHAR(36),
    storage_class VARCHAR(32) DEFAULT 'STANDARD' COMMENT 'S3 storage class: STANDARD, GLACIER, DEEP_ARCHIVE, etc.',
    transitioned_at BIGINT NOT NULL DEFAULT 0 COMMENT 'Unix nano when object was transitioned, 0 if not transitioned',
    transitioned_ref VARCHAR(512) NOT NULL DEFAULT '' COMMENT 'Remote object key in tier backend (e.g., ab/cd/uuid for S3)',
    chunk_refs JSON,
    ec_group_ids JSON,
    is_latest TINYINT(1) NOT NULL DEFAULT 1 COMMENT 'True for the current version of the object',

    -- Encryption metadata
    sse_algorithm VARCHAR(20) DEFAULT '' COMMENT 'Encryption algorithm: AES256 (SSE-S3/SSE-C), aws:kms (SSE-KMS), or empty',
    sse_customer_key_md5 VARCHAR(32) DEFAULT '' COMMENT 'MD5 hash of customer-provided key for SSE-C validation',
    sse_kms_key_id VARCHAR(255) DEFAULT '' COMMENT 'KMS key ID for SSE-KMS encryption',
    sse_kms_context TEXT COMMENT 'KMS encryption context for SSE-KMS',

    -- Restore tracking for archive tiers (GLACIER, DEEP_ARCHIVE)
    restore_status VARCHAR(20) NOT NULL DEFAULT '' COMMENT 'Restore status: empty, pending, in_progress, completed',
    restore_expiry_date BIGINT NOT NULL DEFAULT 0 COMMENT 'Unix nano when restored copy expires',
    restore_tier VARCHAR(20) NOT NULL DEFAULT '' COMMENT 'Retrieval tier: Expedited, Standard, Bulk',
    restore_requested_at BIGINT NOT NULL DEFAULT 0 COMMENT 'Unix nano when restore was requested',

    -- Intelligent tiering access tracking
    last_accessed_at BIGINT NOT NULL DEFAULT 0 COMMENT 'Unix nano of last GET request, 0 if never accessed (use created_at)',

    -- User-defined metadata (x-amz-meta-* headers)
    metadata JSON DEFAULT NULL COMMENT 'User-defined metadata (x-amz-meta-* headers)',

    -- No unique constraint on (bucket, object_key) to allow multiple versions
    -- Use is_latest to identify current version
    INDEX idx_objects_bucket_key_latest (bucket, object_key(255), is_latest),
    INDEX idx_objects_bucket_key_id (bucket, object_key(255), id),
    INDEX idx_objects_deleted (deleted_at),
    INDEX idx_objects_bucket (bucket),
    INDEX idx_objects_storage_class (storage_class),
    INDEX idx_objects_restore_expiry (restore_status, restore_expiry_date),
    INDEX idx_objects_access_pattern (storage_class, last_accessed_at),

    CONSTRAINT fk_objects_bucket FOREIGN KEY (bucket)
        REFERENCES buckets(name) ON DELETE CASCADE
);
