-- Migration: Create objects table
CREATE TABLE IF NOT EXISTS objects (
    id VARCHAR(36) PRIMARY KEY,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    size BIGINT UNSIGNED NOT NULL,
    version BIGINT UNSIGNED DEFAULT 1,
    etag VARCHAR(64) NOT NULL,
    created_at BIGINT NOT NULL,
    deleted_at BIGINT DEFAULT 0,
    ttl INT UNSIGNED DEFAULT 0,
    profile_id VARCHAR(36),
    chunk_refs JSON,
    ec_group_ids JSON,

    -- Encryption metadata
    sse_algorithm VARCHAR(20) DEFAULT '' COMMENT 'Encryption algorithm: AES256 (SSE-S3/SSE-C), aws:kms (SSE-KMS), or empty',
    sse_customer_key_md5 VARCHAR(32) DEFAULT '' COMMENT 'MD5 hash of customer-provided key for SSE-C validation',
    sse_kms_key_id VARCHAR(255) DEFAULT '' COMMENT 'KMS key ID for SSE-KMS encryption',
    sse_kms_context TEXT COMMENT 'KMS encryption context for SSE-KMS',

    -- UNIQUE constraint on (bucket, object_key) for proper UPSERT behavior
    -- Using prefix length (255) due to MySQL utf8mb4 key length limit
    UNIQUE INDEX idx_objects_bucket_key (bucket, object_key(255)),
    INDEX idx_objects_deleted (deleted_at),
    INDEX idx_objects_bucket (bucket),

    CONSTRAINT fk_objects_bucket FOREIGN KEY (bucket)
        REFERENCES buckets(name) ON DELETE CASCADE
);
