-- Multipart uploads table
-- Note: object_key uses prefix index (255) due to MySQL utf8mb4 index key length limit (3072 bytes)
CREATE TABLE IF NOT EXISTS multipart_uploads (
    id VARCHAR(36) NOT NULL,
    upload_id VARCHAR(64) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    owner_id VARCHAR(255) NOT NULL,
    initiated BIGINT NOT NULL,
    content_type VARCHAR(255),
    storage_class VARCHAR(32) DEFAULT 'STANDARD',
    metadata JSON,
    -- Server-side encryption fields
    sse_algorithm VARCHAR(32),
    sse_kms_key_id VARCHAR(255),
    sse_kms_context TEXT,
    sse_dek_ciphertext BLOB,
    -- ACL for the final object (JSON-encoded)
    acl_json TEXT,
    PRIMARY KEY (upload_id),
    INDEX idx_bucket_key (bucket, object_key(255)),
    INDEX idx_bucket_initiated (bucket, initiated)
);

-- Multipart parts table
CREATE TABLE IF NOT EXISTS multipart_parts (
    id VARCHAR(36) NOT NULL,
    upload_id VARCHAR(64) NOT NULL,
    part_number INT NOT NULL,
    size BIGINT NOT NULL,
    etag VARCHAR(64) NOT NULL,
    last_modified BIGINT NOT NULL,
    chunk_refs JSON,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
);

