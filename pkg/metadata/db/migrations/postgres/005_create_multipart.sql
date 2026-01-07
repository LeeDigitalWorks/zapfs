-- Multipart uploads table
CREATE TABLE IF NOT EXISTS multipart_uploads (
    id VARCHAR(36) NOT NULL,
    upload_id VARCHAR(64) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    owner_id VARCHAR(255) NOT NULL,
    initiated BIGINT NOT NULL,
    content_type VARCHAR(255),
    storage_class VARCHAR(32) DEFAULT 'STANDARD',
    metadata JSONB,
    sse_algorithm VARCHAR(20) DEFAULT '',
    sse_kms_key_id VARCHAR(255) DEFAULT '',
    sse_kms_context TEXT,
    sse_dek_ciphertext TEXT,
    -- ACL for the final object (JSON-encoded)
    acl_json TEXT,
    PRIMARY KEY (upload_id)
);

CREATE INDEX IF NOT EXISTS idx_multipart_bucket_key ON multipart_uploads(bucket, object_key);
CREATE INDEX IF NOT EXISTS idx_multipart_bucket_initiated ON multipart_uploads(bucket, initiated);

-- Multipart parts table
CREATE TABLE IF NOT EXISTS multipart_parts (
    id VARCHAR(36) NOT NULL,
    upload_id VARCHAR(64) NOT NULL,
    part_number INT NOT NULL,
    size BIGINT NOT NULL,
    etag VARCHAR(64) NOT NULL,
    last_modified BIGINT NOT NULL,
    chunk_refs JSONB,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
);
