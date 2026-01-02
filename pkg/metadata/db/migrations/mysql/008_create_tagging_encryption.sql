-- Bucket tagging table
CREATE TABLE IF NOT EXISTS bucket_tagging (
    bucket VARCHAR(255) NOT NULL,
    tags_json JSON NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

-- Object tagging table
-- Note: Using surrogate key + unique index due to MySQL utf8mb4 key length limit
CREATE TABLE IF NOT EXISTS object_tagging (
    id VARCHAR(36) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    tags_json JSON NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE INDEX idx_object_tagging_bucket_key (bucket, object_key(255))
);

-- Bucket encryption configuration table
CREATE TABLE IF NOT EXISTS bucket_encryption (
    bucket VARCHAR(255) NOT NULL,
    encryption_json JSON NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

