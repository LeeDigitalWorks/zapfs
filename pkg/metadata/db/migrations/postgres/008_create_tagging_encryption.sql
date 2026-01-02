-- Bucket tagging table
CREATE TABLE IF NOT EXISTS bucket_tagging (
    bucket VARCHAR(255) NOT NULL,
    tags_json JSONB NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

-- Object tagging table
CREATE TABLE IF NOT EXISTS object_tagging (
    id VARCHAR(36) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    tags_json JSONB NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_object_tagging_bucket_key ON object_tagging(bucket, object_key);

-- Bucket encryption configuration table
CREATE TABLE IF NOT EXISTS bucket_encryption (
    bucket VARCHAR(255) NOT NULL,
    encryption_json JSONB NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);
