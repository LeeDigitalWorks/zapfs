-- Bucket lifecycle configuration table
CREATE TABLE IF NOT EXISTS bucket_lifecycle (
    bucket VARCHAR(255) NOT NULL,
    lifecycle_json JSON NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

-- Bucket Object Lock configuration table
CREATE TABLE IF NOT EXISTS bucket_object_lock (
    bucket VARCHAR(255) NOT NULL,
    config_json JSON NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

-- Object retention table
-- Note: Using surrogate key + unique index due to MySQL utf8mb4 key length limit
CREATE TABLE IF NOT EXISTS object_retention (
    id VARCHAR(36) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    mode VARCHAR(20) NOT NULL,
    retain_until_date VARCHAR(64) NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE INDEX idx_object_retention_bucket_key (bucket, object_key(255))
);

-- Object legal hold table
-- Note: Using surrogate key + unique index due to MySQL utf8mb4 key length limit
CREATE TABLE IF NOT EXISTS object_legal_hold (
    id VARCHAR(36) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    status VARCHAR(10) NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE INDEX idx_object_legal_hold_bucket_key (bucket, object_key(255))
);

