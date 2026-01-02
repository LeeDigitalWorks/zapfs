-- Bucket ACLs table
CREATE TABLE IF NOT EXISTS bucket_acls (
    bucket VARCHAR(255) NOT NULL,
    acl_json JSON NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

-- Object ACLs table
-- Note: Using surrogate key + unique index due to MySQL utf8mb4 key length limit
CREATE TABLE IF NOT EXISTS object_acls (
    id VARCHAR(36) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    acl_json JSON NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE INDEX idx_object_acls_bucket_key (bucket, object_key(255))
);

-- Bucket policies table
CREATE TABLE IF NOT EXISTS bucket_policies (
    bucket VARCHAR(255) NOT NULL,
    policy_json JSON NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

