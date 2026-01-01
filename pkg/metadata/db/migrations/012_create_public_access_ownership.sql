-- Bucket Public Access Block configurations table
CREATE TABLE IF NOT EXISTS bucket_public_access_block (
    bucket VARCHAR(255) NOT NULL,
    block_public_acls BOOLEAN NOT NULL DEFAULT FALSE,
    ignore_public_acls BOOLEAN NOT NULL DEFAULT FALSE,
    block_public_policy BOOLEAN NOT NULL DEFAULT FALSE,
    restrict_public_buckets BOOLEAN NOT NULL DEFAULT FALSE,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

-- Bucket Ownership Controls table
CREATE TABLE IF NOT EXISTS bucket_ownership_controls (
    bucket VARCHAR(255) NOT NULL,
    object_ownership VARCHAR(50) NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);
