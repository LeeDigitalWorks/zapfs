-- Bucket CORS configurations table
CREATE TABLE IF NOT EXISTS bucket_cors (
    bucket VARCHAR(255) NOT NULL,
    cors_json JSON NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

-- Bucket website configurations table
CREATE TABLE IF NOT EXISTS bucket_website (
    bucket VARCHAR(255) NOT NULL,
    website_json JSON NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (bucket),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);

