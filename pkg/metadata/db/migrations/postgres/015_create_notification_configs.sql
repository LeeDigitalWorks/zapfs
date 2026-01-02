-- Migration: Create notification_configs table for S3 event notifications
-- Stores bucket notification configuration (TopicConfigurations, QueueConfigurations)

CREATE TABLE IF NOT EXISTS notification_configs (
    bucket VARCHAR(255) NOT NULL PRIMARY KEY,
    config JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_notification_bucket FOREIGN KEY (bucket)
        REFERENCES buckets(name) ON DELETE CASCADE
);
