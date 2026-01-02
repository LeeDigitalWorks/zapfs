-- Migration: Create notification_configs table for S3 event notifications
-- Stores bucket notification configuration (TopicConfigurations, QueueConfigurations)

CREATE TABLE IF NOT EXISTS notification_configs (
    bucket VARCHAR(255) NOT NULL PRIMARY KEY,
    config JSON NOT NULL COMMENT 'S3 NotificationConfiguration XML serialized as JSON',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT fk_notification_bucket FOREIGN KEY (bucket)
        REFERENCES buckets(name) ON DELETE CASCADE
);
