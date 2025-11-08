-- Task queue table for background processing
-- Supports both community and enterprise task types
CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR(36) PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 5,
    payload JSON,

    -- Scheduling
    scheduled_at DATETIME(6) NOT NULL,
    started_at DATETIME(6),
    completed_at DATETIME(6),

    -- Retry handling
    attempts INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    retry_after DATETIME(6),

    -- Worker tracking (visibility timeout)
    worker_id VARCHAR(100),
    heartbeat_at DATETIME(6),

    -- Error tracking
    last_error TEXT,

    -- Metadata
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    region VARCHAR(50),

    -- Indexes for efficient polling
    INDEX idx_status_scheduled (status, scheduled_at),
    INDEX idx_type_status (type, status),
    INDEX idx_retry_after (retry_after),
    INDEX idx_running_heartbeat (status, heartbeat_at),
    INDEX idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
