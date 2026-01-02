-- Lifecycle scanner state table
-- Track scan progress for resumption after restarts

CREATE TABLE IF NOT EXISTS lifecycle_scan_state (
    bucket VARCHAR(255) NOT NULL,
    -- Pagination cursor for resumption
    last_key VARCHAR(1024) NOT NULL DEFAULT '',
    last_version_id VARCHAR(64) NOT NULL DEFAULT '',
    -- Timing (Unix nanoseconds for consistency with other tables)
    scan_started_at BIGINT NOT NULL DEFAULT 0,
    scan_completed_at BIGINT NOT NULL DEFAULT 0,
    -- Statistics
    objects_scanned INT NOT NULL DEFAULT 0,
    actions_enqueued INT NOT NULL DEFAULT 0,
    -- Error tracking
    last_error TEXT,
    consecutive_errors INT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Index for finding stale scans (buckets that need scanning)
CREATE INDEX idx_lifecycle_scan_completed ON lifecycle_scan_state(scan_completed_at);
