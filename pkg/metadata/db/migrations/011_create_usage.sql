-- Usage tracking tables for billing and analytics
-- These tables exist in all editions (community and enterprise)
-- Data is only written when enterprise usage reporting is enabled

-- Raw usage events (append-only, short retention, partitioned by month)
-- Partitioning enables O(1) partition drops instead of row-by-row deletion
CREATE TABLE IF NOT EXISTS usage_events (
    id BIGINT AUTO_INCREMENT,
    event_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    -- Ownership
    owner_id VARCHAR(64) NOT NULL,
    bucket VARCHAR(255) NOT NULL,

    -- Event type: storage_delta, object_delta, request, bandwidth
    event_type VARCHAR(20) NOT NULL,

    -- Metrics (interpretation depends on event_type)
    bytes_delta BIGINT DEFAULT 0,
    count_delta INT DEFAULT 0,
    operation VARCHAR(32),
    direction VARCHAR(10),       -- ingress, egress
    storage_class VARCHAR(32),

    -- PRIMARY KEY must include partition column for MySQL partitioning
    PRIMARY KEY (id, event_time),

    -- Covering index for AggregateEvents() GROUP BY queries
    INDEX idx_usage_agg (owner_id, bucket, event_type, event_time, storage_class),
    INDEX idx_owner_time (owner_id, event_time),
    INDEX idx_bucket_time (bucket, event_time),
    INDEX idx_event_time (event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (TO_DAYS(event_time)) (
    PARTITION p_2025_12 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p_2026_01 VALUES LESS THAN (TO_DAYS('2026-02-01')),
    PARTITION p_2026_02 VALUES LESS THAN (TO_DAYS('2026-03-01')),
    PARTITION p_2026_03 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p_2026_04 VALUES LESS THAN (TO_DAYS('2026-05-01')),
    PARTITION p_2026_05 VALUES LESS THAN (TO_DAYS('2026-06-01')),
    PARTITION p_2026_06 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Daily usage aggregates (permanent retention)
CREATE TABLE IF NOT EXISTS usage_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- Time period (UTC day)
    usage_date DATE NOT NULL,

    -- Ownership
    owner_id VARCHAR(64) NOT NULL,
    bucket VARCHAR(255) NOT NULL,

    -- Storage (point-in-time snapshot at end of day)
    storage_bytes BIGINT NOT NULL DEFAULT 0,
    storage_bytes_std BIGINT NOT NULL DEFAULT 0,      -- STANDARD class
    storage_bytes_ia BIGINT NOT NULL DEFAULT 0,       -- INFREQUENT_ACCESS
    storage_bytes_glacier BIGINT NOT NULL DEFAULT 0,  -- GLACIER/ARCHIVE
    object_count BIGINT NOT NULL DEFAULT 0,

    -- Requests (totals for the day)
    requests_get INT NOT NULL DEFAULT 0,
    requests_put INT NOT NULL DEFAULT 0,
    requests_delete INT NOT NULL DEFAULT 0,
    requests_list INT NOT NULL DEFAULT 0,
    requests_head INT NOT NULL DEFAULT 0,
    requests_copy INT NOT NULL DEFAULT 0,
    requests_other INT NOT NULL DEFAULT 0,

    -- Bandwidth (totals for the day)
    bandwidth_ingress_bytes BIGINT NOT NULL DEFAULT 0,
    bandwidth_egress_bytes BIGINT NOT NULL DEFAULT 0,

    -- Metadata
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

    -- Unique constraint for upserts
    UNIQUE INDEX idx_unique_day (owner_id, bucket, usage_date),
    INDEX idx_owner_date (owner_id, usage_date),
    INDEX idx_date (usage_date),

    -- Optimal index for GetDailyUsageByBucket() range queries
    INDEX idx_owner_bucket_date (owner_id, bucket, usage_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Async report generation jobs (24h expiry)
CREATE TABLE IF NOT EXISTS usage_report_jobs (
    id VARCHAR(36) PRIMARY KEY,

    -- Request parameters
    owner_id VARCHAR(64) NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    include_daily BOOLEAN NOT NULL DEFAULT FALSE,

    -- Job status: pending, processing, completed, failed
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    progress_pct TINYINT NOT NULL DEFAULT 0,
    error_message TEXT,

    -- Result (JSON blob when completed)
    result_json LONGTEXT,

    -- Timestamps
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    started_at DATETIME(6),
    completed_at DATETIME(6),

    -- Cleanup
    expires_at DATETIME(6) NOT NULL,

    INDEX idx_owner (owner_id),
    INDEX idx_status (status),
    INDEX idx_expires (expires_at),
    INDEX idx_status_created (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
