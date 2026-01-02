-- Usage tracking tables for billing and analytics
-- These tables exist in all editions (community and enterprise)
-- Data is only written when enterprise usage reporting is enabled

-- Raw usage events (append-only, short retention)
-- Using declarative partitioning for PostgreSQL
CREATE TABLE IF NOT EXISTS usage_events (
    id BIGSERIAL,
    event_time TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

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

    PRIMARY KEY (id, event_time)
) PARTITION BY RANGE (event_time);

-- Create partitions (PostgreSQL syntax)
CREATE TABLE IF NOT EXISTS usage_events_2025_12 PARTITION OF usage_events
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS usage_events_2026_01 PARTITION OF usage_events
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE IF NOT EXISTS usage_events_2026_02 PARTITION OF usage_events
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS usage_events_2026_03 PARTITION OF usage_events
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS usage_events_2026_04 PARTITION OF usage_events
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS usage_events_2026_05 PARTITION OF usage_events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS usage_events_2026_06 PARTITION OF usage_events
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS usage_events_default PARTITION OF usage_events DEFAULT;

-- Covering index for AggregateEvents() GROUP BY queries
CREATE INDEX IF NOT EXISTS idx_usage_agg ON usage_events(owner_id, bucket, event_type, event_time, storage_class);
CREATE INDEX IF NOT EXISTS idx_usage_owner_time ON usage_events(owner_id, event_time);
CREATE INDEX IF NOT EXISTS idx_usage_bucket_time ON usage_events(bucket, event_time);
CREATE INDEX IF NOT EXISTS idx_usage_event_time ON usage_events(event_time);

-- Daily usage aggregates (permanent retention)
CREATE TABLE IF NOT EXISTS usage_daily (
    id BIGSERIAL PRIMARY KEY,

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
    created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_daily_unique ON usage_daily(owner_id, bucket, usage_date);
CREATE INDEX IF NOT EXISTS idx_usage_daily_owner_date ON usage_daily(owner_id, usage_date);
CREATE INDEX IF NOT EXISTS idx_usage_daily_date ON usage_daily(usage_date);
CREATE INDEX IF NOT EXISTS idx_usage_daily_owner_bucket_date ON usage_daily(owner_id, bucket, usage_date);

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
    progress_pct SMALLINT NOT NULL DEFAULT 0,
    error_message TEXT,

    -- Result (JSON blob when completed)
    result_json TEXT,

    -- Timestamps
    created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ(6),
    completed_at TIMESTAMPTZ(6),

    -- Cleanup
    expires_at TIMESTAMPTZ(6) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_report_jobs_owner ON usage_report_jobs(owner_id);
CREATE INDEX IF NOT EXISTS idx_report_jobs_status ON usage_report_jobs(status);
CREATE INDEX IF NOT EXISTS idx_report_jobs_expires ON usage_report_jobs(expires_at);
CREATE INDEX IF NOT EXISTS idx_report_jobs_status_created ON usage_report_jobs(status, created_at);
