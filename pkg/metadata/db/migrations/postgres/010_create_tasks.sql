-- Task queue table for background processing
-- Supports both community and enterprise task types
CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR(36) PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 5,
    payload JSONB,

    -- Scheduling
    scheduled_at TIMESTAMPTZ(6) NOT NULL,
    started_at TIMESTAMPTZ(6),
    completed_at TIMESTAMPTZ(6),

    -- Retry handling
    attempts INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    retry_after TIMESTAMPTZ(6),

    -- Worker tracking (visibility timeout)
    worker_id VARCHAR(100),
    heartbeat_at TIMESTAMPTZ(6),

    -- Error tracking
    last_error TEXT,

    -- Metadata
    created_at TIMESTAMPTZ(6) NOT NULL,
    updated_at TIMESTAMPTZ(6) NOT NULL,
    region VARCHAR(50)
);

-- Indexes for efficient polling
CREATE INDEX IF NOT EXISTS idx_tasks_status_scheduled ON tasks(status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_tasks_type_status ON tasks(type, status);
CREATE INDEX IF NOT EXISTS idx_tasks_retry_after ON tasks(retry_after);
CREATE INDEX IF NOT EXISTS idx_tasks_running_heartbeat ON tasks(status, heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_tasks_region ON tasks(region);
CREATE INDEX IF NOT EXISTS idx_tasks_created_desc ON tasks(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_cleanup ON tasks(status, completed_at);
