-- Copyright 2025 ZapFS Authors
-- SPDX-License-Identifier: Apache-2.0

-- chunk_registry: Central RefCount tracking for chunks
-- Eliminates distributed gc_decrement tasks by centralizing reference counting
CREATE TABLE IF NOT EXISTS chunk_registry (
    chunk_id VARCHAR(64) PRIMARY KEY,
    size BIGINT NOT NULL,
    ref_count INT NOT NULL DEFAULT 0,
    zero_ref_since BIGINT DEFAULT 0,
    created_at BIGINT NOT NULL,

    -- Index for GC queries: find chunks with ref_count=0 past grace period
    INDEX idx_gc (ref_count, zero_ref_since)
);

-- chunk_replicas: Track which file servers have which chunks
-- Enables efficient rebalancing and GC without querying all objects
CREATE TABLE IF NOT EXISTS chunk_replicas (
    chunk_id VARCHAR(64) NOT NULL,
    server_id VARCHAR(255) NOT NULL,
    backend_id VARCHAR(64) NOT NULL,
    verified_at BIGINT NOT NULL,

    PRIMARY KEY (chunk_id, server_id),

    -- Index for querying all chunks on a specific server (for rebalancing/decommission)
    INDEX idx_server (server_id),

    -- CASCADE: when chunk_registry entry deleted, replicas auto-cleanup
    CONSTRAINT fk_chunk_registry FOREIGN KEY (chunk_id)
        REFERENCES chunk_registry(chunk_id) ON DELETE CASCADE
);
