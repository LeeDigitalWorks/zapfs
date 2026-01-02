-- Migration: Create storage pools table
CREATE TABLE IF NOT EXISTS storage_pools (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    backend_type VARCHAR(32) NOT NULL,
    disk_type VARCHAR(32),
    weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    endpoint VARCHAR(512),
    region VARCHAR(64),
    storage_class VARCHAR(64),
    credentials_ref VARCHAR(255),
    read_only BOOLEAN DEFAULT FALSE,
    protected BOOLEAN DEFAULT FALSE,
    deletion_protection_until TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_pools_name ON storage_pools(name);
CREATE INDEX IF NOT EXISTS idx_pools_type ON storage_pools(backend_type);

-- Migration: Create storage profiles table
CREATE TABLE IF NOT EXISTS storage_profiles (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    replication INT DEFAULT 1,
    ec_data_shards INT DEFAULT 0,
    ec_parity_shards INT DEFAULT 0,
    compression VARCHAR(32) DEFAULT 'none',
    spread_across_racks BOOLEAN DEFAULT FALSE,
    spread_across_dcs BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_profiles_name ON storage_profiles(name);

-- Migration: Create profile-pool associations
CREATE TABLE IF NOT EXISTS profile_pools (
    profile_id VARCHAR(36) NOT NULL,
    pool_id VARCHAR(36) NOT NULL,
    weight_override DOUBLE PRECISION DEFAULT 0,

    PRIMARY KEY (profile_id, pool_id),
    CONSTRAINT fk_pp_profile FOREIGN KEY (profile_id)
        REFERENCES storage_profiles(id) ON DELETE CASCADE,
    CONSTRAINT fk_pp_pool FOREIGN KEY (pool_id)
        REFERENCES storage_pools(id) ON DELETE CASCADE
);
