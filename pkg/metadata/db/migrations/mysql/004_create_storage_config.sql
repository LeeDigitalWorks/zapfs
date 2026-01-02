-- Migration: Create storage pools table
CREATE TABLE IF NOT EXISTS storage_pools (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    backend_type VARCHAR(32) NOT NULL,
    disk_type VARCHAR(32),
    weight DOUBLE NOT NULL DEFAULT 1.0,
    endpoint VARCHAR(512),
    region VARCHAR(64),
    storage_class VARCHAR(64),
    credentials_ref VARCHAR(255),
    read_only BOOLEAN DEFAULT FALSE,
    protected BOOLEAN DEFAULT FALSE,
    deletion_protection_until TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    modified_by VARCHAR(255),
    
    INDEX idx_pools_name (name),
    INDEX idx_pools_type (backend_type)
);

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
    spread_across_dcs BOOLEAN DEFAULT FALSE,
    
    INDEX idx_profiles_name (name)
);

-- Migration: Create profile-pool associations
CREATE TABLE IF NOT EXISTS profile_pools (
    profile_id VARCHAR(36) NOT NULL,
    pool_id VARCHAR(36) NOT NULL,
    weight_override DOUBLE DEFAULT 0,
    
    PRIMARY KEY (profile_id, pool_id),
    CONSTRAINT fk_pp_profile FOREIGN KEY (profile_id) 
        REFERENCES storage_profiles(id) ON DELETE CASCADE,
    CONSTRAINT fk_pp_pool FOREIGN KEY (pool_id) 
        REFERENCES storage_pools(id) ON DELETE CASCADE
);
