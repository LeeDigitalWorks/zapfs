// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// ============================================================================
// Federation Operations
// ============================================================================

// GetFederationConfig retrieves the federation config for a bucket.
func (p *Postgres) GetFederationConfig(ctx context.Context, bucket string) (*s3types.FederationConfig, error) {
	var config s3types.FederationConfig
	var secretKeyEncrypted []byte

	err := p.Store.DB().QueryRowContext(ctx, `
		SELECT bucket, endpoint, region, access_key_id, secret_access_key_encrypted,
		       external_bucket, path_style, migration_started_at, migration_paused,
		       objects_discovered, objects_synced, bytes_synced, last_sync_key,
		       dual_write_enabled, created_at, updated_at
		FROM federation_configs WHERE bucket = $1
	`, bucket).Scan(
		&config.Bucket, &config.Endpoint, &config.Region,
		&config.AccessKeyID, &secretKeyEncrypted,
		&config.ExternalBucket, &config.PathStyle,
		&config.MigrationStartedAt, &config.MigrationPaused,
		&config.ObjectsDiscovered, &config.ObjectsSynced, &config.BytesSynced,
		&config.LastSyncKey, &config.DualWriteEnabled,
		&config.CreatedAt, &config.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, db.ErrFederationNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get federation config: %w", err)
	}

	// Note: SecretAccessKey is stored encrypted - decryption happens at service layer
	config.SecretAccessKey = string(secretKeyEncrypted)

	return &config, nil
}

// SetFederationConfig stores or updates the federation config for a bucket.
func (p *Postgres) SetFederationConfig(ctx context.Context, config *s3types.FederationConfig) error {
	now := time.Now().UnixNano()

	// Note: SecretAccessKey should be encrypted before calling this - service layer responsibility
	_, err := p.Store.DB().ExecContext(ctx, `
		INSERT INTO federation_configs (
			bucket, endpoint, region, access_key_id, secret_access_key_encrypted,
			external_bucket, path_style, migration_started_at, migration_paused,
			objects_discovered, objects_synced, bytes_synced, last_sync_key,
			dual_write_enabled, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (bucket) DO UPDATE SET
			endpoint = EXCLUDED.endpoint,
			region = EXCLUDED.region,
			access_key_id = EXCLUDED.access_key_id,
			secret_access_key_encrypted = EXCLUDED.secret_access_key_encrypted,
			external_bucket = EXCLUDED.external_bucket,
			path_style = EXCLUDED.path_style,
			migration_started_at = EXCLUDED.migration_started_at,
			migration_paused = EXCLUDED.migration_paused,
			objects_discovered = EXCLUDED.objects_discovered,
			objects_synced = EXCLUDED.objects_synced,
			bytes_synced = EXCLUDED.bytes_synced,
			last_sync_key = EXCLUDED.last_sync_key,
			dual_write_enabled = EXCLUDED.dual_write_enabled,
			updated_at = EXCLUDED.updated_at
	`,
		config.Bucket, config.Endpoint, config.Region,
		config.AccessKeyID, []byte(config.SecretAccessKey),
		config.ExternalBucket, config.PathStyle,
		config.MigrationStartedAt, config.MigrationPaused,
		config.ObjectsDiscovered, config.ObjectsSynced, config.BytesSynced,
		config.LastSyncKey, config.DualWriteEnabled,
		now, now,
	)
	if err != nil {
		return fmt.Errorf("set federation config: %w", err)
	}
	return nil
}

// DeleteFederationConfig removes the federation config for a bucket.
func (p *Postgres) DeleteFederationConfig(ctx context.Context, bucket string) error {
	_, err := p.Store.DB().ExecContext(ctx, `
		DELETE FROM federation_configs WHERE bucket = $1
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete federation config: %w", err)
	}
	return nil
}

// ListFederatedBuckets returns all buckets with federation configurations.
func (p *Postgres) ListFederatedBuckets(ctx context.Context) ([]*s3types.FederationConfig, error) {
	rows, err := p.Store.DB().QueryContext(ctx, `
		SELECT bucket, endpoint, region, access_key_id, secret_access_key_encrypted,
		       external_bucket, path_style, migration_started_at, migration_paused,
		       objects_discovered, objects_synced, bytes_synced, last_sync_key,
		       dual_write_enabled, created_at, updated_at
		FROM federation_configs
		ORDER BY bucket
	`)
	if err != nil {
		return nil, fmt.Errorf("list federated buckets: %w", err)
	}
	defer rows.Close()

	var configs []*s3types.FederationConfig
	for rows.Next() {
		var config s3types.FederationConfig
		var secretKeyEncrypted []byte

		err := rows.Scan(
			&config.Bucket, &config.Endpoint, &config.Region,
			&config.AccessKeyID, &secretKeyEncrypted,
			&config.ExternalBucket, &config.PathStyle,
			&config.MigrationStartedAt, &config.MigrationPaused,
			&config.ObjectsDiscovered, &config.ObjectsSynced, &config.BytesSynced,
			&config.LastSyncKey, &config.DualWriteEnabled,
			&config.CreatedAt, &config.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan federation config: %w", err)
		}

		// Note: SecretAccessKey is stored encrypted - decryption happens at service layer
		config.SecretAccessKey = string(secretKeyEncrypted)
		configs = append(configs, &config)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate federation configs: %w", err)
	}

	return configs, nil
}

// UpdateMigrationProgress updates the migration progress counters for a bucket.
func (p *Postgres) UpdateMigrationProgress(ctx context.Context, bucket string, objectsSynced, bytesSynced int64, lastSyncKey string) error {
	now := time.Now().UnixNano()

	result, err := p.Store.DB().ExecContext(ctx, `
		UPDATE federation_configs
		SET objects_synced = $1, bytes_synced = $2, last_sync_key = $3, updated_at = $4
		WHERE bucket = $5
	`, objectsSynced, bytesSynced, lastSyncKey, now, bucket)
	if err != nil {
		return fmt.Errorf("update migration progress: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if affected == 0 {
		return db.ErrFederationNotFound
	}

	return nil
}

// SetMigrationPaused sets the migration_paused flag for a bucket.
func (p *Postgres) SetMigrationPaused(ctx context.Context, bucket string, paused bool) error {
	now := time.Now().UnixNano()

	result, err := p.Store.DB().ExecContext(ctx, `
		UPDATE federation_configs SET migration_paused = $1, updated_at = $2 WHERE bucket = $3
	`, paused, now, bucket)
	if err != nil {
		return fmt.Errorf("set migration paused: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if affected == 0 {
		return db.ErrFederationNotFound
	}

	return nil
}

// SetDualWriteEnabled sets the dual_write_enabled flag for a bucket.
func (p *Postgres) SetDualWriteEnabled(ctx context.Context, bucket string, enabled bool) error {
	now := time.Now().UnixNano()

	result, err := p.Store.DB().ExecContext(ctx, `
		UPDATE federation_configs SET dual_write_enabled = $1, updated_at = $2 WHERE bucket = $3
	`, enabled, now, bucket)
	if err != nil {
		return fmt.Errorf("set dual write enabled: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if affected == 0 {
		return db.ErrFederationNotFound
	}

	return nil
}

// GetFederatedBucketsNeedingSync returns buckets that are in migrating mode and not paused.
// Note: The bucket mode is stored in the Manager's Raft state, not in the metadata DB.
// This method returns all non-paused federation configs; the caller should check bucket mode.
func (p *Postgres) GetFederatedBucketsNeedingSync(ctx context.Context, limit int) ([]*s3types.FederationConfig, error) {
	rows, err := p.Store.DB().QueryContext(ctx, `
		SELECT bucket, endpoint, region, access_key_id, secret_access_key_encrypted,
		       external_bucket, path_style, migration_started_at, migration_paused,
		       objects_discovered, objects_synced, bytes_synced, last_sync_key,
		       dual_write_enabled, created_at, updated_at
		FROM federation_configs
		WHERE migration_paused = false
		  AND migration_started_at > 0
		ORDER BY updated_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get federated buckets needing sync: %w", err)
	}
	defer rows.Close()

	var configs []*s3types.FederationConfig
	for rows.Next() {
		var config s3types.FederationConfig
		var secretKeyEncrypted []byte

		err := rows.Scan(
			&config.Bucket, &config.Endpoint, &config.Region,
			&config.AccessKeyID, &secretKeyEncrypted,
			&config.ExternalBucket, &config.PathStyle,
			&config.MigrationStartedAt, &config.MigrationPaused,
			&config.ObjectsDiscovered, &config.ObjectsSynced, &config.BytesSynced,
			&config.LastSyncKey, &config.DualWriteEnabled,
			&config.CreatedAt, &config.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan federation config: %w", err)
		}

		config.SecretAccessKey = string(secretKeyEncrypted)
		configs = append(configs, &config)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate federation configs: %w", err)
	}

	return configs, nil
}

// Ensure Postgres implements db.FederationStore
var _ db.FederationStore = (*Postgres)(nil)
