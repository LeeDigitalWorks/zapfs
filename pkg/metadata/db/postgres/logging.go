// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/google/uuid"
)

// ============================================================================
// Bucket Logging Operations
// ============================================================================

// GetBucketLogging retrieves the logging configuration for a bucket.
func (p *Postgres) GetBucketLogging(ctx context.Context, bucket string) (*db.BucketLoggingConfig, error) {
	row := p.Store.DB().QueryRowContext(ctx, `
		SELECT id, source_bucket, target_bucket, target_prefix, created_at, updated_at
		FROM bucket_logging
		WHERE source_bucket = $1
	`, bucket)

	var config db.BucketLoggingConfig
	var createdAt, updatedAt int64

	err := row.Scan(
		&config.ID,
		&config.SourceBucket,
		&config.TargetBucket,
		&config.TargetPrefix,
		&createdAt,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	config.CreatedAt = time.Unix(0, createdAt)
	config.UpdatedAt = time.Unix(0, updatedAt)

	return &config, nil
}

// SetBucketLogging stores the logging configuration for a bucket.
func (p *Postgres) SetBucketLogging(ctx context.Context, config *db.BucketLoggingConfig) error {
	now := time.Now().UnixNano()

	// If target bucket is empty, delete the configuration
	if config.TargetBucket == "" {
		return p.DeleteBucketLogging(ctx, config.SourceBucket)
	}

	// Generate ID if not set
	if config.ID == "" {
		config.ID = uuid.New().String()
	}

	// Upsert the configuration
	_, err := p.Store.DB().ExecContext(ctx, `
		INSERT INTO bucket_logging (id, source_bucket, target_bucket, target_prefix, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (source_bucket) DO UPDATE SET
			target_bucket = EXCLUDED.target_bucket,
			target_prefix = EXCLUDED.target_prefix,
			updated_at = EXCLUDED.updated_at
	`, config.ID, config.SourceBucket, config.TargetBucket, config.TargetPrefix, now, now)

	return err
}

// DeleteBucketLogging removes the logging configuration for a bucket.
func (p *Postgres) DeleteBucketLogging(ctx context.Context, bucket string) error {
	_, err := p.Store.DB().ExecContext(ctx, `
		DELETE FROM bucket_logging WHERE source_bucket = $1
	`, bucket)
	return err
}

// ListLoggingConfigs returns all bucket logging configurations.
func (p *Postgres) ListLoggingConfigs(ctx context.Context) ([]*db.BucketLoggingConfig, error) {
	rows, err := p.Store.DB().QueryContext(ctx, `
		SELECT id, source_bucket, target_bucket, target_prefix, created_at, updated_at
		FROM bucket_logging
		ORDER BY source_bucket
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []*db.BucketLoggingConfig

	for rows.Next() {
		var config db.BucketLoggingConfig
		var createdAt, updatedAt int64

		if err := rows.Scan(
			&config.ID,
			&config.SourceBucket,
			&config.TargetBucket,
			&config.TargetPrefix,
			&createdAt,
			&updatedAt,
		); err != nil {
			return nil, err
		}

		config.CreatedAt = time.Unix(0, createdAt)
		config.UpdatedAt = time.Unix(0, updatedAt)

		configs = append(configs, &config)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return configs, nil
}
