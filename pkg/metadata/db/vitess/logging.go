package vitess

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/google/uuid"
)

// GetBucketLogging retrieves the logging configuration for a bucket.
func (v *Vitess) GetBucketLogging(ctx context.Context, bucket string) (*db.BucketLoggingConfig, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, source_bucket, target_bucket, target_prefix, created_at, updated_at
		FROM bucket_logging
		WHERE source_bucket = ?
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
func (v *Vitess) SetBucketLogging(ctx context.Context, config *db.BucketLoggingConfig) error {
	now := time.Now().UnixNano()

	// If target bucket is empty, delete the configuration
	if config.TargetBucket == "" {
		return v.DeleteBucketLogging(ctx, config.SourceBucket)
	}

	// Generate ID if not set
	if config.ID == "" {
		config.ID = uuid.New().String()
	}

	// Upsert the configuration
	_, err := v.db.ExecContext(ctx, `
		INSERT INTO bucket_logging (id, source_bucket, target_bucket, target_prefix, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			target_bucket = VALUES(target_bucket),
			target_prefix = VALUES(target_prefix),
			updated_at = VALUES(updated_at)
	`, config.ID, config.SourceBucket, config.TargetBucket, config.TargetPrefix, now, now)

	return err
}

// DeleteBucketLogging removes the logging configuration for a bucket.
func (v *Vitess) DeleteBucketLogging(ctx context.Context, bucket string) error {
	_, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_logging WHERE source_bucket = ?
	`, bucket)
	return err
}

// ListLoggingConfigs returns all bucket logging configurations.
func (v *Vitess) ListLoggingConfigs(ctx context.Context) ([]*db.BucketLoggingConfig, error) {
	rows, err := v.db.QueryContext(ctx, `
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
