// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// ============================================================================
// Public Access Block Operations
// ============================================================================

func (v *Vitess) GetPublicAccessBlock(ctx context.Context, bucket string) (*s3types.PublicAccessBlockConfig, error) {
	var blockPublicAcls, ignorePublicAcls, blockPublicPolicy, restrictPublicBuckets bool
	err := v.Store.DB().QueryRowContext(ctx, `
		SELECT block_public_acls, ignore_public_acls, block_public_policy, restrict_public_buckets
		FROM bucket_public_access_block WHERE bucket = ?
	`, bucket).Scan(&blockPublicAcls, &ignorePublicAcls, &blockPublicPolicy, &restrictPublicBuckets)

	if err == sql.ErrNoRows {
		return nil, db.ErrPublicAccessBlockNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get public access block: %w", err)
	}

	return &s3types.PublicAccessBlockConfig{
		BlockPublicAcls:       blockPublicAcls,
		IgnorePublicAcls:      ignorePublicAcls,
		BlockPublicPolicy:     blockPublicPolicy,
		RestrictPublicBuckets: restrictPublicBuckets,
	}, nil
}

func (v *Vitess) SetPublicAccessBlock(ctx context.Context, bucket string, config *s3types.PublicAccessBlockConfig) error {
	now := time.Now().UnixNano()
	_, err := v.Store.DB().ExecContext(ctx, `
		INSERT INTO bucket_public_access_block (bucket, block_public_acls, ignore_public_acls, block_public_policy, restrict_public_buckets, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			block_public_acls = VALUES(block_public_acls),
			ignore_public_acls = VALUES(ignore_public_acls),
			block_public_policy = VALUES(block_public_policy),
			restrict_public_buckets = VALUES(restrict_public_buckets),
			updated_at = VALUES(updated_at)
	`, bucket, config.BlockPublicAcls, config.IgnorePublicAcls, config.BlockPublicPolicy, config.RestrictPublicBuckets, now, now)
	if err != nil {
		return fmt.Errorf("set public access block: %w", err)
	}
	return nil
}

func (v *Vitess) DeletePublicAccessBlock(ctx context.Context, bucket string) error {
	result, err := v.Store.DB().ExecContext(ctx, `
		DELETE FROM bucket_public_access_block WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete public access block: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrPublicAccessBlockNotFound
	}
	return nil
}

// ============================================================================
// Ownership Controls Operations
// ============================================================================

func (v *Vitess) GetOwnershipControls(ctx context.Context, bucket string) (*s3types.OwnershipControls, error) {
	var objectOwnership string
	err := v.Store.DB().QueryRowContext(ctx, `
		SELECT object_ownership FROM bucket_ownership_controls WHERE bucket = ?
	`, bucket).Scan(&objectOwnership)

	if err == sql.ErrNoRows {
		return nil, db.ErrOwnershipControlsNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get ownership controls: %w", err)
	}

	return &s3types.OwnershipControls{
		Rules: []s3types.OwnershipControlsRule{
			{ObjectOwnership: objectOwnership},
		},
	}, nil
}

func (v *Vitess) SetOwnershipControls(ctx context.Context, bucket string, controls *s3types.OwnershipControls) error {
	if len(controls.Rules) == 0 {
		return fmt.Errorf("ownership controls must have at least one rule")
	}

	objectOwnership := controls.Rules[0].ObjectOwnership
	now := time.Now().UnixNano()
	_, err := v.Store.DB().ExecContext(ctx, `
		INSERT INTO bucket_ownership_controls (bucket, object_ownership, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE object_ownership = VALUES(object_ownership), updated_at = VALUES(updated_at)
	`, bucket, objectOwnership, now, now)
	if err != nil {
		return fmt.Errorf("set ownership controls: %w", err)
	}
	return nil
}

func (v *Vitess) DeleteOwnershipControls(ctx context.Context, bucket string) error {
	result, err := v.Store.DB().ExecContext(ctx, `
		DELETE FROM bucket_ownership_controls WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete ownership controls: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrOwnershipControlsNotFound
	}
	return nil
}
