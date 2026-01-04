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
// Public Access Block Operations
// ============================================================================

func (p *Postgres) GetPublicAccessBlock(ctx context.Context, bucket string) (*s3types.PublicAccessBlockConfig, error) {
	var blockPublicAcls, ignorePublicAcls, blockPublicPolicy, restrictPublicBuckets bool
	err := p.Store.DB().QueryRowContext(ctx, `
		SELECT block_public_acls, ignore_public_acls, block_public_policy, restrict_public_buckets
		FROM bucket_public_access_block WHERE bucket = $1
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

func (p *Postgres) SetPublicAccessBlock(ctx context.Context, bucket string, config *s3types.PublicAccessBlockConfig) error {
	now := time.Now().UnixNano()
	_, err := p.Store.DB().ExecContext(ctx, `
		INSERT INTO bucket_public_access_block (bucket, block_public_acls, ignore_public_acls, block_public_policy, restrict_public_buckets, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (bucket) DO UPDATE SET
			block_public_acls = EXCLUDED.block_public_acls,
			ignore_public_acls = EXCLUDED.ignore_public_acls,
			block_public_policy = EXCLUDED.block_public_policy,
			restrict_public_buckets = EXCLUDED.restrict_public_buckets,
			updated_at = EXCLUDED.updated_at
	`, bucket, config.BlockPublicAcls, config.IgnorePublicAcls, config.BlockPublicPolicy, config.RestrictPublicBuckets, now, now)
	if err != nil {
		return fmt.Errorf("set public access block: %w", err)
	}
	return nil
}

func (p *Postgres) DeletePublicAccessBlock(ctx context.Context, bucket string) error {
	result, err := p.Store.DB().ExecContext(ctx, `
		DELETE FROM bucket_public_access_block WHERE bucket = $1
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

func (p *Postgres) GetOwnershipControls(ctx context.Context, bucket string) (*s3types.OwnershipControls, error) {
	var objectOwnership string
	err := p.Store.DB().QueryRowContext(ctx, `
		SELECT object_ownership FROM bucket_ownership_controls WHERE bucket = $1
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

func (p *Postgres) SetOwnershipControls(ctx context.Context, bucket string, controls *s3types.OwnershipControls) error {
	if len(controls.Rules) == 0 {
		return fmt.Errorf("ownership controls must have at least one rule")
	}

	objectOwnership := controls.Rules[0].ObjectOwnership
	now := time.Now().UnixNano()
	_, err := p.Store.DB().ExecContext(ctx, `
		INSERT INTO bucket_ownership_controls (bucket, object_ownership, created_at, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (bucket) DO UPDATE SET object_ownership = EXCLUDED.object_ownership, updated_at = EXCLUDED.updated_at
	`, bucket, objectOwnership, now, now)
	if err != nil {
		return fmt.Errorf("set ownership controls: %w", err)
	}
	return nil
}

func (p *Postgres) DeleteOwnershipControls(ctx context.Context, bucket string) error {
	result, err := p.Store.DB().ExecContext(ctx, `
		DELETE FROM bucket_ownership_controls WHERE bucket = $1
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
