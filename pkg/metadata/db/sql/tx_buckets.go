// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// ============================================================================
// Transaction Bucket Operations
// ============================================================================

// CreateBucket creates a new bucket using INSERT IGNORE/ON CONFLICT DO NOTHING.
func (t *TxStore) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	query := fmt.Sprintf(`
		INSERT %sINTO buckets (id, name, owner_id, region, created_at, default_profile_id, versioning)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		%s
	`, t.dialect.InsertIgnorePrefix(), t.dialect.InsertIgnoreSuffix("name"))

	_, err := t.Exec(ctx, query,
		bucket.ID.String(),
		bucket.Name,
		bucket.OwnerID,
		bucket.Region,
		bucket.CreatedAt,
		bucket.DefaultProfileID,
		bucket.Versioning,
	)
	if err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	return nil
}

// GetBucket retrieves a bucket by name.
func (t *TxStore) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	row := t.QueryRow(ctx, `
		SELECT `+BucketColumns+`
		FROM buckets
		WHERE name = $1
	`, name)

	return ScanBucket(row, t.dialect)
}

// DeleteBucket deletes a bucket by name.
// Note: Does not check RowsAffected - caller should verify bucket exists if needed.
func (t *TxStore) DeleteBucket(ctx context.Context, name string) error {
	_, err := t.Exec(ctx, `DELETE FROM buckets WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}
	return nil
}

// ListBuckets lists buckets with optional filtering and pagination.
func (t *TxStore) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
	// Build query with filters - always filter by owner_id
	query := `
		SELECT ` + BucketColumns + `
		FROM buckets
		WHERE owner_id = $1
	`
	args := []any{params.OwnerID}
	argIdx := 2

	// Add prefix filter
	if params.Prefix != "" {
		query += fmt.Sprintf(" AND name LIKE $%d", argIdx)
		args = append(args, params.Prefix+"%")
		argIdx++
	}

	// Add region filter
	if params.BucketRegion != "" {
		query += fmt.Sprintf(" AND region = $%d", argIdx)
		args = append(args, params.BucketRegion)
		argIdx++
	}

	// Add continuation token (pagination marker)
	if params.ContinuationToken != "" {
		query += fmt.Sprintf(" AND name > $%d", argIdx)
		args = append(args, params.ContinuationToken)
		argIdx++
	}

	// Order by name for consistent pagination
	query += " ORDER BY name"

	// Determine fetch limit (fetch one extra to detect truncation)
	maxBuckets := params.MaxBuckets
	if maxBuckets <= 0 {
		maxBuckets = 10000
	}
	fetchLimit := maxBuckets + 1
	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, fetchLimit)

	rows, err := t.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	defer rows.Close()

	buckets, err := ScanBuckets(rows, t.dialect)
	if err != nil {
		return nil, err
	}

	// Check for truncation
	result := &db.ListBucketsResult{
		Buckets: buckets,
	}
	if len(buckets) > maxBuckets {
		result.IsTruncated = true
		result.Buckets = buckets[:maxBuckets]
		// Next continuation token is the name of the last bucket returned
		if len(result.Buckets) > 0 {
			result.NextContinuationToken = result.Buckets[len(result.Buckets)-1].Name
		}
	}

	return result, nil
}

// UpdateBucketVersioning updates the versioning status for a bucket.
func (t *TxStore) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	_, err := t.Exec(ctx, `
		UPDATE buckets SET versioning = $1 WHERE name = $2
	`, versioning, bucket)
	if err != nil {
		return fmt.Errorf("update bucket versioning: %w", err)
	}
	return nil
}

// CountBuckets returns the total number of buckets.
func (t *TxStore) CountBuckets(ctx context.Context) (int64, error) {
	var count int64
	err := t.QueryRow(ctx, `SELECT COUNT(*) FROM buckets`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count buckets: %w", err)
	}
	return count, nil
}
