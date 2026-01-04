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
// Bucket Operations
// ============================================================================

// CreateBucket creates a new bucket, ignoring if it already exists.
func (s *Store) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	// Use dialect-specific insert ignore syntax
	query := fmt.Sprintf(`
		INSERT %sINTO buckets (id, name, owner_id, region, created_at, default_profile_id, versioning)
		VALUES ($1, $2, $3, $4, $5, $6, $7)%s
	`, s.dialect.InsertIgnorePrefix(), s.dialect.InsertIgnoreSuffix("name"))

	_, err := s.Exec(ctx, query,
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
func (s *Store) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	query := `SELECT ` + BucketColumns + ` FROM buckets WHERE name = $1`
	row := s.QueryRow(ctx, query, name)
	return ScanBucket(row, s.dialect)
}

// DeleteBucket deletes a bucket by name.
func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	result, err := s.Exec(ctx, `DELETE FROM buckets WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrBucketNotFound
	}
	return nil
}

// ListBuckets returns buckets matching the given parameters.
func (s *Store) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
	maxBuckets := params.MaxBuckets
	if maxBuckets <= 0 || maxBuckets > 10000 {
		maxBuckets = 1000
	}

	fetchLimit := maxBuckets + 1

	query := `SELECT ` + BucketColumns + ` FROM buckets WHERE 1=1`
	args := []any{}
	argIdx := 1

	if params.OwnerID != "" {
		query += fmt.Sprintf(" AND owner_id = $%d", argIdx)
		args = append(args, params.OwnerID)
		argIdx++
	}
	if params.Prefix != "" {
		query += fmt.Sprintf(" AND name LIKE $%d", argIdx)
		args = append(args, params.Prefix+"%")
		argIdx++
	}
	if params.BucketRegion != "" {
		query += fmt.Sprintf(" AND region = $%d", argIdx)
		args = append(args, params.BucketRegion)
		argIdx++
	}
	if params.ContinuationToken != "" {
		query += fmt.Sprintf(" AND name > $%d", argIdx)
		args = append(args, params.ContinuationToken)
		argIdx++
	}

	query += " ORDER BY name"
	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, fetchLimit)

	rows, err := s.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	defer rows.Close()

	buckets, err := ScanBuckets(rows, s.dialect)
	if err != nil {
		return nil, err
	}

	result := &db.ListBucketsResult{
		Buckets: buckets,
	}

	if len(buckets) > maxBuckets {
		result.IsTruncated = true
		result.Buckets = buckets[:maxBuckets]
		if len(result.Buckets) > 0 {
			result.NextContinuationToken = result.Buckets[len(result.Buckets)-1].Name
		}
	}

	return result, nil
}

// UpdateBucketVersioning updates the versioning status for a bucket.
func (s *Store) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	_, err := s.Exec(ctx, `UPDATE buckets SET versioning = $1 WHERE name = $2`, versioning, bucket)
	if err != nil {
		return fmt.Errorf("update bucket versioning: %w", err)
	}
	return nil
}

// CountBuckets returns the total number of buckets.
func (s *Store) CountBuckets(ctx context.Context) (int64, error) {
	var count int64
	err := s.QueryRow(ctx, `SELECT COUNT(*) FROM buckets`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count buckets: %w", err)
	}
	return count, nil
}
