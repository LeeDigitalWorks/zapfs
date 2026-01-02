// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/google/uuid"
)

// ============================================================================
// Bucket Operations
// ============================================================================

func (v *Vitess) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	_, err := v.db.ExecContext(ctx, `
		INSERT INTO buckets (id, name, owner_id, region, created_at, default_profile_id, versioning)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
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

func (v *Vitess) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, name, owner_id, region, created_at, default_profile_id, versioning
		FROM buckets
		WHERE name = ?
	`, name)

	var bucket types.BucketInfo
	var idStr string
	var region, profileID, versioning sql.NullString

	err := row.Scan(
		&idStr,
		&bucket.Name,
		&bucket.OwnerID,
		&region,
		&bucket.CreatedAt,
		&profileID,
		&versioning,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrBucketNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan bucket: %w", err)
	}

	bucket.ID, _ = uuid.Parse(idStr)
	bucket.Region = region.String
	bucket.DefaultProfileID = profileID.String
	bucket.Versioning = versioning.String

	return &bucket, nil
}

func (v *Vitess) DeleteBucket(ctx context.Context, name string) error {
	result, err := v.db.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return db.ErrBucketNotFound
	}
	return nil
}

func (v *Vitess) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
	// Build query with filters
	query := `
		SELECT id, name, owner_id, region, created_at, default_profile_id, versioning
		FROM buckets
		WHERE 1=1
	`
	var args []any

	// Add owner filter (optional - when empty, returns all buckets for cache loading)
	if params.OwnerID != "" {
		query += " AND owner_id = ?"
		args = append(args, params.OwnerID)
	}

	// Add prefix filter
	if params.Prefix != "" {
		query += " AND name LIKE ?"
		args = append(args, params.Prefix+"%")
	}

	// Add region filter
	if params.BucketRegion != "" {
		query += " AND region = ?"
		args = append(args, params.BucketRegion)
	}

	// Add continuation token (pagination marker)
	if params.ContinuationToken != "" {
		query += " AND name > ?"
		args = append(args, params.ContinuationToken)
	}

	// Order by name for consistent pagination
	query += " ORDER BY name"

	// Determine fetch limit (fetch one extra to detect truncation)
	maxBuckets := params.MaxBuckets
	if maxBuckets <= 0 {
		maxBuckets = 10000
	}
	fetchLimit := maxBuckets + 1
	query += " LIMIT ?"
	args = append(args, fetchLimit)

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	defer rows.Close()

	var buckets []*types.BucketInfo
	for rows.Next() {
		var bucket types.BucketInfo
		var idStr string
		var region, profileID, versioning sql.NullString

		err := rows.Scan(
			&idStr,
			&bucket.Name,
			&bucket.OwnerID,
			&region,
			&bucket.CreatedAt,
			&profileID,
			&versioning,
		)
		if err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}

		bucket.ID, _ = uuid.Parse(idStr)
		bucket.Region = region.String
		bucket.DefaultProfileID = profileID.String
		bucket.Versioning = versioning.String

		buckets = append(buckets, &bucket)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
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

func (v *Vitess) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	_, err := v.db.ExecContext(ctx, `
		UPDATE buckets SET versioning = ? WHERE name = ?
	`, versioning, bucket)
	if err != nil {
		return fmt.Errorf("update bucket versioning: %w", err)
	}
	return nil
}
