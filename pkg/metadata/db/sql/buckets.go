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
// Bucket Operations - Store
// ============================================================================

func (s *Store) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	return createBucket(ctx, s, bucket)
}

func (s *Store) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	return getBucket(ctx, s, name)
}

func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	return deleteBucket(ctx, s, name, true)
}

func (s *Store) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
	return listBuckets(ctx, s, params)
}

func (s *Store) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	return updateBucketVersioning(ctx, s, bucket, versioning)
}

func (s *Store) CountBuckets(ctx context.Context) (int64, error) {
	return countBuckets(ctx, s)
}

// ============================================================================
// Bucket Operations - TxStore
// ============================================================================

func (t *TxStore) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	return createBucket(ctx, t, bucket)
}

func (t *TxStore) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	return getBucket(ctx, t, name)
}

func (t *TxStore) DeleteBucket(ctx context.Context, name string) error {
	return deleteBucket(ctx, t, name, false) // TxStore doesn't check rows affected
}

func (t *TxStore) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
	return listBuckets(ctx, t, params)
}

func (t *TxStore) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	return updateBucketVersioning(ctx, t, bucket, versioning)
}

func (t *TxStore) CountBuckets(ctx context.Context) (int64, error) {
	return countBuckets(ctx, t)
}

// ============================================================================
// Shared Implementations
// ============================================================================

func createBucket(ctx context.Context, q Querier, bucket *types.BucketInfo) error {
	query := fmt.Sprintf(`
		INSERT %sINTO buckets (id, name, owner_id, region, created_at, default_profile_id, versioning)
		VALUES ($1, $2, $3, $4, $5, $6, $7)%s
	`, q.Dialect().InsertIgnorePrefix(), q.Dialect().InsertIgnoreSuffix("name"))

	_, err := q.Exec(ctx, query,
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

func getBucket(ctx context.Context, q Querier, name string) (*types.BucketInfo, error) {
	query := `SELECT ` + BucketColumns + ` FROM buckets WHERE name = $1`
	row := q.QueryRow(ctx, query, name)
	return ScanBucket(row, q.Dialect())
}

func deleteBucket(ctx context.Context, q Querier, name string, checkAffected bool) error {
	result, err := q.Exec(ctx, `DELETE FROM buckets WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}

	if checkAffected {
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return db.ErrBucketNotFound
		}
	}
	return nil
}

func listBuckets(ctx context.Context, q Querier, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
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

	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	defer rows.Close()

	buckets, err := ScanBuckets(rows, q.Dialect())
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

func updateBucketVersioning(ctx context.Context, q Querier, bucket string, versioning string) error {
	_, err := q.Exec(ctx, `UPDATE buckets SET versioning = $1 WHERE name = $2`, versioning, bucket)
	if err != nil {
		return fmt.Errorf("update bucket versioning: %w", err)
	}
	return nil
}

func countBuckets(ctx context.Context, q Querier) (int64, error) {
	var count int64
	err := q.QueryRow(ctx, `SELECT COUNT(*) FROM buckets`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count buckets: %w", err)
	}
	return count, nil
}
