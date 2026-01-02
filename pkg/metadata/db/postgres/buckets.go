// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

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

func (p *Postgres) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	_, err := p.db.ExecContext(ctx, `
		INSERT INTO buckets (id, name, owner_id, region, created_at, versioning)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, bucket.ID.String(), bucket.Name, bucket.OwnerID, bucket.Region, bucket.CreatedAt, bucket.Versioning)
	if err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	return nil
}

func (p *Postgres) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	row := p.db.QueryRowContext(ctx, `
		SELECT id, name, owner_id, region, created_at, versioning FROM buckets WHERE name = $1
	`, name)

	var bucket types.BucketInfo
	var id string
	var region sql.NullString
	if err := row.Scan(&id, &bucket.Name, &bucket.OwnerID, &region, &bucket.CreatedAt, &bucket.Versioning); err != nil {
		if err == sql.ErrNoRows {
			return nil, db.ErrBucketNotFound
		}
		return nil, fmt.Errorf("get bucket: %w", err)
	}
	bucket.ID, _ = uuid.Parse(id)
	if region.Valid {
		bucket.Region = region.String
	}
	return &bucket, nil
}

func (p *Postgres) DeleteBucket(ctx context.Context, name string) error {
	result, err := p.db.ExecContext(ctx, `DELETE FROM buckets WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrBucketNotFound
	}
	return nil
}

func (p *Postgres) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
	maxBuckets := params.MaxBuckets
	if maxBuckets <= 0 || maxBuckets > 10000 {
		maxBuckets = 1000
	}

	fetchLimit := maxBuckets + 1

	query := `SELECT id, name, owner_id, region, created_at, versioning FROM buckets WHERE 1=1`
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

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	defer rows.Close()

	var buckets []*types.BucketInfo
	for rows.Next() {
		var bucket types.BucketInfo
		var id string
		var region sql.NullString
		if err := rows.Scan(&id, &bucket.Name, &bucket.OwnerID, &region, &bucket.CreatedAt, &bucket.Versioning); err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}
		bucket.ID, _ = uuid.Parse(id)
		if region.Valid {
			bucket.Region = region.String
		}
		buckets = append(buckets, &bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := &db.ListBucketsResult{
		Buckets: buckets,
	}

	if len(buckets) > maxBuckets {
		result.IsTruncated = true
		result.Buckets = buckets[:maxBuckets]
		result.NextContinuationToken = result.Buckets[len(result.Buckets)-1].Name
	}

	return result, nil
}

func (p *Postgres) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	_, err := p.db.ExecContext(ctx, `UPDATE buckets SET versioning = $1 WHERE name = $2`, versioning, bucket)
	if err != nil {
		return fmt.Errorf("update bucket versioning: %w", err)
	}
	return nil
}
