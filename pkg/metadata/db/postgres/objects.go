// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/google/uuid"
)

// ============================================================================
// Object Operations
// ============================================================================

func (p *Postgres) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	row := p.db.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE id = $1
	`, id.String())

	return scanObject(row)
}

func (p *Postgres) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	chunkRefsJSON, err := json.Marshal(obj.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}
	ecGroupIDsJSON, err := json.Marshal(obj.ECGroupIDs)
	if err != nil {
		return fmt.Errorf("marshal ec group ids: %w", err)
	}

	// Mark old versions as not latest
	_, err = p.db.ExecContext(ctx, `
		UPDATE objects SET is_latest = FALSE WHERE bucket = $1 AND object_key = $2 AND is_latest = TRUE
	`, obj.Bucket, obj.Key)
	if err != nil {
		return fmt.Errorf("mark old versions not latest: %w", err)
	}

	_, err = p.db.ExecContext(ctx, `
		INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, TRUE, $14, $15, $16, $17)
	`,
		obj.ID.String(),
		obj.Bucket,
		obj.Key,
		obj.Size,
		obj.Version,
		obj.ETag,
		obj.CreatedAt,
		obj.DeletedAt,
		obj.TTL,
		obj.ProfileID,
		storageClass(obj.StorageClass),
		string(chunkRefsJSON),
		string(ecGroupIDsJSON),
		obj.SSEAlgorithm,
		obj.SSECustomerKeyMD5,
		obj.SSEKMSKeyID,
		obj.SSEKMSContext,
	)
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (p *Postgres) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	row := p.db.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = $1 AND object_key = $2 AND deleted_at = 0 AND is_latest = TRUE
	`, bucket, key)

	return scanObject(row)
}

func (p *Postgres) DeleteObject(ctx context.Context, bucket, key string) error {
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM objects WHERE bucket = $1 AND object_key = $2 AND is_latest = TRUE
	`, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrObjectNotFound
	}
	return nil
}

func (p *Postgres) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	result, err := p.ListObjectsV2(ctx, &db.ListObjectsParams{
		Bucket:  bucket,
		Prefix:  prefix,
		MaxKeys: limit,
	})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

func (p *Postgres) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
	marker := params.ContinuationToken
	if marker == "" {
		marker = params.Marker
	}
	if marker == "" {
		marker = params.StartAfter
	}

	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = $1
		  AND object_key LIKE $2
		  AND deleted_at = 0
		  AND is_latest = TRUE
	`
	args := []any{params.Bucket, params.Prefix + "%"}
	argIdx := 3

	if marker != "" {
		query += fmt.Sprintf(" AND object_key > $%d", argIdx)
		args = append(args, marker)
		argIdx++
	}

	query += " ORDER BY object_key"

	fetchLimit := params.MaxKeys + 1
	if fetchLimit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, fetchLimit)
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list objects v2: %w", err)
	}
	defer rows.Close()

	var objects []*types.ObjectRef
	for rows.Next() {
		obj, err := scanObject(rows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := &db.ListObjectsResult{
		CommonPrefixes: make([]string, 0),
	}

	if params.Delimiter != "" {
		seenPrefixes := make(map[string]bool)
		filteredObjects := make([]*types.ObjectRef, 0, len(objects))

		for _, obj := range objects {
			afterPrefix := obj.Key[len(params.Prefix):]
			idx := strings.Index(afterPrefix, params.Delimiter)
			if idx >= 0 {
				commonPrefix := params.Prefix + afterPrefix[:idx+len(params.Delimiter)]
				if !seenPrefixes[commonPrefix] {
					seenPrefixes[commonPrefix] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
				}
			} else {
				filteredObjects = append(filteredObjects, obj)
			}
		}
		objects = filteredObjects
	}

	totalItems := len(objects) + len(result.CommonPrefixes)
	if totalItems > params.MaxKeys {
		result.IsTruncated = true
		if len(objects) > params.MaxKeys {
			objects = objects[:params.MaxKeys]
		}
		if len(objects) > 0 {
			result.NextContinuationToken = objects[len(objects)-1].Key
			result.NextMarker = result.NextContinuationToken
		}
	}

	result.Objects = objects
	return result, nil
}

func (p *Postgres) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE deleted_at > 0 AND deleted_at < $1
		ORDER BY deleted_at
	`
	args := []any{olderThan}

	if limit > 0 {
		query += " LIMIT $2"
		args = append(args, limit)
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list deleted objects: %w", err)
	}
	defer rows.Close()

	var objects []*types.ObjectRef
	for rows.Next() {
		obj, err := scanObject(rows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}

	return objects, rows.Err()
}

func (p *Postgres) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE objects SET deleted_at = $1 WHERE bucket = $2 AND object_key = $3 AND is_latest = TRUE
	`, deletedAt, bucket, key)
	if err != nil {
		return fmt.Errorf("mark object deleted: %w", err)
	}
	return nil
}
