// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

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

func (v *Vitess) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	chunkRefsJSON, err := json.Marshal(obj.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk_refs: %w", err)
	}

	ecGroupIDsJSON, err := json.Marshal(obj.ECGroupIDs)
	if err != nil {
		return fmt.Errorf("marshal ec_group_ids: %w", err)
	}

	// Convert IsLatest bool to int for MySQL
	isLatest := 0
	if obj.IsLatest {
		isLatest = 1
	}

	if obj.IsLatest {
		// Versioning mode: mark old versions as not latest, then insert new
		_, err = v.db.ExecContext(ctx, `
			UPDATE objects SET is_latest = 0 WHERE bucket = ? AND object_key = ? AND is_latest = 1
		`, obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("mark old versions: %w", err)
		}

		// Insert new version
		_, err = v.db.ExecContext(ctx, `
			INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
			chunkRefsJSON,
			ecGroupIDsJSON,
			isLatest,
			obj.SSEAlgorithm,
			obj.SSECustomerKeyMD5,
			obj.SSEKMSKeyID,
			obj.SSEKMSContext,
		)
	} else {
		// Non-versioning mode: delete old versions, then insert new (replace behavior)
		_, err = v.db.ExecContext(ctx, `
			DELETE FROM objects WHERE bucket = ? AND object_key = ?
		`, obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("delete old object: %w", err)
		}

		// Insert new object
		_, err = v.db.ExecContext(ctx, `
			INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
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
			chunkRefsJSON,
			ecGroupIDsJSON,
			obj.SSEAlgorithm,
			obj.SSECustomerKeyMD5,
			obj.SSEKMSKeyID,
			obj.SSEKMSContext,
		)
	}
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (v *Vitess) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = ? AND object_key = ? AND is_latest = 1 AND deleted_at = 0
	`, bucket, key)

	return scanObject(row)
}

func (v *Vitess) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE id = ?
	`, id.String())

	return scanObject(row)
}

func (v *Vitess) DeleteObject(ctx context.Context, bucket, key string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM objects WHERE bucket = ? AND object_key = ?
	`, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return db.ErrObjectNotFound
	}
	return nil
}

func (v *Vitess) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	result, err := v.db.ExecContext(ctx, `
		UPDATE objects SET deleted_at = ? WHERE bucket = ? AND object_key = ? AND deleted_at = 0
	`, deletedAt, bucket, key)
	if err != nil {
		return fmt.Errorf("mark object deleted: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return db.ErrObjectNotFound
	}
	return nil
}

func (v *Vitess) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	// Use ListObjectsV2 internally for consistency
	result, err := v.ListObjectsV2(ctx, &db.ListObjectsParams{
		Bucket:  bucket,
		Prefix:  prefix,
		MaxKeys: limit,
	})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

func (v *Vitess) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
	// Determine marker from params
	marker := params.ContinuationToken
	if marker == "" {
		marker = params.Marker
	}
	if marker == "" {
		marker = params.StartAfter
	}

	// Build efficient query with proper indexes
	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = ?
		  AND object_key LIKE ?
		  AND deleted_at = 0
		  AND is_latest = 1
	`
	args := []any{params.Bucket, params.Prefix + "%"}

	// Add marker condition for pagination (uses index efficiently)
	if marker != "" {
		query += " AND object_key > ?"
		args = append(args, marker)
	}

	query += " ORDER BY object_key"

	// Fetch one extra to detect truncation
	fetchLimit := params.MaxKeys + 1
	if fetchLimit > 0 {
		query += " LIMIT ?"
		args = append(args, fetchLimit)
	}

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list objects v2: %w", err)
	}
	defer rows.Close()

	objects, err := scanObjects(rows)
	if err != nil {
		return nil, err
	}

	result := &db.ListObjectsResult{
		CommonPrefixes: make([]string, 0),
	}

	// Handle delimiter for folder simulation
	if params.Delimiter != "" {
		seenPrefixes := make(map[string]bool)
		filteredObjects := make([]*types.ObjectRef, 0, len(objects))

		for _, obj := range objects {
			// Get the part of the key after the prefix
			afterPrefix := obj.Key[len(params.Prefix):]

			// Check if delimiter exists in remaining key
			idx := strings.Index(afterPrefix, params.Delimiter)
			if idx >= 0 {
				// This is a "folder" - extract common prefix
				commonPrefix := params.Prefix + afterPrefix[:idx+len(params.Delimiter)]
				if !seenPrefixes[commonPrefix] {
					seenPrefixes[commonPrefix] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
				}
			} else {
				// Regular object
				filteredObjects = append(filteredObjects, obj)
			}
		}
		objects = filteredObjects
	}

	// Check truncation
	totalItems := len(objects) + len(result.CommonPrefixes)
	if totalItems > params.MaxKeys {
		result.IsTruncated = true
		// Trim to MaxKeys
		if len(objects) > params.MaxKeys {
			objects = objects[:params.MaxKeys]
		}
		// Set continuation token
		if len(objects) > 0 {
			result.NextContinuationToken = objects[len(objects)-1].Key
			result.NextMarker = result.NextContinuationToken
		}
	}

	result.Objects = objects
	return result, nil
}

func (v *Vitess) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE deleted_at > 0 AND deleted_at < ?
		ORDER BY deleted_at
	`
	args := []any{olderThan}

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list deleted objects: %w", err)
	}
	defer rows.Close()

	return scanObjects(rows)
}
