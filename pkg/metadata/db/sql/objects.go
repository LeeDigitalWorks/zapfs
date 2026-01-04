// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/google/uuid"
)

// ObjectColumns is the standard column list for object queries.
const ObjectColumns = `id, bucket, object_key, size, version, etag, content_type, created_at, deleted_at, ttl, profile_id, storage_class, transitioned_at, transitioned_ref, restore_status, restore_expiry_date, restore_tier, restore_requested_at, last_accessed_at, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context`

// ============================================================================
// Object Operations
// ============================================================================

// GetObjectByID retrieves an object by its unique ID.
func (s *Store) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	query := `
		SELECT ` + ObjectColumns + `
		FROM objects
		WHERE id = $1
	`
	row := s.QueryRow(ctx, query, id.String())
	return ScanObject(row, s.dialect)
}

// GetObject retrieves the latest version of an object by bucket and key.
func (s *Store) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	query := `
		SELECT ` + ObjectColumns + `
		FROM objects
		WHERE bucket = $1 AND object_key = $2 AND deleted_at = 0 AND ` + s.dialect.BoolColumn("is_latest", true) + `
	`
	row := s.QueryRow(ctx, query, bucket, key)
	return ScanObject(row, s.dialect)
}

// DeleteObject permanently deletes the latest version of an object.
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	query := `DELETE FROM objects WHERE bucket = $1 AND object_key = $2 AND ` + s.dialect.BoolColumn("is_latest", true)
	result, err := s.Exec(ctx, query, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrObjectNotFound
	}
	return nil
}

// ListObjects returns objects matching the prefix.
func (s *Store) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	result, err := s.ListObjectsV2(ctx, &db.ListObjectsParams{
		Bucket:  bucket,
		Prefix:  prefix,
		MaxKeys: limit,
	})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

// ListObjectsV2 returns objects with full S3 ListObjectsV2 semantics.
func (s *Store) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
	marker := params.ContinuationToken
	if marker == "" {
		marker = params.Marker
	}
	if marker == "" {
		marker = params.StartAfter
	}

	// Base query - placeholders will be converted by dialect
	query := `
		SELECT ` + ObjectColumns + `
		FROM objects
		WHERE bucket = $1
		  AND object_key LIKE $2
		  AND deleted_at = 0
		  AND ` + s.dialect.BoolColumn("is_latest", true) + `
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

	rows, err := s.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list objects v2: %w", err)
	}
	defer rows.Close()

	objects, err := ScanObjects(rows, s.dialect)
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

// ListDeletedObjects returns soft-deleted objects older than the threshold.
func (s *Store) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	query := `
		SELECT ` + ObjectColumns + `
		FROM objects
		WHERE deleted_at > 0 AND deleted_at < $1
		ORDER BY deleted_at
	`
	args := []any{olderThan}

	if limit > 0 {
		query += " LIMIT $2"
		args = append(args, limit)
	}

	rows, err := s.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list deleted objects: %w", err)
	}
	defer rows.Close()

	return ScanObjects(rows, s.dialect)
}

// MarkObjectDeleted soft-deletes an object by setting deleted_at.
func (s *Store) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	query := `UPDATE objects SET deleted_at = $1 WHERE bucket = $2 AND object_key = $3 AND ` + s.dialect.BoolColumn("is_latest", true)
	_, err := s.Exec(ctx, query, deletedAt, bucket, key)
	if err != nil {
		return fmt.Errorf("mark object deleted: %w", err)
	}
	return nil
}

// UpdateObjectTransition updates an object's storage class and transition metadata.
func (s *Store) UpdateObjectTransition(ctx context.Context, objectID string, storageClass string, transitionedAt int64, transitionedRef string) error {
	query := `
		UPDATE objects
		SET storage_class = $1, transitioned_at = $2, transitioned_ref = $3
		WHERE id = $4
	`
	result, err := s.Exec(ctx, query, storageClass, transitionedAt, transitionedRef, objectID)
	if err != nil {
		return fmt.Errorf("update object transition: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rows == 0 {
		return db.ErrObjectNotFound
	}

	return nil
}

// UpdateRestoreStatus updates the restore status for an archived object.
func (s *Store) UpdateRestoreStatus(ctx context.Context, objectID string, status string, tier string, requestedAt int64) error {
	query := `
		UPDATE objects
		SET restore_status = $1, restore_tier = $2, restore_requested_at = $3
		WHERE id = $4
	`
	result, err := s.Exec(ctx, query, status, tier, requestedAt, objectID)
	if err != nil {
		return fmt.Errorf("update restore status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rows == 0 {
		return db.ErrObjectNotFound
	}

	return nil
}

// UpdateRestoreExpiry extends the expiry date of a restored object copy.
func (s *Store) UpdateRestoreExpiry(ctx context.Context, objectID string, expiryDate int64) error {
	query := `
		UPDATE objects
		SET restore_expiry_date = $1
		WHERE id = $2
	`
	result, err := s.Exec(ctx, query, expiryDate, objectID)
	if err != nil {
		return fmt.Errorf("update restore expiry: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rows == 0 {
		return db.ErrObjectNotFound
	}

	return nil
}

// CompleteRestore marks a restore as complete.
func (s *Store) CompleteRestore(ctx context.Context, objectID string, expiryDate int64) error {
	query := `
		UPDATE objects
		SET restore_status = 'completed', restore_expiry_date = $1
		WHERE id = $2
	`
	result, err := s.Exec(ctx, query, expiryDate, objectID)
	if err != nil {
		return fmt.Errorf("complete restore: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rows == 0 {
		return db.ErrObjectNotFound
	}

	return nil
}

// ResetRestoreStatus clears the restore status after a restored copy expires.
func (s *Store) ResetRestoreStatus(ctx context.Context, objectID string) error {
	query := `
		UPDATE objects
		SET restore_status = '', restore_expiry_date = 0, restore_tier = '', restore_requested_at = 0
		WHERE id = $1
	`
	result, err := s.Exec(ctx, query, objectID)
	if err != nil {
		return fmt.Errorf("reset restore status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rows == 0 {
		return db.ErrObjectNotFound
	}

	return nil
}

// UpdateLastAccessedAt updates the last access timestamp for an object.
func (s *Store) UpdateLastAccessedAt(ctx context.Context, objectID string, accessedAt int64) error {
	query := `UPDATE objects SET last_accessed_at = $1 WHERE id = $2`
	_, err := s.Exec(ctx, query, accessedAt, objectID)
	if err != nil {
		return fmt.Errorf("update last accessed at: %w", err)
	}
	return nil
}

// GetColdIntelligentTieringObjects finds INTELLIGENT_TIERING objects not accessed since threshold.
func (s *Store) GetColdIntelligentTieringObjects(ctx context.Context, threshold int64, minSize int64, limit int) ([]*types.ObjectRef, error) {
	query := `
		SELECT ` + ObjectColumns + `
		FROM objects
		WHERE storage_class = 'INTELLIGENT_TIERING'
		  AND ` + s.dialect.BoolColumn("is_latest", true) + `
		  AND deleted_at = 0
		  AND size >= $1
		  AND ((last_accessed_at > 0 AND last_accessed_at < $2) OR (last_accessed_at = 0 AND created_at < $3))
		LIMIT $4
	`
	rows, err := s.Query(ctx, query, minSize, threshold, threshold, limit)
	if err != nil {
		return nil, fmt.Errorf("query cold intelligent tiering objects: %w", err)
	}
	defer rows.Close()

	objects, err := ScanObjects(rows, s.dialect)
	if err != nil {
		return nil, fmt.Errorf("scan objects: %w", err)
	}
	return objects, nil
}

// GetExpiredRestores returns objects with expired restore copies for cleanup.
func (s *Store) GetExpiredRestores(ctx context.Context, now int64, limit int) ([]*types.ObjectRef, error) {
	query := `
		SELECT ` + ObjectColumns + `
		FROM objects
		WHERE restore_status = 'completed' AND restore_expiry_date > 0 AND restore_expiry_date < $1
		ORDER BY restore_expiry_date ASC
		LIMIT $2
	`
	rows, err := s.Query(ctx, query, now, limit)
	if err != nil {
		return nil, fmt.Errorf("get expired restores: %w", err)
	}
	defer rows.Close()

	return ScanObjects(rows, s.dialect)
}
