// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/google/uuid"
)

// ============================================================================
// Transaction Object Operations
// ============================================================================

// objectColumns is the standard column list for object queries.
const objectColumns = `id, bucket, object_key, size, version, etag, content_type, created_at, deleted_at, ttl, profile_id, storage_class, transitioned_at, transitioned_ref, restore_status, restore_expiry_date, restore_tier, restore_requested_at, last_accessed_at, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context`

// PutObject creates or updates an object in the database.
func (t *TxStore) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	chunkRefsJSON, err := json.Marshal(obj.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	ecGroupIDsJSON, err := json.Marshal(obj.ECGroupIDs)
	if err != nil {
		return fmt.Errorf("marshal ec group ids: %w", err)
	}

	if obj.IsLatest {
		// Versioning mode: use SELECT FOR UPDATE to prevent race condition
		// where two concurrent PutObject calls both set is_latest=1.
		// This acquires a row lock on the current latest version (if any).
		var existingID string
		err = t.QueryRow(ctx, fmt.Sprintf(`
			SELECT id FROM objects
			WHERE bucket = $1 AND object_key = $2 AND is_latest = %s
			FOR UPDATE
		`, t.dialect.BoolLiteral(true)), obj.Bucket, obj.Key).Scan(&existingID)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("lock current version: %w", err)
		}
		// Note: sql.ErrNoRows is fine - means no existing version to lock

		// Mark old versions as not latest (now safe under row lock)
		_, err = t.Exec(ctx, fmt.Sprintf(`
			UPDATE objects SET is_latest = %s WHERE bucket = $1 AND object_key = $2 AND is_latest = %s
		`, t.dialect.BoolLiteral(false), t.dialect.BoolLiteral(true)), obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("mark old versions: %w", err)
		}

		// Insert new version
		_, err = t.Exec(ctx, `
			INSERT INTO objects (id, bucket, object_key, size, version, etag, content_type, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
		`,
			obj.ID.String(),
			obj.Bucket,
			obj.Key,
			obj.Size,
			obj.Version,
			obj.ETag,
			ContentType(obj.ContentType),
			obj.CreatedAt,
			obj.DeletedAt,
			obj.TTL,
			obj.ProfileID,
			StorageClass(obj.StorageClass),
			string(chunkRefsJSON),
			string(ecGroupIDsJSON),
			t.BoolValue(true),
			obj.SSEAlgorithm,
			obj.SSECustomerKeyMD5,
			obj.SSEKMSKeyID,
			obj.SSEKMSContext,
		)
	} else {
		// Non-versioning mode: delete old, then insert new (replace behavior)
		_, err = t.Exec(ctx, `
			DELETE FROM objects WHERE bucket = $1 AND object_key = $2
		`, obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("delete old object: %w", err)
		}

		_, err = t.Exec(ctx, `
			INSERT INTO objects (id, bucket, object_key, size, version, etag, content_type, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
		`,
			obj.ID.String(),
			obj.Bucket,
			obj.Key,
			obj.Size,
			obj.Version,
			obj.ETag,
			ContentType(obj.ContentType),
			obj.CreatedAt,
			obj.DeletedAt,
			obj.TTL,
			obj.ProfileID,
			StorageClass(obj.StorageClass),
			string(chunkRefsJSON),
			string(ecGroupIDsJSON),
			t.BoolValue(true),
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

// GetObject retrieves the latest version of an object.
func (t *TxStore) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	row := t.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s
		FROM objects
		WHERE bucket = $1 AND object_key = $2 AND is_latest = %s
	`, objectColumns, t.dialect.BoolLiteral(true)), bucket, key)
	return ScanObject(row, t.dialect)
}

// GetObjectByID retrieves an object by its ID.
func (t *TxStore) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	row := t.QueryRow(ctx, `
		SELECT `+objectColumns+`
		FROM objects
		WHERE id = $1
	`, id.String())
	return ScanObject(row, t.dialect)
}

// DeleteObject deletes all versions of an object.
func (t *TxStore) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := t.Exec(ctx, `
		DELETE FROM objects WHERE bucket = $1 AND object_key = $2
	`, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

// MarkObjectDeleted marks an object as soft-deleted.
func (t *TxStore) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	result, err := t.Exec(ctx, `
		UPDATE objects SET deleted_at = $1 WHERE bucket = $2 AND object_key = $3 AND deleted_at = 0
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

// ListObjects lists objects with a prefix.
func (t *TxStore) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	// Use ListObjectsV2 internally for consistency
	result, err := t.ListObjectsV2(ctx, &db.ListObjectsParams{
		Bucket:  bucket,
		Prefix:  prefix,
		MaxKeys: limit,
	})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

// ListObjectsV2 lists objects with full S3 ListObjectsV2 support.
func (t *TxStore) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
	marker := params.ContinuationToken
	if marker == "" {
		marker = params.Marker
	}
	if marker == "" {
		marker = params.StartAfter
	}

	query := fmt.Sprintf(`
		SELECT %s
		FROM objects
		WHERE bucket = $1
		  AND object_key LIKE $2
		  AND deleted_at = 0
		  AND is_latest = %s
	`, objectColumns, t.dialect.BoolLiteral(true))
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

	rows, err := t.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list objects v2: %w", err)
	}
	defer rows.Close()

	objects, err := ScanObjects(rows, t.dialect)
	if err != nil {
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

// ListDeletedObjects returns soft-deleted objects older than a threshold.
func (t *TxStore) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	query := `
		SELECT ` + objectColumns + `
		FROM objects
		WHERE deleted_at > 0 AND deleted_at < $1
		ORDER BY deleted_at
	`
	args := []any{olderThan}

	if limit > 0 {
		query += " LIMIT $2"
		args = append(args, limit)
	}

	rows, err := t.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list deleted objects: %w", err)
	}
	defer rows.Close()

	return ScanObjects(rows, t.dialect)
}

// UpdateObjectTransition updates an object's storage class and transition metadata.
func (t *TxStore) UpdateObjectTransition(ctx context.Context, objectID string, storageClass string, transitionedAt int64, transitionedRef string) error {
	result, err := t.Exec(ctx, `
		UPDATE objects
		SET storage_class = $1, transitioned_at = $2, transitioned_ref = $3
		WHERE id = $4
	`, storageClass, transitionedAt, transitionedRef, objectID)
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
func (t *TxStore) UpdateRestoreStatus(ctx context.Context, objectID string, status string, tier string, requestedAt int64) error {
	result, err := t.Exec(ctx, `
		UPDATE objects
		SET restore_status = $1, restore_tier = $2, restore_requested_at = $3
		WHERE id = $4
	`, status, tier, requestedAt, objectID)
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
func (t *TxStore) UpdateRestoreExpiry(ctx context.Context, objectID string, expiryDate int64) error {
	result, err := t.Exec(ctx, `
		UPDATE objects
		SET restore_expiry_date = $1
		WHERE id = $2
	`, expiryDate, objectID)
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
func (t *TxStore) CompleteRestore(ctx context.Context, objectID string, expiryDate int64) error {
	result, err := t.Exec(ctx, `
		UPDATE objects
		SET restore_status = 'completed', restore_expiry_date = $1
		WHERE id = $2
	`, expiryDate, objectID)
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
func (t *TxStore) ResetRestoreStatus(ctx context.Context, objectID string) error {
	result, err := t.Exec(ctx, `
		UPDATE objects
		SET restore_status = '', restore_expiry_date = 0, restore_tier = '', restore_requested_at = 0
		WHERE id = $1
	`, objectID)
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
func (t *TxStore) UpdateLastAccessedAt(ctx context.Context, objectID string, accessedAt int64) error {
	_, err := t.Exec(ctx, `
		UPDATE objects SET last_accessed_at = $1 WHERE id = $2
	`, accessedAt, objectID)
	if err != nil {
		return fmt.Errorf("update last accessed at: %w", err)
	}
	return nil
}

// GetColdIntelligentTieringObjects finds INTELLIGENT_TIERING objects that haven't been accessed recently.
func (t *TxStore) GetColdIntelligentTieringObjects(ctx context.Context, threshold int64, minSize int64, limit int) ([]*types.ObjectRef, error) {
	// Find INTELLIGENT_TIERING objects that haven't been accessed since threshold.
	// If last_accessed_at = 0, the object was never accessed so use created_at.
	rows, err := t.Query(ctx, fmt.Sprintf(`
		SELECT %s
		FROM objects
		WHERE storage_class = 'INTELLIGENT_TIERING'
		  AND is_latest = %s
		  AND deleted_at = 0
		  AND size >= $1
		  AND ((last_accessed_at > 0 AND last_accessed_at < $2) OR (last_accessed_at = 0 AND created_at < $3))
		LIMIT $4
	`, objectColumns, t.dialect.BoolLiteral(true)), minSize, threshold, threshold, limit)
	if err != nil {
		return nil, fmt.Errorf("query cold intelligent tiering objects: %w", err)
	}
	defer rows.Close()

	return ScanObjects(rows, t.dialect)
}

// GetExpiredRestores returns objects with expired restore copies for cleanup.
func (t *TxStore) GetExpiredRestores(ctx context.Context, now int64, limit int) ([]*types.ObjectRef, error) {
	rows, err := t.Query(ctx, `
		SELECT `+objectColumns+`
		FROM objects
		WHERE restore_status = 'completed' AND restore_expiry_date > 0 AND restore_expiry_date < $1
		ORDER BY restore_expiry_date ASC
		LIMIT $2
	`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("get expired restores: %w", err)
	}
	defer rows.Close()

	return ScanObjects(rows, t.dialect)
}
