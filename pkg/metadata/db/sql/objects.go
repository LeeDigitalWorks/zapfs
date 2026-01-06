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

// ObjectColumns is the standard column list for object queries.
const ObjectColumns = `id, bucket, object_key, size, version, etag, content_type, created_at, deleted_at, ttl, profile_id, storage_class, transitioned_at, transitioned_ref, restore_status, restore_expiry_date, restore_tier, restore_requested_at, last_accessed_at, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context, metadata`

// ============================================================================
// Object Operations - Store
// ============================================================================

func (s *Store) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	return getObjectByID(ctx, s, id)
}

func (s *Store) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	return getObject(ctx, s, bucket, key)
}

func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	return deleteObject(ctx, s, bucket, key, true) // Store checks rows affected
}

func (s *Store) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	return listObjects(ctx, s, bucket, prefix, limit)
}

func (s *Store) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
	return listObjectsV2(ctx, s, params)
}

func (s *Store) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	return listDeletedObjects(ctx, s, olderThan, limit)
}

func (s *Store) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	return markObjectDeletedLatest(ctx, s, bucket, key, deletedAt)
}

func (s *Store) UpdateObjectTransition(ctx context.Context, objectID string, storageClass string, transitionedAt int64, transitionedRef string) error {
	return updateObjectTransition(ctx, s, objectID, storageClass, transitionedAt, transitionedRef)
}

func (s *Store) UpdateRestoreStatus(ctx context.Context, objectID string, status string, tier string, requestedAt int64) error {
	return updateRestoreStatus(ctx, s, objectID, status, tier, requestedAt)
}

func (s *Store) UpdateRestoreExpiry(ctx context.Context, objectID string, expiryDate int64) error {
	return updateRestoreExpiry(ctx, s, objectID, expiryDate)
}

func (s *Store) CompleteRestore(ctx context.Context, objectID string, expiryDate int64) error {
	return completeRestore(ctx, s, objectID, expiryDate)
}

func (s *Store) ResetRestoreStatus(ctx context.Context, objectID string) error {
	return resetRestoreStatus(ctx, s, objectID)
}

func (s *Store) UpdateLastAccessedAt(ctx context.Context, objectID string, accessedAt int64) error {
	return updateLastAccessedAt(ctx, s, objectID, accessedAt)
}

func (s *Store) GetColdIntelligentTieringObjects(ctx context.Context, threshold int64, minSize int64, limit int) ([]*types.ObjectRef, error) {
	return getColdIntelligentTieringObjects(ctx, s, threshold, minSize, limit)
}

func (s *Store) GetExpiredRestores(ctx context.Context, now int64, limit int) ([]*types.ObjectRef, error) {
	return getExpiredRestores(ctx, s, now, limit)
}

// ============================================================================
// Object Operations - TxStore
// ============================================================================

func (t *TxStore) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	return getObjectByID(ctx, t, id)
}

func (t *TxStore) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	return getObject(ctx, t, bucket, key)
}

func (t *TxStore) DeleteObject(ctx context.Context, bucket, key string) error {
	return deleteObject(ctx, t, bucket, key, false) // TxStore doesn't check rows affected
}

func (t *TxStore) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	return listObjects(ctx, t, bucket, prefix, limit)
}

func (t *TxStore) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
	return listObjectsV2(ctx, t, params)
}

func (t *TxStore) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	return listDeletedObjects(ctx, t, olderThan, limit)
}

func (t *TxStore) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	return markObjectDeletedAny(ctx, t, bucket, key, deletedAt)
}

func (t *TxStore) UpdateObjectTransition(ctx context.Context, objectID string, storageClass string, transitionedAt int64, transitionedRef string) error {
	return updateObjectTransition(ctx, t, objectID, storageClass, transitionedAt, transitionedRef)
}

func (t *TxStore) UpdateRestoreStatus(ctx context.Context, objectID string, status string, tier string, requestedAt int64) error {
	return updateRestoreStatus(ctx, t, objectID, status, tier, requestedAt)
}

func (t *TxStore) UpdateRestoreExpiry(ctx context.Context, objectID string, expiryDate int64) error {
	return updateRestoreExpiry(ctx, t, objectID, expiryDate)
}

func (t *TxStore) CompleteRestore(ctx context.Context, objectID string, expiryDate int64) error {
	return completeRestore(ctx, t, objectID, expiryDate)
}

func (t *TxStore) ResetRestoreStatus(ctx context.Context, objectID string) error {
	return resetRestoreStatus(ctx, t, objectID)
}

func (t *TxStore) UpdateLastAccessedAt(ctx context.Context, objectID string, accessedAt int64) error {
	return updateLastAccessedAt(ctx, t, objectID, accessedAt)
}

func (t *TxStore) GetColdIntelligentTieringObjects(ctx context.Context, threshold int64, minSize int64, limit int) ([]*types.ObjectRef, error) {
	return getColdIntelligentTieringObjects(ctx, t, threshold, minSize, limit)
}

func (t *TxStore) GetExpiredRestores(ctx context.Context, now int64, limit int) ([]*types.ObjectRef, error) {
	return getExpiredRestores(ctx, t, now, limit)
}

// PutObject creates or updates an object in the database.
// This is TxStore-only as it requires transaction semantics.
func (t *TxStore) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	chunkRefsJSON, err := json.Marshal(obj.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	ecGroupIDsJSON, err := json.Marshal(obj.ECGroupIDs)
	if err != nil {
		return fmt.Errorf("marshal ec group ids: %w", err)
	}

	metadataJSON, err := json.Marshal(obj.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	if obj.IsLatest {
		// Versioning mode: use SELECT FOR UPDATE to prevent race condition
		var existingID string
		err = t.QueryRow(ctx, fmt.Sprintf(`
			SELECT id FROM objects
			WHERE bucket = $1 AND object_key = $2 AND is_latest = %s
			FOR UPDATE
		`, t.dialect.BoolLiteral(true)), obj.Bucket, obj.Key).Scan(&existingID)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("lock current version: %w", err)
		}

		// Mark old versions as not latest
		_, err = t.Exec(ctx, fmt.Sprintf(`
			UPDATE objects SET is_latest = %s WHERE bucket = $1 AND object_key = $2 AND is_latest = %s
		`, t.dialect.BoolLiteral(false), t.dialect.BoolLiteral(true)), obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("mark old versions: %w", err)
		}

		// Insert new version
		_, err = t.Exec(ctx, `
			INSERT INTO objects (id, bucket, object_key, size, version, etag, content_type, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
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
			string(metadataJSON),
		)
	} else {
		// Non-versioning mode: delete old, then insert new
		_, err = t.Exec(ctx, `
			DELETE FROM objects WHERE bucket = $1 AND object_key = $2
		`, obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("delete old object: %w", err)
		}

		_, err = t.Exec(ctx, `
			INSERT INTO objects (id, bucket, object_key, size, version, etag, content_type, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
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
			string(metadataJSON),
		)
	}
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

// ============================================================================
// Shared Implementations
// ============================================================================

func getObjectByID(ctx context.Context, q Querier, id uuid.UUID) (*types.ObjectRef, error) {
	query := `SELECT ` + ObjectColumns + ` FROM objects WHERE id = $1`
	row := q.QueryRow(ctx, query, id.String())
	return ScanObject(row, q.Dialect())
}

func getObject(ctx context.Context, q Querier, bucket, key string) (*types.ObjectRef, error) {
	query := `
		SELECT ` + ObjectColumns + `
		FROM objects
		WHERE bucket = $1 AND object_key = $2 AND deleted_at = 0 AND ` + q.Dialect().BoolColumn("is_latest", true)
	row := q.QueryRow(ctx, query, bucket, key)
	return ScanObject(row, q.Dialect())
}

func deleteObject(ctx context.Context, q Querier, bucket, key string, checkAffected bool) error {
	query := `DELETE FROM objects WHERE bucket = $1 AND object_key = $2 AND ` + q.Dialect().BoolColumn("is_latest", true)
	result, err := q.Exec(ctx, query, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	if checkAffected {
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return db.ErrObjectNotFound
		}
	}
	return nil
}

func listObjects(ctx context.Context, q Querier, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	result, err := listObjectsV2(ctx, q, &db.ListObjectsParams{
		Bucket:  bucket,
		Prefix:  prefix,
		MaxKeys: limit,
	})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

func listObjectsV2(ctx context.Context, q Querier, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
	marker := params.ContinuationToken
	if marker == "" {
		marker = params.Marker
	}
	if marker == "" {
		marker = params.StartAfter
	}

	query := `
		SELECT ` + ObjectColumns + `
		FROM objects
		WHERE bucket = $1
		  AND object_key LIKE $2
		  AND deleted_at = 0
		  AND ` + q.Dialect().BoolColumn("is_latest", true)
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

	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list objects v2: %w", err)
	}
	defer rows.Close()

	objects, err := ScanObjects(rows, q.Dialect())
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

func listDeletedObjects(ctx context.Context, q Querier, olderThan int64, limit int) ([]*types.ObjectRef, error) {
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

	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list deleted objects: %w", err)
	}
	defer rows.Close()

	return ScanObjects(rows, q.Dialect())
}

// markObjectDeletedLatest marks only the latest version as deleted (Store behavior).
func markObjectDeletedLatest(ctx context.Context, q Querier, bucket, key string, deletedAt int64) error {
	query := `UPDATE objects SET deleted_at = $1 WHERE bucket = $2 AND object_key = $3 AND ` + q.Dialect().BoolColumn("is_latest", true)
	_, err := q.Exec(ctx, query, deletedAt, bucket, key)
	if err != nil {
		return fmt.Errorf("mark object deleted: %w", err)
	}
	return nil
}

// markObjectDeletedAny marks any non-deleted version as deleted (TxStore behavior).
func markObjectDeletedAny(ctx context.Context, q Querier, bucket, key string, deletedAt int64) error {
	result, err := q.Exec(ctx, `
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

func updateObjectTransition(ctx context.Context, q Querier, objectID string, storageClass string, transitionedAt int64, transitionedRef string) error {
	query := `
		UPDATE objects
		SET storage_class = $1, transitioned_at = $2, transitioned_ref = $3
		WHERE id = $4
	`
	result, err := q.Exec(ctx, query, storageClass, transitionedAt, transitionedRef, objectID)
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

func updateRestoreStatus(ctx context.Context, q Querier, objectID string, status string, tier string, requestedAt int64) error {
	query := `
		UPDATE objects
		SET restore_status = $1, restore_tier = $2, restore_requested_at = $3
		WHERE id = $4
	`
	result, err := q.Exec(ctx, query, status, tier, requestedAt, objectID)
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

func updateRestoreExpiry(ctx context.Context, q Querier, objectID string, expiryDate int64) error {
	query := `
		UPDATE objects
		SET restore_expiry_date = $1
		WHERE id = $2
	`
	result, err := q.Exec(ctx, query, expiryDate, objectID)
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

func completeRestore(ctx context.Context, q Querier, objectID string, expiryDate int64) error {
	query := `
		UPDATE objects
		SET restore_status = 'completed', restore_expiry_date = $1
		WHERE id = $2
	`
	result, err := q.Exec(ctx, query, expiryDate, objectID)
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

func resetRestoreStatus(ctx context.Context, q Querier, objectID string) error {
	query := `
		UPDATE objects
		SET restore_status = '', restore_expiry_date = 0, restore_tier = '', restore_requested_at = 0
		WHERE id = $1
	`
	result, err := q.Exec(ctx, query, objectID)
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

func updateLastAccessedAt(ctx context.Context, q Querier, objectID string, accessedAt int64) error {
	query := `UPDATE objects SET last_accessed_at = $1 WHERE id = $2`
	_, err := q.Exec(ctx, query, accessedAt, objectID)
	if err != nil {
		return fmt.Errorf("update last accessed at: %w", err)
	}
	return nil
}

func getColdIntelligentTieringObjects(ctx context.Context, q Querier, threshold int64, minSize int64, limit int) ([]*types.ObjectRef, error) {
	query := `
		SELECT ` + ObjectColumns + `
		FROM objects
		WHERE storage_class = 'INTELLIGENT_TIERING'
		  AND ` + q.Dialect().BoolColumn("is_latest", true) + `
		  AND deleted_at = 0
		  AND size >= $1
		  AND ((last_accessed_at > 0 AND last_accessed_at < $2) OR (last_accessed_at = 0 AND created_at < $3))
		LIMIT $4
	`
	rows, err := q.Query(ctx, query, minSize, threshold, threshold, limit)
	if err != nil {
		return nil, fmt.Errorf("query cold intelligent tiering objects: %w", err)
	}
	defer rows.Close()

	objects, err := ScanObjects(rows, q.Dialect())
	if err != nil {
		return nil, fmt.Errorf("scan objects: %w", err)
	}
	return objects, nil
}

func getExpiredRestores(ctx context.Context, q Querier, now int64, limit int) ([]*types.ObjectRef, error) {
	query := `
		SELECT ` + ObjectColumns + `
		FROM objects
		WHERE restore_status = 'completed' AND restore_expiry_date > 0 AND restore_expiry_date < $1
		ORDER BY restore_expiry_date ASC
		LIMIT $2
	`
	rows, err := q.Query(ctx, query, now, limit)
	if err != nil {
		return nil, fmt.Errorf("get expired restores: %w", err)
	}
	defer rows.Close()

	return ScanObjects(rows, q.Dialect())
}
