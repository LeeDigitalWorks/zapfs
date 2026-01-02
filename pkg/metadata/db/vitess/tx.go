// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

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

// vitessTx wraps a sql.Tx to implement db.TxStore
type vitessTx struct {
	tx *sql.Tx
}

// ============================================================================
// Transaction Object Operations
// ============================================================================

func (t *vitessTx) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	chunkRefsJSON, err := json.Marshal(obj.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	ecGroupIDsJSON, err := json.Marshal(obj.ECGroupIDs)
	if err != nil {
		return fmt.Errorf("marshal ec group ids: %w", err)
	}

	// Convert IsLatest bool to int for MySQL
	isLatest := 0
	if obj.IsLatest {
		isLatest = 1
	}

	if obj.IsLatest {
		// Versioning mode: mark old versions as not latest, then insert new
		_, err = t.tx.ExecContext(ctx, `
			UPDATE objects SET is_latest = 0 WHERE bucket = ? AND object_key = ? AND is_latest = 1
		`, obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("mark old versions: %w", err)
		}

		// Insert new version
		_, err = t.tx.ExecContext(ctx, `
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
			string(chunkRefsJSON),
			string(ecGroupIDsJSON),
			isLatest,
			obj.SSEAlgorithm,
			obj.SSECustomerKeyMD5,
			obj.SSEKMSKeyID,
			obj.SSEKMSContext,
		)
	} else {
		// Non-versioning mode: delete old, then insert new (replace behavior)
		_, err = t.tx.ExecContext(ctx, `
			DELETE FROM objects WHERE bucket = ? AND object_key = ?
		`, obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("delete old object: %w", err)
		}

		_, err = t.tx.ExecContext(ctx, `
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
			string(chunkRefsJSON),
			string(ecGroupIDsJSON),
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

func (t *vitessTx) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = ? AND object_key = ? AND is_latest = 1
	`, bucket, key)
	return scanObject(row)
}

func (t *vitessTx) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE id = ?
	`, id.String())
	return scanObject(row)
}

func (t *vitessTx) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := t.tx.ExecContext(ctx, `
		DELETE FROM objects WHERE bucket = ? AND object_key = ?
	`, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (t *vitessTx) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	result, err := t.tx.ExecContext(ctx, `
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

func (t *vitessTx) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
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

func (t *vitessTx) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
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
		WHERE bucket = ?
		  AND object_key LIKE ?
		  AND deleted_at = 0
		  AND is_latest = 1
	`
	args := []any{params.Bucket, params.Prefix + "%"}

	if marker != "" {
		query += " AND object_key > ?"
		args = append(args, marker)
	}

	query += " ORDER BY object_key"

	fetchLimit := params.MaxKeys + 1
	if fetchLimit > 0 {
		query += " LIMIT ?"
		args = append(args, fetchLimit)
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)
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

func (t *vitessTx) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
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

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list deleted objects: %w", err)
	}
	defer rows.Close()

	return scanObjects(rows)
}

// ============================================================================
// Transaction Bucket Operations
// ============================================================================

func (t *vitessTx) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	// Use INSERT IGNORE to handle case where bucket already exists in DB
	_, err := t.tx.ExecContext(ctx, `
		INSERT IGNORE INTO buckets (id, name, owner_id, region, created_at, default_profile_id, versioning)
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

func (t *vitessTx) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, name, owner_id, region, created_at, default_profile_id, versioning
		FROM buckets
		WHERE name = ?
	`, name)

	var bucket types.BucketInfo
	var idStr string
	err := row.Scan(
		&idStr,
		&bucket.Name,
		&bucket.OwnerID,
		&bucket.Region,
		&bucket.CreatedAt,
		&bucket.DefaultProfileID,
		&bucket.Versioning,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrBucketNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan bucket: %w", err)
	}

	bucket.ID, err = uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("parse bucket id: %w", err)
	}

	return &bucket, nil
}

func (t *vitessTx) DeleteBucket(ctx context.Context, name string) error {
	_, err := t.tx.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}
	return nil
}

func (t *vitessTx) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
	// Build query with filters
	query := `
		SELECT id, name, owner_id, region, created_at, default_profile_id, versioning
		FROM buckets
		WHERE owner_id = ?
	`
	args := []any{params.OwnerID}

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

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list buckets v2: %w", err)
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
		return nil, fmt.Errorf("list buckets v2: %w", err)
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

func (t *vitessTx) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	_, err := t.tx.ExecContext(ctx, `
		UPDATE buckets SET versioning = ? WHERE name = ?
	`, versioning, bucket)
	if err != nil {
		return fmt.Errorf("update bucket versioning: %w", err)
	}
	return nil
}

// ============================================================================
// Transaction Multipart Operations
// ============================================================================

func (t *vitessTx) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	metadataJSON, err := json.Marshal(upload.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	_, err = t.tx.ExecContext(ctx, `
		INSERT INTO multipart_uploads (id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		upload.ID.String(),
		upload.UploadID,
		upload.Bucket,
		upload.Key,
		upload.OwnerID,
		upload.Initiated,
		upload.ContentType,
		upload.StorageClass,
		string(metadataJSON),
	)
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	return nil
}

func (t *vitessTx) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata
		FROM multipart_uploads
		WHERE upload_id = ? AND bucket = ? AND object_key = ?
	`, uploadID, bucket, key)

	return scanMultipartUpload(row)
}

func (t *vitessTx) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	_, err := t.tx.ExecContext(ctx, `
		DELETE FROM multipart_uploads WHERE upload_id = ? AND bucket = ? AND object_key = ?
	`, uploadID, bucket, key)
	if err != nil {
		return fmt.Errorf("delete multipart upload: %w", err)
	}
	return nil
}

func (t *vitessTx) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	query := `
		SELECT id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata
		FROM multipart_uploads
		WHERE bucket = ?
	`
	args := []any{bucket}

	if prefix != "" {
		query += " AND object_key LIKE ?"
		args = append(args, prefix+"%")
	}

	if keyMarker != "" {
		if uploadIDMarker != "" {
			query += " AND (object_key > ? OR (object_key = ? AND upload_id > ?))"
			args = append(args, keyMarker, keyMarker, uploadIDMarker)
		} else {
			query += " AND object_key > ?"
			args = append(args, keyMarker)
		}
	}

	query += " ORDER BY object_key, upload_id"

	if maxUploads > 0 {
		query += " LIMIT ?"
		args = append(args, maxUploads+1)
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []*types.MultipartUpload
	for rows.Next() {
		upload, err := scanMultipartUpload(rows)
		if err != nil {
			return nil, false, err
		}
		uploads = append(uploads, upload)
	}

	isTruncated := false
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
		isTruncated = true
	}

	return uploads, isTruncated, nil
}

func (t *vitessTx) PutPart(ctx context.Context, part *types.MultipartPart) error {
	chunkRefsJSON, err := json.Marshal(part.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	_, err = t.tx.ExecContext(ctx, `
		INSERT INTO multipart_parts (id, upload_id, part_number, size, etag, last_modified, chunk_refs)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			size = VALUES(size),
			etag = VALUES(etag),
			last_modified = VALUES(last_modified),
			chunk_refs = VALUES(chunk_refs)
	`,
		part.ID.String(),
		part.UploadID,
		part.PartNumber,
		part.Size,
		part.ETag,
		part.LastModified,
		string(chunkRefsJSON),
	)
	if err != nil {
		return fmt.Errorf("put part: %w", err)
	}
	return nil
}

func (t *vitessTx) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, upload_id, part_number, size, etag, last_modified, chunk_refs
		FROM multipart_parts
		WHERE upload_id = ? AND part_number = ?
	`, uploadID, partNumber)

	return scanPart(row)
}

func (t *vitessTx) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
	query := `
		SELECT id, upload_id, part_number, size, etag, last_modified, chunk_refs
		FROM multipart_parts
		WHERE upload_id = ? AND part_number > ?
		ORDER BY part_number
	`
	args := []any{uploadID, partNumberMarker}

	if maxParts > 0 {
		query += " LIMIT ?"
		args = append(args, maxParts+1)
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list parts: %w", err)
	}
	defer rows.Close()

	var parts []*types.MultipartPart
	for rows.Next() {
		part, err := scanPart(rows)
		if err != nil {
			return nil, false, err
		}
		parts = append(parts, part)
	}

	isTruncated := false
	if maxParts > 0 && len(parts) > maxParts {
		parts = parts[:maxParts]
		isTruncated = true
	}

	return parts, isTruncated, nil
}

func (t *vitessTx) DeleteParts(ctx context.Context, uploadID string) error {
	_, err := t.tx.ExecContext(ctx, `DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID)
	if err != nil {
		return fmt.Errorf("delete parts: %w", err)
	}
	return nil
}
