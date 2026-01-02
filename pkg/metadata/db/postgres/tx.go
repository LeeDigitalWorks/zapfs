// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

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

// postgresTx wraps a SQL transaction and implements db.TxStore
type postgresTx struct {
	tx *sql.Tx
}

// GetObjectByID retrieves an object by its UUID
func (t *postgresTx) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE id = $1
	`, id.String())

	return scanObject(row)
}

// ============================================================================
// Transaction Object Operations
// ============================================================================

func (t *postgresTx) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	chunkRefsJSON, err := json.Marshal(obj.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}
	ecGroupIDsJSON, err := json.Marshal(obj.ECGroupIDs)
	if err != nil {
		return fmt.Errorf("marshal ec group ids: %w", err)
	}

	// Mark old versions as not latest
	_, err = t.tx.ExecContext(ctx, `
		UPDATE objects SET is_latest = FALSE WHERE bucket = $1 AND object_key = $2 AND is_latest = TRUE
	`, obj.Bucket, obj.Key)
	if err != nil {
		return fmt.Errorf("mark old versions not latest: %w", err)
	}

	_, err = t.tx.ExecContext(ctx, `
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

func (t *postgresTx) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = $1 AND object_key = $2 AND deleted_at = 0 AND is_latest = TRUE
	`, bucket, key)

	return scanObject(row)
}

func (t *postgresTx) DeleteObject(ctx context.Context, bucket, key string) error {
	result, err := t.tx.ExecContext(ctx, `
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

func (t *postgresTx) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	_, err := t.tx.ExecContext(ctx, `
		UPDATE objects SET deleted_at = $1 WHERE bucket = $2 AND object_key = $3 AND is_latest = TRUE
	`, deletedAt, bucket, key)
	if err != nil {
		return fmt.Errorf("mark object deleted: %w", err)
	}
	return nil
}

func (t *postgresTx) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
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

func (t *postgresTx) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
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

	rows, err := t.tx.QueryContext(ctx, query, args...)
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

func (t *postgresTx) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
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

	rows, err := t.tx.QueryContext(ctx, query, args...)
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

// ============================================================================
// Transaction Bucket Operations
// ============================================================================

func (t *postgresTx) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	_, err := t.tx.ExecContext(ctx, `
		INSERT INTO buckets (id, name, owner_id, region, created_at, versioning)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, bucket.ID.String(), bucket.Name, bucket.OwnerID, bucket.Region, bucket.CreatedAt, bucket.Versioning)
	if err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	return nil
}

func (t *postgresTx) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	row := t.tx.QueryRowContext(ctx, `
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

func (t *postgresTx) DeleteBucket(ctx context.Context, name string) error {
	result, err := t.tx.ExecContext(ctx, `DELETE FROM buckets WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrBucketNotFound
	}
	return nil
}

func (t *postgresTx) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
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

	rows, err := t.tx.QueryContext(ctx, query, args...)
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

func (t *postgresTx) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	_, err := t.tx.ExecContext(ctx, `UPDATE buckets SET versioning = $1 WHERE name = $2`, versioning, bucket)
	if err != nil {
		return fmt.Errorf("update bucket versioning: %w", err)
	}
	return nil
}

// ============================================================================
// Transaction Multipart Operations
// ============================================================================

func (t *postgresTx) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	metadataJSON, err := json.Marshal(upload.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	_, err = t.tx.ExecContext(ctx, `
		INSERT INTO multipart_uploads (id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata, sse_algorithm, sse_kms_key_id, sse_kms_context, sse_dek_ciphertext)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
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
		upload.SSEAlgorithm,
		upload.SSEKMSKeyID,
		upload.SSEKMSContext,
		upload.SSEDEKCiphertext,
	)
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	return nil
}

func (t *postgresTx) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class,
		       COALESCE(sse_algorithm, ''), COALESCE(sse_kms_key_id, ''), COALESCE(sse_kms_context, ''), COALESCE(sse_dek_ciphertext, '')
		FROM multipart_uploads
		WHERE upload_id = $1 AND bucket = $2 AND object_key = $3
	`, uploadID, bucket, key)

	var upload types.MultipartUpload
	var id string
	var contentType, storageClass sql.NullString
	var sseAlgorithm, sseKMSKeyID, sseKMSContext, sseDEKCiphertext string

	err := row.Scan(
		&id,
		&upload.UploadID,
		&upload.Bucket,
		&upload.Key,
		&upload.OwnerID,
		&upload.Initiated,
		&contentType,
		&storageClass,
		&sseAlgorithm,
		&sseKMSKeyID,
		&sseKMSContext,
		&sseDEKCiphertext,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrUploadNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get multipart upload: %w", err)
	}

	upload.ID, _ = uuid.Parse(id)
	upload.ContentType = contentType.String
	upload.StorageClass = storageClass.String
	upload.SSEAlgorithm = sseAlgorithm
	upload.SSEKMSKeyID = sseKMSKeyID
	upload.SSEKMSContext = sseKMSContext
	upload.SSEDEKCiphertext = sseDEKCiphertext

	return &upload, nil
}

func (t *postgresTx) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	_, err := t.tx.ExecContext(ctx, `DELETE FROM multipart_uploads WHERE upload_id = $1`, uploadID)
	if err != nil {
		return fmt.Errorf("delete multipart upload: %w", err)
	}
	return nil
}

func (t *postgresTx) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	query := `
		SELECT id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class,
		       COALESCE(sse_algorithm, ''), COALESCE(sse_kms_key_id, ''), COALESCE(sse_kms_context, ''), COALESCE(sse_dek_ciphertext, '')
		FROM multipart_uploads
		WHERE bucket = $1
	`
	args := []any{bucket}
	argIdx := 2

	if prefix != "" {
		query += fmt.Sprintf(" AND object_key LIKE $%d", argIdx)
		args = append(args, prefix+"%")
		argIdx++
	}

	if keyMarker != "" {
		if uploadIDMarker != "" {
			query += fmt.Sprintf(" AND (object_key > $%d OR (object_key = $%d AND upload_id > $%d))", argIdx, argIdx+1, argIdx+2)
			args = append(args, keyMarker, keyMarker, uploadIDMarker)
			argIdx += 3
		} else {
			query += fmt.Sprintf(" AND object_key > $%d", argIdx)
			args = append(args, keyMarker)
			argIdx++
		}
	}

	query += " ORDER BY object_key, upload_id"

	if maxUploads > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, maxUploads+1)
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []*types.MultipartUpload
	for rows.Next() {
		var upload types.MultipartUpload
		var idStr string
		var contentType, storageClass sql.NullString
		var sseAlgorithm, sseKMSKeyID, sseKMSContext, sseDEKCiphertext string

		err := rows.Scan(
			&idStr,
			&upload.UploadID,
			&upload.Bucket,
			&upload.Key,
			&upload.OwnerID,
			&upload.Initiated,
			&contentType,
			&storageClass,
			&sseAlgorithm,
			&sseKMSKeyID,
			&sseKMSContext,
			&sseDEKCiphertext,
		)
		if err != nil {
			return nil, false, fmt.Errorf("scan multipart upload: %w", err)
		}

		upload.ID, _ = uuid.Parse(idStr)
		upload.ContentType = contentType.String
		upload.StorageClass = storageClass.String
		upload.SSEAlgorithm = sseAlgorithm
		upload.SSEKMSKeyID = sseKMSKeyID
		upload.SSEKMSContext = sseKMSContext
		upload.SSEDEKCiphertext = sseDEKCiphertext

		uploads = append(uploads, &upload)
	}

	isTruncated := false
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
		isTruncated = true
	}

	return uploads, isTruncated, nil
}

func (t *postgresTx) PutPart(ctx context.Context, part *types.MultipartPart) error {
	chunkRefsJSON, err := json.Marshal(part.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	_, err = t.tx.ExecContext(ctx, `
		INSERT INTO multipart_parts (id, upload_id, part_number, size, etag, last_modified, chunk_refs)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (upload_id, part_number) DO UPDATE SET
			size = EXCLUDED.size,
			etag = EXCLUDED.etag,
			last_modified = EXCLUDED.last_modified,
			chunk_refs = EXCLUDED.chunk_refs
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

func (t *postgresTx) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, upload_id, part_number, size, etag, last_modified, chunk_refs
		FROM multipart_parts
		WHERE upload_id = $1 AND part_number = $2
	`, uploadID, partNumber)

	var part types.MultipartPart
	var id string
	var chunkRefsJSON sql.NullString

	err := row.Scan(&id, &part.UploadID, &part.PartNumber, &part.Size, &part.ETag, &part.LastModified, &chunkRefsJSON)
	if err == sql.ErrNoRows {
		return nil, db.ErrPartNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get part: %w", err)
	}

	part.ID, _ = uuid.Parse(id)
	if chunkRefsJSON.Valid && chunkRefsJSON.String != "" {
		if err := json.Unmarshal([]byte(chunkRefsJSON.String), &part.ChunkRefs); err != nil {
			return nil, fmt.Errorf("unmarshal chunk refs: %w", err)
		}
	}

	return &part, nil
}

func (t *postgresTx) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
	query := `
		SELECT id, upload_id, part_number, size, etag, last_modified, chunk_refs
		FROM multipart_parts
		WHERE upload_id = $1 AND part_number > $2
		ORDER BY part_number
	`
	args := []any{uploadID, partNumberMarker}

	if maxParts > 0 {
		query += " LIMIT $3"
		args = append(args, maxParts+1)
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list parts: %w", err)
	}
	defer rows.Close()

	var parts []*types.MultipartPart
	for rows.Next() {
		var part types.MultipartPart
		var idStr string
		var chunkRefsJSON sql.NullString

		err := rows.Scan(
			&idStr, &part.UploadID, &part.PartNumber, &part.Size, &part.ETag, &part.LastModified, &chunkRefsJSON,
		)
		if err != nil {
			return nil, false, fmt.Errorf("scan part: %w", err)
		}

		part.ID, _ = uuid.Parse(idStr)
		if chunkRefsJSON.Valid && chunkRefsJSON.String != "" {
			if err := json.Unmarshal([]byte(chunkRefsJSON.String), &part.ChunkRefs); err != nil {
				return nil, false, fmt.Errorf("unmarshal chunk refs: %w", err)
			}
		}

		parts = append(parts, &part)
	}

	isTruncated := false
	if maxParts > 0 && len(parts) > maxParts {
		parts = parts[:maxParts]
		isTruncated = true
	}

	return parts, isTruncated, nil
}

func (t *postgresTx) DeleteParts(ctx context.Context, uploadID string) error {
	_, err := t.tx.ExecContext(ctx, `DELETE FROM multipart_parts WHERE upload_id = $1`, uploadID)
	if err != nil {
		return fmt.Errorf("delete parts: %w", err)
	}
	return nil
}
