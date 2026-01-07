// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// MultipartUploadColumns is the standard column list for multipart upload queries (with SSE).
const MultipartUploadColumns = `id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata,
       COALESCE(sse_algorithm, ''), COALESCE(sse_kms_key_id, ''), COALESCE(sse_kms_context, ''), COALESCE(sse_dek_ciphertext, ''), COALESCE(acl_json, '')`

// MultipartUploadColumnsBasic is the column list without SSE fields.
const MultipartUploadColumnsBasic = `id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata, COALESCE(acl_json, '')`

// PartColumns is the standard column list for part queries.
const PartColumns = `id, upload_id, part_number, size, etag, last_modified, chunk_refs`

// ============================================================================
// Multipart Upload Operations - Store
// ============================================================================

func (s *Store) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	return createMultipartUploadWithSSE(ctx, s, upload)
}

func (s *Store) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	return getMultipartUploadWithSSE(ctx, s, bucket, key, uploadID)
}

func (s *Store) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	return deleteMultipartUpload(ctx, s, bucket, key, uploadID)
}

func (s *Store) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	return listMultipartUploadsWithSSE(ctx, s, bucket, prefix, keyMarker, uploadIDMarker, maxUploads)
}

func (s *Store) PutPart(ctx context.Context, part *types.MultipartPart) error {
	return putPart(ctx, s, part)
}

func (s *Store) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	return getPart(ctx, s, uploadID, partNumber)
}

func (s *Store) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
	return listParts(ctx, s, uploadID, partNumberMarker, maxParts)
}

func (s *Store) DeleteParts(ctx context.Context, uploadID string) error {
	return deleteParts(ctx, s, uploadID)
}

// ============================================================================
// Multipart Upload Operations - TxStore
// ============================================================================

func (t *TxStore) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	return createMultipartUploadBasic(ctx, t, upload)
}

func (t *TxStore) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	return getMultipartUploadBasic(ctx, t, bucket, key, uploadID)
}

func (t *TxStore) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	return deleteMultipartUpload(ctx, t, bucket, key, uploadID)
}

func (t *TxStore) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	return listMultipartUploadsBasic(ctx, t, bucket, prefix, keyMarker, uploadIDMarker, maxUploads)
}

func (t *TxStore) PutPart(ctx context.Context, part *types.MultipartPart) error {
	return putPart(ctx, t, part)
}

func (t *TxStore) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	return getPart(ctx, t, uploadID, partNumber)
}

func (t *TxStore) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
	return listParts(ctx, t, uploadID, partNumberMarker, maxParts)
}

func (t *TxStore) DeleteParts(ctx context.Context, uploadID string) error {
	return deleteParts(ctx, t, uploadID)
}

// ============================================================================
// Shared Implementations - Multipart Uploads
// ============================================================================

// createMultipartUploadWithSSE creates a multipart upload with SSE fields.
func createMultipartUploadWithSSE(ctx context.Context, q Querier, upload *types.MultipartUpload) error {
	metadataJSON, err := json.Marshal(upload.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	query := `
		INSERT INTO multipart_uploads (id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata, sse_algorithm, sse_kms_key_id, sse_kms_context, sse_dek_ciphertext, acl_json)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`

	_, err = q.Exec(ctx, query,
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
		upload.ACLJSON,
	)
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	return nil
}

// createMultipartUploadBasic creates a multipart upload without SSE fields.
func createMultipartUploadBasic(ctx context.Context, q Querier, upload *types.MultipartUpload) error {
	metadataJSON, err := json.Marshal(upload.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	_, err = q.Exec(ctx, `
		INSERT INTO multipart_uploads (id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata, acl_json)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
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
		upload.ACLJSON,
	)
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	return nil
}

func getMultipartUploadWithSSE(ctx context.Context, q Querier, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	query := `
		SELECT ` + MultipartUploadColumns + `
		FROM multipart_uploads
		WHERE upload_id = $1 AND bucket = $2 AND object_key = $3
	`
	row := q.QueryRow(ctx, query, uploadID, bucket, key)
	return ScanMultipartUpload(row, q.Dialect())
}

func getMultipartUploadBasic(ctx context.Context, q Querier, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	row := q.QueryRow(ctx, `
		SELECT `+MultipartUploadColumnsBasic+`
		FROM multipart_uploads
		WHERE upload_id = $1 AND bucket = $2 AND object_key = $3
	`, uploadID, bucket, key)
	return ScanMultipartUploadBasic(row, q.Dialect())
}

func deleteMultipartUpload(ctx context.Context, q Querier, bucket, key, uploadID string) error {
	query := `DELETE FROM multipart_uploads WHERE upload_id = $1 AND bucket = $2 AND object_key = $3`
	_, err := q.Exec(ctx, query, uploadID, bucket, key)
	if err != nil {
		return fmt.Errorf("delete multipart upload: %w", err)
	}
	return nil
}

func listMultipartUploadsWithSSE(ctx context.Context, q Querier, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	query := `
		SELECT ` + MultipartUploadColumns + `
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

	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []*types.MultipartUpload
	for rows.Next() {
		upload, err := ScanMultipartUpload(rows, q.Dialect())
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

func listMultipartUploadsBasic(ctx context.Context, q Querier, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	query := `
		SELECT ` + MultipartUploadColumnsBasic + `
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

	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []*types.MultipartUpload
	for rows.Next() {
		upload, err := ScanMultipartUploadBasic(rows, q.Dialect())
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

// ============================================================================
// Shared Implementations - Parts
// ============================================================================

func putPart(ctx context.Context, q Querier, part *types.MultipartPart) error {
	chunkRefsJSON, err := json.Marshal(part.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO multipart_parts (id, upload_id, part_number, size, etag, last_modified, chunk_refs)
		VALUES ($1, $2, $3, $4, $5, $6, $7)%s
	`, q.Dialect().UpsertSuffix("upload_id, part_number", []string{"size", "etag", "last_modified", "chunk_refs"}))

	_, err = q.Exec(ctx, query,
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

func getPart(ctx context.Context, q Querier, uploadID string, partNumber int) (*types.MultipartPart, error) {
	query := `
		SELECT ` + PartColumns + `
		FROM multipart_parts
		WHERE upload_id = $1 AND part_number = $2
	`
	row := q.QueryRow(ctx, query, uploadID, partNumber)
	return ScanPart(row, q.Dialect())
}

func listParts(ctx context.Context, q Querier, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
	query := `
		SELECT ` + PartColumns + `
		FROM multipart_parts
		WHERE upload_id = $1 AND part_number > $2
		ORDER BY part_number
	`
	args := []any{uploadID, partNumberMarker}

	if maxParts > 0 {
		query += " LIMIT $3"
		args = append(args, maxParts+1)
	}

	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list parts: %w", err)
	}
	defer rows.Close()

	var parts []*types.MultipartPart
	for rows.Next() {
		part, err := ScanPart(rows, q.Dialect())
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

func deleteParts(ctx context.Context, q Querier, uploadID string) error {
	_, err := q.Exec(ctx, `DELETE FROM multipart_parts WHERE upload_id = $1`, uploadID)
	if err != nil {
		return fmt.Errorf("delete parts: %w", err)
	}
	return nil
}
