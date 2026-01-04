// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// ============================================================================
// Transaction Multipart Operations
// ============================================================================

// multipartColumns is the standard column list for multipart upload queries (without SSE).
const multipartColumns = `id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata`

// CreateMultipartUpload creates a new multipart upload.
func (t *TxStore) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	metadataJSON, err := json.Marshal(upload.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	_, err = t.Exec(ctx, `
		INSERT INTO multipart_uploads (id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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

// GetMultipartUpload retrieves a multipart upload by its ID.
func (t *TxStore) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	row := t.QueryRow(ctx, `
		SELECT `+multipartColumns+`
		FROM multipart_uploads
		WHERE upload_id = $1 AND bucket = $2 AND object_key = $3
	`, uploadID, bucket, key)

	return ScanMultipartUploadBasic(row, t.dialect)
}

// DeleteMultipartUpload deletes a multipart upload.
func (t *TxStore) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	_, err := t.Exec(ctx, `
		DELETE FROM multipart_uploads WHERE upload_id = $1 AND bucket = $2 AND object_key = $3
	`, uploadID, bucket, key)
	if err != nil {
		return fmt.Errorf("delete multipart upload: %w", err)
	}
	return nil
}

// ListMultipartUploads lists multipart uploads for a bucket.
func (t *TxStore) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	query := `
		SELECT ` + multipartColumns + `
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

	rows, err := t.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []*types.MultipartUpload
	for rows.Next() {
		upload, err := ScanMultipartUploadBasic(rows, t.dialect)
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

// PutPart creates or updates a multipart part.
func (t *TxStore) PutPart(ctx context.Context, part *types.MultipartPart) error {
	chunkRefsJSON, err := json.Marshal(part.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO multipart_parts (id, upload_id, part_number, size, etag, last_modified, chunk_refs)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		%s
	`, t.dialect.UpsertSuffix("upload_id, part_number", []string{"size", "etag", "last_modified", "chunk_refs"}))

	_, err = t.Exec(ctx, query,
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

// GetPart retrieves a multipart part.
func (t *TxStore) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	row := t.QueryRow(ctx, `
		SELECT id, upload_id, part_number, size, etag, last_modified, chunk_refs
		FROM multipart_parts
		WHERE upload_id = $1 AND part_number = $2
	`, uploadID, partNumber)

	return ScanPart(row, t.dialect)
}

// ListParts lists parts for a multipart upload.
func (t *TxStore) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
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

	rows, err := t.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list parts: %w", err)
	}
	defer rows.Close()

	var parts []*types.MultipartPart
	for rows.Next() {
		part, err := ScanPart(rows, t.dialect)
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

// DeleteParts deletes all parts for a multipart upload.
func (t *TxStore) DeleteParts(ctx context.Context, uploadID string) error {
	_, err := t.Exec(ctx, `DELETE FROM multipart_parts WHERE upload_id = $1`, uploadID)
	if err != nil {
		return fmt.Errorf("delete parts: %w", err)
	}
	return nil
}
