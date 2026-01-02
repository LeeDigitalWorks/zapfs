// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// ============================================================================
// Multipart Upload Operations
// ============================================================================

func (v *Vitess) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	metadataJSON, err := json.Marshal(upload.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	_, err = v.db.ExecContext(ctx, `
		INSERT INTO multipart_uploads (id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata, sse_algorithm, sse_kms_key_id, sse_kms_context, sse_dek_ciphertext)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

func (v *Vitess) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata,
		       COALESCE(sse_algorithm, ''), COALESCE(sse_kms_key_id, ''), COALESCE(sse_kms_context, ''), COALESCE(sse_dek_ciphertext, '')
		FROM multipart_uploads
		WHERE upload_id = ? AND bucket = ? AND object_key = ?
	`, uploadID, bucket, key)

	return scanMultipartUploadFull(row)
}

func (v *Vitess) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	_, err := v.db.ExecContext(ctx, `
		DELETE FROM multipart_uploads WHERE upload_id = ? AND bucket = ? AND object_key = ?
	`, uploadID, bucket, key)
	if err != nil {
		return fmt.Errorf("delete multipart upload: %w", err)
	}
	return nil
}

func (v *Vitess) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	query := `
		SELECT id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata,
		       COALESCE(sse_algorithm, ''), COALESCE(sse_kms_key_id, ''), COALESCE(sse_kms_context, ''), COALESCE(sse_dek_ciphertext, '')
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

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []*types.MultipartUpload
	for rows.Next() {
		upload, err := scanMultipartUploadFull(rows)
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

func (v *Vitess) PutPart(ctx context.Context, part *types.MultipartPart) error {
	chunkRefsJSON, err := json.Marshal(part.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	_, err = v.db.ExecContext(ctx, `
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

func (v *Vitess) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, upload_id, part_number, size, etag, last_modified, chunk_refs
		FROM multipart_parts
		WHERE upload_id = ? AND part_number = ?
	`, uploadID, partNumber)

	return scanPart(row)
}

func (v *Vitess) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
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

	rows, err := v.db.QueryContext(ctx, query, args...)
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

func (v *Vitess) DeleteParts(ctx context.Context, uploadID string) error {
	_, err := v.db.ExecContext(ctx, `DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID)
	if err != nil {
		return fmt.Errorf("delete parts: %w", err)
	}
	return nil
}
