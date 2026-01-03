// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/google/uuid"
)

// ============================================================================
// Helpers
// ============================================================================

// storageClass returns the storage class, defaulting to STANDARD if empty
func storageClass(sc string) string {
	if sc == "" {
		return "STANDARD"
	}
	return sc
}

// scanner is an interface for sql.Row and sql.Rows
type scanner interface {
	Scan(dest ...any) error
}

func scanObject(s scanner) (*types.ObjectRef, error) {
	var obj types.ObjectRef
	var idStr string
	var profileID, storageClass sql.NullString
	var transitionedRef sql.NullString
	var chunkRefsJSON, ecGroupIDsJSON []byte
	var isLatest int
	var sseAlgorithm, sseCustomerKeyMD5, sseKMSKeyID sql.NullString
	var sseKMSContext sql.NullString

	err := s.Scan(
		&idStr,
		&obj.Bucket,
		&obj.Key,
		&obj.Size,
		&obj.Version,
		&obj.ETag,
		&obj.CreatedAt,
		&obj.DeletedAt,
		&obj.TTL,
		&profileID,
		&storageClass,
		&obj.TransitionedAt,
		&transitionedRef,
		&chunkRefsJSON,
		&ecGroupIDsJSON,
		&isLatest,
		&sseAlgorithm,
		&sseCustomerKeyMD5,
		&sseKMSKeyID,
		&sseKMSContext,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrObjectNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan object: %w", err)
	}

	obj.ID, _ = uuid.Parse(idStr)
	obj.ProfileID = profileID.String
	obj.StorageClass = storageClass.String
	if obj.StorageClass == "" {
		obj.StorageClass = "STANDARD"
	}
	obj.TransitionedRef = transitionedRef.String
	obj.IsLatest = isLatest == 1

	if len(chunkRefsJSON) > 0 && string(chunkRefsJSON) != "null" {
		if err := json.Unmarshal(chunkRefsJSON, &obj.ChunkRefs); err != nil {
			return nil, fmt.Errorf("unmarshal chunk_refs: %w", err)
		}
	}

	if len(ecGroupIDsJSON) > 0 && string(ecGroupIDsJSON) != "null" {
		if err := json.Unmarshal(ecGroupIDsJSON, &obj.ECGroupIDs); err != nil {
			return nil, fmt.Errorf("unmarshal ec_group_ids: %w", err)
		}
	}

	// Set encryption fields
	obj.SSEAlgorithm = sseAlgorithm.String
	obj.SSECustomerKeyMD5 = sseCustomerKeyMD5.String
	obj.SSEKMSKeyID = sseKMSKeyID.String
	obj.SSEKMSContext = sseKMSContext.String

	return &obj, nil
}

func scanObjects(rows *sql.Rows) ([]*types.ObjectRef, error) {
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

// multipartScanner is an interface for scanning multipart upload rows
type multipartScanner interface {
	Scan(dest ...any) error
}

func scanMultipartUpload(s multipartScanner) (*types.MultipartUpload, error) {
	var upload types.MultipartUpload
	var idStr string
	var contentType, storageClass sql.NullString
	var metadataJSON []byte

	err := s.Scan(
		&idStr,
		&upload.UploadID,
		&upload.Bucket,
		&upload.Key,
		&upload.OwnerID,
		&upload.Initiated,
		&contentType,
		&storageClass,
		&metadataJSON,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrUploadNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan multipart upload: %w", err)
	}

	upload.ID, _ = uuid.Parse(idStr)
	upload.ContentType = contentType.String
	upload.StorageClass = storageClass.String

	if len(metadataJSON) > 0 && string(metadataJSON) != "null" {
		if err := json.Unmarshal(metadataJSON, &upload.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return &upload, nil
}

func scanPart(s multipartScanner) (*types.MultipartPart, error) {
	var part types.MultipartPart
	var idStr string
	var chunkRefsJSON []byte

	err := s.Scan(
		&idStr,
		&part.UploadID,
		&part.PartNumber,
		&part.Size,
		&part.ETag,
		&part.LastModified,
		&chunkRefsJSON,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrPartNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan part: %w", err)
	}

	part.ID, _ = uuid.Parse(idStr)

	if len(chunkRefsJSON) > 0 && string(chunkRefsJSON) != "null" {
		if err := json.Unmarshal(chunkRefsJSON, &part.ChunkRefs); err != nil {
			return nil, fmt.Errorf("unmarshal chunk refs: %w", err)
		}
	}

	return &part, nil
}

// scanMultipartUploadFull scans a multipart upload with SSE fields
func scanMultipartUploadFull(s multipartScanner) (*types.MultipartUpload, error) {
	var upload types.MultipartUpload
	var idStr string
	var contentType, storageClass sql.NullString
	var metadataJSON []byte
	var sseAlgorithm, sseKMSKeyID, sseKMSContext, sseDEKCiphertext string

	err := s.Scan(
		&idStr,
		&upload.UploadID,
		&upload.Bucket,
		&upload.Key,
		&upload.OwnerID,
		&upload.Initiated,
		&contentType,
		&storageClass,
		&metadataJSON,
		&sseAlgorithm,
		&sseKMSKeyID,
		&sseKMSContext,
		&sseDEKCiphertext,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrUploadNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan multipart upload: %w", err)
	}

	upload.ID, _ = uuid.Parse(idStr)
	upload.ContentType = contentType.String
	upload.StorageClass = storageClass.String
	upload.SSEAlgorithm = sseAlgorithm
	upload.SSEKMSKeyID = sseKMSKeyID
	upload.SSEKMSContext = sseKMSContext
	upload.SSEDEKCiphertext = sseDEKCiphertext

	if len(metadataJSON) > 0 && string(metadataJSON) != "null" {
		if err := json.Unmarshal(metadataJSON, &upload.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return &upload, nil
}
