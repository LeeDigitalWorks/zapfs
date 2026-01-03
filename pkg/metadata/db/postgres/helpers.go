// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/google/uuid"
)

// scanner is implemented by both *sql.Row and *sql.Rows
type scanner interface {
	Scan(dest ...any) error
}

func scanObject(s scanner) (*types.ObjectRef, error) {
	var obj types.ObjectRef
	var idStr string
	var profileID sql.NullString
	var storageClassDB sql.NullString
	var transitionedRef sql.NullString
	var chunkRefsJSON, ecGroupIDsJSON []byte
	var sseAlgorithm, sseCustomerKeyMD5, sseKMSKeyID, sseKMSContext sql.NullString

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
		&storageClassDB,
		&obj.TransitionedAt,
		&transitionedRef,
		&chunkRefsJSON,
		&ecGroupIDsJSON,
		&obj.IsLatest,
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
	if profileID.Valid {
		obj.ProfileID = profileID.String
	}
	if storageClassDB.Valid {
		obj.StorageClass = storageClassDB.String
	}
	if transitionedRef.Valid {
		obj.TransitionedRef = transitionedRef.String
	}

	if len(chunkRefsJSON) > 0 && string(chunkRefsJSON) != "null" {
		if err := json.Unmarshal(chunkRefsJSON, &obj.ChunkRefs); err != nil {
			return nil, fmt.Errorf("unmarshal chunk refs: %w", err)
		}
	}

	if len(ecGroupIDsJSON) > 0 && string(ecGroupIDsJSON) != "null" {
		if err := json.Unmarshal(ecGroupIDsJSON, &obj.ECGroupIDs); err != nil {
			return nil, fmt.Errorf("unmarshal ec group ids: %w", err)
		}
	}

	if sseAlgorithm.Valid {
		obj.SSEAlgorithm = sseAlgorithm.String
	}
	if sseCustomerKeyMD5.Valid {
		obj.SSECustomerKeyMD5 = sseCustomerKeyMD5.String
	}
	if sseKMSKeyID.Valid {
		obj.SSEKMSKeyID = sseKMSKeyID.String
	}
	if sseKMSContext.Valid {
		obj.SSEKMSContext = sseKMSContext.String
	}

	return &obj, nil
}

func scanMultipartUpload(s scanner) (*types.MultipartUpload, error) {
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

func scanPart(s scanner) (*types.MultipartPart, error) {
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
