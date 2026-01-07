// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/google/uuid"
)

// ============================================================================
// Object Scanning
// ============================================================================

// ScanObject scans a single object row using the appropriate dialect.
// The dialect determines how boolean fields (like is_latest) are scanned.
func ScanObject(s scanner, dialect Dialect) (*types.ObjectRef, error) {
	var obj types.ObjectRef
	var idStr string
	var contentTypeDB sql.NullString
	var profileID sql.NullString
	var storageClassDB sql.NullString
	var transitionedRef sql.NullString
	var restoreStatus, restoreTier sql.NullString
	var chunkRefsJSON, ecGroupIDsJSON, metadataJSON []byte
	var sseAlgorithm, sseCustomerKeyMD5, sseKMSKeyID, sseKMSContext sql.NullString

	// Use dialect-specific boolean scanner
	isLatestScanner := dialect.ScanBool()

	err := s.Scan(
		&idStr,
		&obj.Bucket,
		&obj.Key,
		&obj.Size,
		&obj.Version,
		&obj.ETag,
		&contentTypeDB,
		&obj.CreatedAt,
		&obj.DeletedAt,
		&obj.TTL,
		&profileID,
		&storageClassDB,
		&obj.TransitionedAt,
		&transitionedRef,
		&restoreStatus,
		&obj.RestoreExpiryDate,
		&restoreTier,
		&obj.RestoreRequestedAt,
		&obj.LastAccessedAt,
		&chunkRefsJSON,
		&ecGroupIDsJSON,
		isLatestScanner.Dest(),
		&sseAlgorithm,
		&sseCustomerKeyMD5,
		&sseKMSKeyID,
		&sseKMSContext,
		&metadataJSON,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrObjectNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan object: %w", err)
	}

	obj.ID, _ = uuid.Parse(idStr)
	obj.IsLatest = isLatestScanner.Value()

	// Content type with default
	obj.ContentType = contentTypeDB.String
	if obj.ContentType == "" {
		obj.ContentType = "application/octet-stream"
	}

	// Optional fields
	obj.ProfileID = profileID.String
	obj.StorageClass = storageClassDB.String
	if obj.StorageClass == "" {
		obj.StorageClass = "STANDARD"
	}
	obj.TransitionedRef = transitionedRef.String
	obj.RestoreStatus = restoreStatus.String
	obj.RestoreTier = restoreTier.String

	// Unmarshal JSON fields
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

	if len(metadataJSON) > 0 && string(metadataJSON) != "null" {
		if err := json.Unmarshal(metadataJSON, &obj.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	// SSE fields
	obj.SSEAlgorithm = sseAlgorithm.String
	obj.SSECustomerKeyMD5 = sseCustomerKeyMD5.String
	obj.SSEKMSKeyID = sseKMSKeyID.String
	obj.SSEKMSContext = sseKMSContext.String

	return &obj, nil
}

// ScanObjects scans multiple object rows.
func ScanObjects(rows *sql.Rows, dialect Dialect) ([]*types.ObjectRef, error) {
	var objects []*types.ObjectRef
	for rows.Next() {
		obj, err := ScanObject(rows, dialect)
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}

// ============================================================================
// Multipart Upload Scanning
// ============================================================================

// ScanMultipartUpload scans a multipart upload row with SSE fields.
func ScanMultipartUpload(s scanner, _ Dialect) (*types.MultipartUpload, error) {
	var upload types.MultipartUpload
	var idStr string
	var contentType, storageClass sql.NullString
	var metadataJSON []byte
	var sseAlgorithm, sseKMSKeyID, sseKMSContext, sseDEKCiphertext string
	var aclJSON string

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
		&aclJSON,
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
	upload.ACLJSON = aclJSON

	if len(metadataJSON) > 0 && string(metadataJSON) != "null" {
		if err := json.Unmarshal(metadataJSON, &upload.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return &upload, nil
}

// ScanMultipartUploadBasic scans a multipart upload row without SSE fields.
// Used when querying uploads without encryption data.
func ScanMultipartUploadBasic(s scanner, _ Dialect) (*types.MultipartUpload, error) {
	var upload types.MultipartUpload
	var idStr string
	var contentType, storageClass sql.NullString
	var metadataJSON []byte
	var aclJSON string

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
		&aclJSON,
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
	upload.ACLJSON = aclJSON

	if len(metadataJSON) > 0 && string(metadataJSON) != "null" {
		if err := json.Unmarshal(metadataJSON, &upload.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return &upload, nil
}

// ============================================================================
// Part Scanning
// ============================================================================

// ScanPart scans a multipart part row.
func ScanPart(s scanner, _ Dialect) (*types.MultipartPart, error) {
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

// ============================================================================
// Bucket Scanning
// ============================================================================

// BucketColumns is the standard column list for bucket queries.
const BucketColumns = `id, name, owner_id, region, created_at, default_profile_id, versioning`

// ScanBucket scans a single bucket row.
func ScanBucket(s scanner, _ Dialect) (*types.BucketInfo, error) {
	var bucket types.BucketInfo
	var idStr string
	var region, profileID, versioning sql.NullString

	err := s.Scan(
		&idStr,
		&bucket.Name,
		&bucket.OwnerID,
		&region,
		&bucket.CreatedAt,
		&profileID,
		&versioning,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrBucketNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan bucket: %w", err)
	}

	bucket.ID, _ = uuid.Parse(idStr)
	bucket.Region = region.String
	bucket.DefaultProfileID = profileID.String
	bucket.Versioning = versioning.String

	return &bucket, nil
}

// ScanBuckets scans multiple bucket rows.
func ScanBuckets(rows *sql.Rows, dialect Dialect) ([]*types.BucketInfo, error) {
	var buckets []*types.BucketInfo
	for rows.Next() {
		bucket, err := ScanBucket(rows, dialect)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, bucket)
	}
	return buckets, rows.Err()
}
