// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/xml"
	"io"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// ============================================================================
// Public Access Block Configuration
// Controls public access settings for bucket and objects
// ============================================================================

// GetPublicAccessBlockHandler returns the public access block configuration.
// GET /{bucket}?publicAccessBlock
//
// Returns NoSuchPublicAccessBlockConfiguration if not configured.
func (s *MetadataServer) GetPublicAccessBlockHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket

	config, err := s.db.GetPublicAccessBlock(d.Ctx, bucket)
	if err != nil {
		if err == db.ErrPublicAccessBlockNotFound {
			writeXMLErrorResponse(w, d, s3err.ErrNoSuchPublicAccessBlockConfiguration)
			return
		}
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(config)
}

// PutPublicAccessBlockHandler sets the public access block configuration.
// PUT /{bucket}?publicAccessBlock
func (s *MetadataServer) PutPublicAccessBlockHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket

	// Parse request body
	body, err := io.ReadAll(io.LimitReader(d.Req.Body, 64*1024)) // 64KB limit
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	var config s3types.PublicAccessBlockConfig
	if err := xml.Unmarshal(body, &config); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Store the configuration
	if err := s.db.SetPublicAccessBlock(d.Ctx, bucket, &config); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
}

// DeletePublicAccessBlockHandler removes the public access block configuration.
// DELETE /{bucket}?publicAccessBlock
func (s *MetadataServer) DeletePublicAccessBlockHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket

	err := s.db.DeletePublicAccessBlock(d.Ctx, bucket)
	if err != nil && err != db.ErrPublicAccessBlockNotFound {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Success - return 204 No Content
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)
}

// ============================================================================
// Bucket Ownership Controls
// Controls ACL ownership behavior (BucketOwnerPreferred, ObjectWriter, BucketOwnerEnforced)
// ============================================================================

// GetBucketOwnershipControlsHandler returns bucket ownership controls.
// GET /{bucket}?ownershipControls
//
// Returns OwnershipControlsNotFoundError if not configured.
func (s *MetadataServer) GetBucketOwnershipControlsHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket

	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		writeXMLErrorResponse(w, d, s3err.ErrOwnershipControlsNotFoundError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(bucketInfo.OwnershipControls)
}

// PutBucketOwnershipControlsHandler sets bucket ownership controls.
// PUT /{bucket}?ownershipControls
func (s *MetadataServer) PutBucketOwnershipControlsHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket

	// Parse request body
	body, err := io.ReadAll(io.LimitReader(d.Req.Body, 64*1024)) // 64KB limit
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	var controls s3types.OwnershipControls
	if err := xml.Unmarshal(body, &controls); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Validate rules
	if len(controls.Rules) == 0 || len(controls.Rules) > 1 {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Validate ObjectOwnership value
	objectOwnership := controls.Rules[0].ObjectOwnership
	switch objectOwnership {
	case s3types.ObjectOwnershipBucketOwnerPreferred,
		s3types.ObjectOwnershipObjectWriter,
		s3types.ObjectOwnershipBucketOwnerEnforced:
		// Valid values
	default:
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Store the configuration
	if err := s.db.SetOwnershipControls(d.Ctx, bucket, &controls); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Update bucket cache
	if bucketInfo, exists := s.bucketStore.GetBucket(bucket); exists {
		bucketInfo.OwnershipControls = &controls
		s.bucketStore.SetBucket(bucket, bucketInfo)
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
}

// DeleteBucketOwnershipControlsHandler removes bucket ownership controls.
// DELETE /{bucket}?ownershipControls
func (s *MetadataServer) DeleteBucketOwnershipControlsHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket

	err := s.db.DeleteOwnershipControls(d.Ctx, bucket)
	if err != nil && err != db.ErrOwnershipControlsNotFound {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Update bucket cache
	if bucketInfo, exists := s.bucketStore.GetBucket(bucket); exists {
		bucketInfo.OwnershipControls = nil
		s.bucketStore.SetBucket(bucket, bucketInfo)
	}

	// Success - return 204 No Content
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)
}
