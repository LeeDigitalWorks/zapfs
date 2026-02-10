// Copyright 2025 ZapFS Authors. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the LICENSE file.

package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// GetBucketReplicationHandler returns the replication configuration for a bucket.
// GET /{bucket}?replication
//
// Requires FeatureMultiRegion license.
func (s *MetadataServer) GetBucketReplicationHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckMultiRegion() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	bucket := d.S3Info.Bucket

	// Verify bucket exists
	if _, exists := s.bucketStore.GetBucket(bucket); !exists {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchBucket)
		return
	}

	// Get replication configuration from database
	config, err := s.db.GetReplicationConfiguration(d.Req.Context(), bucket)
	if err != nil {
		if errors.Is(err, db.ErrReplicationNotFound) {
			writeXMLErrorResponse(w, d, s3err.ErrReplicationConfigurationNotFoundError)
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to get replication config")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Return replication configuration
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(config)
}

// PutBucketReplicationHandler sets the replication configuration for a bucket.
// PUT /{bucket}?replication
//
// Requires FeatureMultiRegion license.
func (s *MetadataServer) PutBucketReplicationHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckMultiRegion() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	bucket := d.S3Info.Bucket

	// Parse request body
	body, err := io.ReadAll(io.LimitReader(d.Req.Body, maxXMLBodySize+1))
	if err != nil {
		logger.Error().Err(err).Msg("failed to read replication config body")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}
	if int64(len(body)) > maxXMLBodySize {
		writeXMLErrorResponse(w, d, s3err.ErrEntityTooLarge)
		return
	}

	var config s3types.ReplicationConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		logger.Error().Err(err).Msg("failed to parse replication config XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Validate configuration
	if err := validateReplicationConfig(&config); err != nil {
		logger.Error().Err(err).Msg("invalid replication configuration")
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Get bucket info from local cache
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchBucket)
		return
	}

	// Versioning is required for replication
	if bucketInfo.Versioning != s3types.VersioningEnabled {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidBucketState)
		return
	}

	// Persist to database
	if err := s.db.SetReplicationConfiguration(d.Req.Context(), bucket, &config); err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to save replication config")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Update bucket cache with replication config
	bucketInfo.ReplicationConfig = &config
	s.bucketStore.SetBucket(bucket, bucketInfo)

	logger.Info().
		Str("bucket", bucket).
		Int("rules", len(config.Rules)).
		Msg("bucket replication configuration updated")

	w.WriteHeader(http.StatusOK)
}

// DeleteBucketReplicationHandler removes the replication configuration from a bucket.
// DELETE /{bucket}?replication
//
// Requires FeatureMultiRegion license.
func (s *MetadataServer) DeleteBucketReplicationHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckMultiRegion() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	bucket := d.S3Info.Bucket

	// Get bucket info from local cache
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchBucket)
		return
	}

	// Delete from database (ignore not found - idempotent delete)
	if err := s.db.DeleteReplicationConfiguration(d.Req.Context(), bucket); err != nil {
		if !errors.Is(err, db.ErrReplicationNotFound) {
			logger.Error().Err(err).Str("bucket", bucket).Msg("failed to delete replication config")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
			return
		}
	}

	// Update bucket cache
	bucketInfo.ReplicationConfig = nil
	s.bucketStore.SetBucket(bucket, bucketInfo)

	logger.Info().Str("bucket", bucket).Msg("bucket replication configuration deleted")

	w.WriteHeader(http.StatusNoContent)
}
