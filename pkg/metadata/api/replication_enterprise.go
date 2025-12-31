//go:build enterprise

// Copyright 2025 ZapInvest, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package api

import (
	"encoding/xml"
	"io"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// GetBucketReplicationHandler returns the replication configuration for a bucket.
// GET /{bucket}?replication
func (s *MetadataServer) GetBucketReplicationHandler(d *data.Data, w http.ResponseWriter) {
	if !checkReplicationLicense() {
		logger.Warn().Msg("replication requires enterprise license")
		writeXMLErrorResponse(w, d, s3err.ErrAccessDenied)
		return
	}

	bucket := d.S3Info.Bucket

	// Get bucket info from local cache
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchBucket)
		return
	}

	// Check if replication is configured
	if bucketInfo.ReplicationConfig == nil {
		writeXMLErrorResponse(w, d, s3err.ErrReplicationConfigurationNotFoundError)
		return
	}

	// Return replication configuration
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(bucketInfo.ReplicationConfig)
}

// PutBucketReplicationHandler sets the replication configuration for a bucket.
// PUT /{bucket}?replication
func (s *MetadataServer) PutBucketReplicationHandler(d *data.Data, w http.ResponseWriter) {
	if !checkReplicationLicense() {
		logger.Warn().Msg("replication requires enterprise license")
		writeXMLErrorResponse(w, d, s3err.ErrAccessDenied)
		return
	}

	bucket := d.S3Info.Bucket

	// Parse request body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil {
		logger.Error().Err(err).Msg("failed to read replication config body")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
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

	// Update bucket with replication config
	bucketInfo.ReplicationConfig = &config

	// Store in local cache (will be persisted to DB)
	s.bucketStore.SetBucket(bucket, bucketInfo)

	// TODO: Persist to database
	// Replication will be triggered by CRRHook on PutObject/DeleteObject

	logger.Info().
		Str("bucket", bucket).
		Int("rules", len(config.Rules)).
		Msg("bucket replication configuration updated")

	w.WriteHeader(http.StatusOK)
}

// DeleteBucketReplicationHandler removes the replication configuration from a bucket.
// DELETE /{bucket}?replication
func (s *MetadataServer) DeleteBucketReplicationHandler(d *data.Data, w http.ResponseWriter) {
	if !checkReplicationLicense() {
		logger.Warn().Msg("replication requires enterprise license")
		writeXMLErrorResponse(w, d, s3err.ErrAccessDenied)
		return
	}

	bucket := d.S3Info.Bucket

	// Get bucket info from local cache
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchBucket)
		return
	}

	// Remove replication config
	bucketInfo.ReplicationConfig = nil
	s.bucketStore.SetBucket(bucket, bucketInfo)

	// TODO: Persist to database

	logger.Info().Str("bucket", bucket).Msg("bucket replication configuration deleted")

	w.WriteHeader(http.StatusNoContent)
}
