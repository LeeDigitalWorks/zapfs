// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/xml"
	"io"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// GetBucketLoggingHandler returns bucket logging configuration.
// GET /{bucket}?logging
//
// This API works in both community and enterprise editions.
// Actual log collection and delivery requires enterprise license with FeatureAccessLog.
func (s *MetadataServer) GetBucketLoggingHandler(d *data.Data, w http.ResponseWriter) {
	// Build response
	response := s3types.BucketLoggingStatus{}

	// Check cache first
	if bucket, exists := s.bucketStore.GetBucket(d.S3Info.Bucket); exists && bucket.Logging != nil {
		response = *bucket.Logging
	} else {
		// Cache miss - get logging configuration from database
		config, err := s.db.GetBucketLogging(d.Ctx, d.S3Info.Bucket)
		if err != nil {
			logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get logging config")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
			return
		}

		if config != nil && config.TargetBucket != "" {
			response.LoggingEnabled = s3types.LoggingRule{
				TargetBucket: config.TargetBucket,
				TargetPrefix: config.TargetPrefix,
			}

			// Update cache
			if bucket, exists := s.bucketStore.GetBucket(d.S3Info.Bucket); exists {
				bucket.Logging = &response
				s.bucketStore.SetBucket(d.S3Info.Bucket, bucket)
			}
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))

	output, err := xml.MarshalIndent(response, "", "  ")
	if err != nil {
		logger.Error().Err(err).Msg("failed to marshal logging status")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header))
	w.Write(output)
}

// PutBucketLoggingHandler sets bucket logging configuration.
// PUT /{bucket}?logging
//
// This API works in both community and enterprise editions.
// Actual log collection and delivery requires enterprise license with FeatureAccessLog.
func (s *MetadataServer) PutBucketLoggingHandler(d *data.Data, w http.ResponseWriter) {
	// Parse request body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil {
		logger.Error().Err(err).Msg("failed to read request body")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	var loggingStatus s3types.BucketLoggingStatus
	if err := xml.Unmarshal(body, &loggingStatus); err != nil {
		logger.Error().Err(err).Msg("failed to parse logging status XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Build config
	config := &db.BucketLoggingConfig{
		SourceBucket: d.S3Info.Bucket,
	}

	if loggingStatus.LoggingEnabled.TargetBucket != "" {
		config.TargetBucket = loggingStatus.LoggingEnabled.TargetBucket
		config.TargetPrefix = loggingStatus.LoggingEnabled.TargetPrefix

		// Validate target bucket exists
		if config.TargetBucket != "" {
			_, err := s.db.GetBucket(d.Ctx, config.TargetBucket)
			if err != nil {
				logger.Warn().
					Str("source", d.S3Info.Bucket).
					Str("target", config.TargetBucket).
					Msg("target bucket not found")
				writeXMLErrorResponse(w, d, s3err.ErrInvalidTargetBucketForLogging)
				return
			}
		}
	}

	// Store logging configuration
	if err := s.db.SetBucketLogging(d.Ctx, config); err != nil {
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to set logging config")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Update bucket cache with new logging config
	if bucket, exists := s.bucketStore.GetBucket(d.S3Info.Bucket); exists {
		if config.TargetBucket != "" {
			bucket.Logging = &s3types.BucketLoggingStatus{
				LoggingEnabled: s3types.LoggingRule{
					TargetBucket: config.TargetBucket,
					TargetPrefix: config.TargetPrefix,
				},
			}
		} else {
			bucket.Logging = nil
		}
		s.bucketStore.SetBucket(d.S3Info.Bucket, bucket)
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
}
