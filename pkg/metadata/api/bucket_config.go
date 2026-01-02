// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/xml"
	"io"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// ============================================================================
// Bucket Request Payment (AWS-Specific - Requester Pays)
// ============================================================================

// GetBucketRequestPaymentHandler returns requester pays configuration.
// GET /{bucket}?requestPayment
//
// ZapFS does not support requester pays - always returns BucketOwner.
func (s *MetadataServer) GetBucketRequestPaymentHandler(d *data.Data, w http.ResponseWriter) {
	// Requester Pays is not supported - always return BucketOwner
	// This is compliant with AWS S3 which defaults to BucketOwner
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Payer>BucketOwner</Payer></RequestPaymentConfiguration>`))
}

// PutBucketRequestPaymentHandler sets requester pays configuration.
// PUT /{bucket}?requestPayment
//
// ZapFS does not support requester pays - accepts but ignores.
func (s *MetadataServer) PutBucketRequestPaymentHandler(d *data.Data, w http.ResponseWriter) {
	// Accept but ignore - requester pays not supported
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
}

// ============================================================================
// Bucket Transfer Acceleration (AWS-Specific)
// ============================================================================

// GetBucketAccelerateConfigurationHandler returns transfer acceleration config.
// GET /{bucket}?accelerate
//
// ZapFS does not support transfer acceleration - returns empty/suspended.
func (s *MetadataServer) GetBucketAccelerateConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// Transfer acceleration not supported - return empty config (suspended)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></AccelerateConfiguration>`))
}

// PutBucketAccelerateConfigurationHandler sets transfer acceleration.
// PUT /{bucket}?accelerate
//
// ZapFS does not support transfer acceleration.
func (s *MetadataServer) PutBucketAccelerateConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// Transfer acceleration not supported
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// ============================================================================
// Bucket Event Notifications (AWS-Specific - Lambda/SNS/SQS)
// ============================================================================

// GetBucketNotificationConfigurationHandler returns notification configuration.
// GET /{bucket}?notification
//
// Requires FeatureEvents license.
func (s *MetadataServer) GetBucketNotificationConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// Enterprise license check
	if !license.CheckEvents() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	config, err := s.db.GetNotificationConfiguration(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get notification config")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Return empty config if none configured
	if config == nil {
		config = &s3types.NotificationConfiguration{}
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(config)
}

// PutBucketNotificationConfigurationHandler sets notification configuration.
// PUT /{bucket}?notification
//
// Requires FeatureEvents license.
func (s *MetadataServer) PutBucketNotificationConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// Enterprise license check
	if !license.CheckEvents() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	// Parse request body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	var config s3types.NotificationConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Empty config = delete notification configuration
	if len(config.TopicConfigurations) == 0 &&
		len(config.QueueConfigurations) == 0 &&
		len(config.LambdaFunctionConfigurations) == 0 {
		if err := s.db.DeleteNotificationConfiguration(d.Ctx, d.S3Info.Bucket); err != nil {
			logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to delete notification config")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
			return
		}
	} else {
		if err := s.db.SetNotificationConfiguration(d.Ctx, d.S3Info.Bucket, &config); err != nil {
			logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to set notification config")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
			return
		}
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
}
