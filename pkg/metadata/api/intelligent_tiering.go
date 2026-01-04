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
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// GetBucketIntelligentTieringConfigurationHandler returns intelligent tiering configuration.
// GET /{bucket}?intelligent-tiering&id={id}
//
// Requires FeatureLifecycle license.
func (s *MetadataServer) GetBucketIntelligentTieringConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckLifecycle() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	bucket := d.S3Info.Bucket
	configID := d.Req.URL.Query().Get("id")
	if configID == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	config, err := s.db.GetIntelligentTieringConfiguration(d.Req.Context(), bucket, configID)
	if err != nil {
		if errors.Is(err, db.ErrIntelligentTieringNotFound) {
			writeXMLErrorResponse(w, d, s3err.ErrNoSuchConfiguration)
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Str("id", configID).Msg("failed to get intelligent tiering config")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(config)
}

// PutBucketIntelligentTieringConfigurationHandler sets intelligent tiering configuration.
// PUT /{bucket}?intelligent-tiering&id={id}
//
// Requires FeatureLifecycle license.
func (s *MetadataServer) PutBucketIntelligentTieringConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckLifecycle() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	bucket := d.S3Info.Bucket
	configID := d.Req.URL.Query().Get("id")
	if configID == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Parse request body
	body, err := io.ReadAll(io.LimitReader(d.Req.Body, 1<<20)) // 1MB limit
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrIncompleteBody)
		return
	}

	var config s3types.IntelligentTieringConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		logger.Debug().Err(err).Msg("failed to parse intelligent tiering config")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Ensure ID matches query parameter
	if config.ID != configID {
		config.ID = configID
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		logger.Debug().Err(err).Msg("invalid intelligent tiering config")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Store configuration
	if err := s.db.PutIntelligentTieringConfiguration(d.Req.Context(), bucket, &config); err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Str("id", configID).Msg("failed to put intelligent tiering config")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	logger.Info().Str("bucket", bucket).Str("id", configID).Msg("intelligent tiering configuration saved")

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
}

// DeleteBucketIntelligentTieringConfigurationHandler removes intelligent tiering configuration.
// DELETE /{bucket}?intelligent-tiering&id={id}
//
// Requires FeatureLifecycle license.
func (s *MetadataServer) DeleteBucketIntelligentTieringConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckLifecycle() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	bucket := d.S3Info.Bucket
	configID := d.Req.URL.Query().Get("id")
	if configID == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	err := s.db.DeleteIntelligentTieringConfiguration(d.Req.Context(), bucket, configID)
	if err != nil {
		if errors.Is(err, db.ErrIntelligentTieringNotFound) {
			writeXMLErrorResponse(w, d, s3err.ErrNoSuchConfiguration)
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Str("id", configID).Msg("failed to delete intelligent tiering config")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	logger.Info().Str("bucket", bucket).Str("id", configID).Msg("intelligent tiering configuration deleted")

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)
}

// ListBucketIntelligentTieringConfigurationsHandler lists intelligent tiering configurations.
// GET /{bucket}?intelligent-tiering
//
// Requires FeatureLifecycle license.
func (s *MetadataServer) ListBucketIntelligentTieringConfigurationsHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckLifecycle() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	bucket := d.S3Info.Bucket

	configs, err := s.db.ListIntelligentTieringConfigurations(d.Req.Context(), bucket)
	if err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to list intelligent tiering configs")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	result := s3types.ListBucketIntelligentTieringConfigurationsResult{
		XMLNS:       "http://s3.amazonaws.com/doc/2006-03-01/",
		IsTruncated: false,
	}
	for _, c := range configs {
		result.Configurations = append(result.Configurations, *c)
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header))
	xml.NewEncoder(w).Encode(result)
}
