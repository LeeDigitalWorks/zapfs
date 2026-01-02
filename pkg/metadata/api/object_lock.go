// Copyright 2025 ZapFS Authors. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the LICENSE file.

package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/config"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// GetObjectLockConfigurationHandler returns the Object Lock configuration for a bucket.
// GET /{bucket}?object-lock
//
// Requires FeatureObjectLock license.
func (s *MetadataServer) GetObjectLockConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckObjectLock() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	cfg, err := s.svc.Config().GetObjectLockConfiguration(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get object lock config")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(cfg)
}

// PutObjectLockConfigurationHandler sets the Object Lock configuration for a bucket.
// PUT /{bucket}?object-lock
//
// Requires FeatureObjectLock license.
func (s *MetadataServer) PutObjectLockConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckObjectLock() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	bucket := d.S3Info.Bucket

	// Read config from body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil || len(body) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Parse Object Lock configuration
	var cfg s3types.ObjectLockConfiguration
	if err := xml.Unmarshal(body, &cfg); err != nil {
		logger.Warn().Err(err).Str("bucket", bucket).Msg("invalid object lock XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Call service (validation is done in service)
	if err := s.svc.Config().SetObjectLockConfiguration(d.Ctx, bucket, &cfg); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to set object lock config")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	logger.Info().Str("bucket", bucket).Msg("object lock configuration updated")
}

// GetObjectRetentionHandler returns the retention settings for an object.
// GET /{bucket}/{key}?retention
//
// Requires FeatureObjectLock license.
func (s *MetadataServer) GetObjectRetentionHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckObjectLock() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Verify object exists
	objRef, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchKey)
		return
	}

	if objRef.IsDeleted() {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchKey)
		return
	}

	// Get retention from database
	retention, err := s.db.GetObjectRetention(ctx, bucket, key)
	if err != nil {
		if err == db.ErrRetentionNotFound {
			writeXMLErrorResponse(w, d, s3err.ErrNoSuchObjectLockConfiguration)
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("failed to get object retention")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	if objRef.ID.String() != "" {
		w.Header().Set("x-amz-version-id", objRef.ID.String())
	}
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(retention)
}

// PutObjectRetentionHandler sets the retention settings for an object.
// PUT /{bucket}/{key}?retention
//
// Requires FeatureObjectLock license.
func (s *MetadataServer) PutObjectRetentionHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckObjectLock() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Verify object exists
	objRef, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchKey)
		return
	}

	if objRef.IsDeleted() {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchKey)
		return
	}

	// Read retention from body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil || len(body) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Parse retention
	var retention s3types.ObjectLockRetention
	if err := xml.Unmarshal(body, &retention); err != nil {
		logger.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("invalid retention XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Validate retention
	if retention.Mode != "GOVERNANCE" && retention.Mode != "COMPLIANCE" {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Parse and validate retain until date
	if retention.RetainUntilDate == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidRetentionPeriod)
		return
	}

	retainUntil, err := time.Parse(time.RFC3339, retention.RetainUntilDate)
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidRetentionPeriod)
		return
	}

	// Must be in the future
	if retainUntil.Before(time.Now()) {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidRetentionPeriod)
		return
	}

	// Check if existing retention prevents modification (COMPLIANCE mode)
	existingRetention, err := s.db.GetObjectRetention(ctx, bucket, key)
	if err == nil && existingRetention != nil {
		if existingRetention.Mode == "COMPLIANCE" {
			existingUntil, _ := time.Parse(time.RFC3339, existingRetention.RetainUntilDate)
			if time.Now().Before(existingUntil) {
				// Cannot modify COMPLIANCE retention until it expires
				// unless extending the retention period
				if retainUntil.Before(existingUntil) {
					writeXMLErrorResponse(w, d, s3err.ErrAccessDenied)
					return
				}
			}
		}
	}

	// Store retention
	if err := s.db.SetObjectRetention(ctx, bucket, key, &retention); err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("failed to set object retention")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	if objRef.ID.String() != "" {
		w.Header().Set("x-amz-version-id", objRef.ID.String())
	}
	w.WriteHeader(http.StatusOK)

	logger.Info().Str("bucket", bucket).Str("key", key).Str("mode", retention.Mode).Msg("object retention updated")
}

// GetObjectLegalHoldHandler returns the legal hold status for an object.
// GET /{bucket}/{key}?legal-hold
//
// Requires FeatureObjectLock license.
func (s *MetadataServer) GetObjectLegalHoldHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckObjectLock() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Verify object exists
	objRef, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchKey)
		return
	}

	if objRef.IsDeleted() {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchKey)
		return
	}

	// Get legal hold from database
	legalHold, err := s.db.GetObjectLegalHold(ctx, bucket, key)
	if err != nil {
		if err == db.ErrLegalHoldNotFound {
			// Return OFF status if no legal hold set
			legalHold = &s3types.ObjectLockLegalHold{Status: "OFF"}
		} else {
			logger.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("failed to get legal hold")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	if objRef.ID.String() != "" {
		w.Header().Set("x-amz-version-id", objRef.ID.String())
	}
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(legalHold)
}

// PutObjectLegalHoldHandler sets the legal hold status for an object.
// PUT /{bucket}/{key}?legal-hold
//
// Requires FeatureObjectLock license.
func (s *MetadataServer) PutObjectLegalHoldHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckObjectLock() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Verify object exists
	objRef, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchKey)
		return
	}

	if objRef.IsDeleted() {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchKey)
		return
	}

	// Read legal hold from body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil || len(body) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Parse legal hold
	var legalHold s3types.ObjectLockLegalHold
	if err := xml.Unmarshal(body, &legalHold); err != nil {
		logger.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("invalid legal hold XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Validate status
	if legalHold.Status != "ON" && legalHold.Status != "OFF" {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Store legal hold
	if err := s.db.SetObjectLegalHold(ctx, bucket, key, &legalHold); err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("failed to set legal hold")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	if objRef.ID.String() != "" {
		w.Header().Set("x-amz-version-id", objRef.ID.String())
	}
	w.WriteHeader(http.StatusOK)

	logger.Info().Str("bucket", bucket).Str("key", key).Str("status", legalHold.Status).Msg("legal hold updated")
}
