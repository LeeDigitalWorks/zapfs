//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/config"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// GetBucketEncryptionHandler returns the encryption configuration for a bucket.
// GET /{bucket}?encryption
// Enterprise feature: requires FeatureKMS license for SSE-KMS.
func (s *MetadataServer) GetBucketEncryptionHandler(d *data.Data, w http.ResponseWriter) {
	if !checkKMSLicense() {
		logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("bucket encryption feature requires enterprise license")
		writeXMLErrorResponse(w, d, s3err.ErrAccessDenied)
		return
	}

	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	encConfig, err := s.svc.Config().GetBucketEncryption(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get bucket encryption")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(encConfig)
}

// PutBucketEncryptionHandler sets the encryption configuration for a bucket.
// PUT /{bucket}?encryption
// Enterprise feature: requires FeatureKMS license for SSE-KMS.
func (s *MetadataServer) PutBucketEncryptionHandler(d *data.Data, w http.ResponseWriter) {
	if !checkKMSLicense() {
		logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("bucket encryption feature requires enterprise license")
		writeXMLErrorResponse(w, d, s3err.ErrAccessDenied)
		return
	}

	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	bucket := d.S3Info.Bucket

	// Read encryption config from body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil || len(body) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Parse encryption configuration
	var encConfig s3types.ServerSideEncryptionConfig
	if err := xml.Unmarshal(body, &encConfig); err != nil {
		logger.Warn().Err(err).Str("bucket", bucket).Msg("invalid encryption XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Call service (validation is done in service)
	if err := s.svc.Config().SetBucketEncryption(d.Ctx, bucket, &encConfig); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to set bucket encryption")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	logger.Info().Str("bucket", bucket).Int("rules", len(encConfig.Rules)).Msg("bucket encryption updated")
}

// DeleteBucketEncryptionHandler removes the encryption configuration for a bucket.
// DELETE /{bucket}?encryption
// Enterprise feature: requires FeatureKMS license for SSE-KMS.
func (s *MetadataServer) DeleteBucketEncryptionHandler(d *data.Data, w http.ResponseWriter) {
	if !checkKMSLicense() {
		logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("bucket encryption feature requires enterprise license")
		writeXMLErrorResponse(w, d, s3err.ErrAccessDenied)
		return
	}

	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	if err := s.svc.Config().DeleteBucketEncryption(d.Ctx, d.S3Info.Bucket); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to delete bucket encryption")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)

	logger.Info().Str("bucket", d.S3Info.Bucket).Msg("bucket encryption deleted")
}
