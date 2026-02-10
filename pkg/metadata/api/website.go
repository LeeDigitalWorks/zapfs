// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/xml"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/config"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/object"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// GetBucketWebsiteHandler returns the website configuration for a bucket.
// GET /{bucket}?website
func (s *MetadataServer) GetBucketWebsiteHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	website, err := s.svc.Config().GetBucketWebsite(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get bucket website")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(website)
}

// PutBucketWebsiteHandler sets the website configuration for a bucket.
// PUT /{bucket}?website
func (s *MetadataServer) PutBucketWebsiteHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	bucket := d.S3Info.Bucket

	// Read website config from body
	body, err := io.ReadAll(io.LimitReader(d.Req.Body, maxXMLBodySize+1))
	if err != nil || len(body) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}
	if int64(len(body)) > maxXMLBodySize {
		writeXMLErrorResponse(w, d, s3err.ErrEntityTooLarge)
		return
	}

	// Parse website configuration
	var website s3types.WebsiteConfiguration
	if err := xml.Unmarshal(body, &website); err != nil {
		logger.Warn().Err(err).Str("bucket", bucket).Msg("invalid website XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Call service (validation is done in service)
	if err := s.svc.Config().SetBucketWebsite(d.Ctx, bucket, &website); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to set bucket website")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	logger.Info().Str("bucket", bucket).Msg("bucket website configuration updated")
}

// DeleteBucketWebsiteHandler removes the website configuration for a bucket.
// DELETE /{bucket}?website
func (s *MetadataServer) DeleteBucketWebsiteHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	if err := s.svc.Config().DeleteBucketWebsite(d.Ctx, d.S3Info.Bucket); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to delete bucket website")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)

	logger.Info().Str("bucket", d.S3Info.Bucket).Msg("bucket website configuration deleted")
}

// ServeWebsiteContent handles GET/HEAD requests for website-hosted buckets.
// Called from GetObjectHandler when d.IsWebsiteRequest is true.
func (s *MetadataServer) ServeWebsiteContent(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeHTMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Get bucket website config
	website, err := s.svc.Config().GetBucketWebsite(d.Ctx, bucket)
	if err != nil {
		writeHTMLErrorResponse(w, d, s3err.ErrNoSuchWebsiteConfiguration)
		return
	}

	// Handle index document for directory paths (empty key or trailing slash)
	if key == "" || strings.HasSuffix(key, "/") {
		if website.IndexDocument != nil && website.IndexDocument.Suffix != "" {
			key = key + website.IndexDocument.Suffix
			d.S3Info.Key = key
		}
	}

	// Get the object
	req := &object.GetObjectRequest{
		Bucket: bucket,
		Key:    key,
	}

	result, err := s.svc.Objects().GetObject(d.Ctx, req)
	if err != nil {
		s.serveWebsiteError(d, w, err, website)
		return
	}
	defer result.Body.Close()

	s.streamWebsiteObject(d, w, result.Object, result.Body)
}

// serveWebsiteError handles errors for website requests.
func (s *MetadataServer) serveWebsiteError(d *data.Data, w http.ResponseWriter, err error, website *s3types.WebsiteConfiguration) {
	var errCode s3err.ErrorCode

	// Check for object service error type
	if objErr, ok := err.(*object.Error); ok {
		errCode = objErr.ToS3Error()
	} else {
		errCode = s3err.ErrInternalError
	}

	// Try custom error document for 404s
	if errCode == s3err.ErrNoSuchKey && website.ErrorDocument != nil && website.ErrorDocument.Key != "" {
		req := &object.GetObjectRequest{
			Bucket: d.S3Info.Bucket,
			Key:    website.ErrorDocument.Key,
		}

		result, getErr := s.svc.Objects().GetObject(d.Ctx, req)
		if getErr == nil {
			defer result.Body.Close()
			w.WriteHeader(http.StatusNotFound)
			s.streamWebsiteObject(d, w, result.Object, result.Body)
			return
		}
	}

	writeHTMLErrorResponse(w, d, errCode)
}

// streamWebsiteObject writes object content with appropriate headers.
func (s *MetadataServer) streamWebsiteObject(d *data.Data, w http.ResponseWriter, obj *types.ObjectRef, body io.Reader) {
	contentType := obj.ContentType
	if contentType == "" {
		contentType = mime.TypeByExtension(filepath.Ext(d.S3Info.Key))
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.FormatUint(obj.Size, 10))
	w.Header().Set("ETag", obj.ETag)

	// Only write body for GET requests, not HEAD
	if d.Req.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	w.WriteHeader(http.StatusOK)
	io.Copy(w, body)
}
