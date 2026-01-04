// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// GetBucketTaggingHandler returns the tag set for a bucket.
// GET /{bucket}?tagging
func (s *MetadataServer) GetBucketTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	tagSet, err := s.svc.Config().GetBucketTagging(d.Ctx, d.S3Info.Bucket)
	if handleServiceError(w, d, err) {
		return
	}

	writeXMLResponse(w, d, http.StatusOK, tagSet)
}

// PutBucketTaggingHandler sets the tag set for a bucket.
// PUT /{bucket}?tagging
func (s *MetadataServer) PutBucketTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	var tagSet s3types.TagSet
	if errCode := parseXMLBody(d, &tagSet); errCode != s3err.ErrNone {
		writeXMLErrorResponse(w, d, errCode)
		return
	}

	if err := s.svc.Config().SetBucketTagging(d.Ctx, d.S3Info.Bucket, &tagSet); handleServiceError(w, d, err) {
		return
	}

	writeNoContent(w, d)
}

// DeleteBucketTaggingHandler removes the tag set for a bucket.
// DELETE /{bucket}?tagging
func (s *MetadataServer) DeleteBucketTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	if err := s.svc.Config().DeleteBucketTagging(d.Ctx, d.S3Info.Bucket); handleServiceError(w, d, err) {
		return
	}

	writeNoContent(w, d)
}

// GetObjectTaggingHandler returns the tag set for an object.
// GET /{bucket}/{key}?tagging
func (s *MetadataServer) GetObjectTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	result, err := s.svc.Config().GetObjectTagging(d.Ctx, d.S3Info.Bucket, d.S3Info.Key)
	if handleServiceError(w, d, err) {
		return
	}

	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", result.VersionID)
	}
	writeXMLResponse(w, d, http.StatusOK, result.Tags)
}

// PutObjectTaggingHandler sets the tag set for an object.
// PUT /{bucket}/{key}?tagging
func (s *MetadataServer) PutObjectTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	var tagSet s3types.TagSet
	if errCode := parseXMLBody(d, &tagSet); errCode != s3err.ErrNone {
		writeXMLErrorResponse(w, d, errCode)
		return
	}

	result, err := s.svc.Config().SetObjectTagging(d.Ctx, d.S3Info.Bucket, d.S3Info.Key, &tagSet)
	if handleServiceError(w, d, err) {
		return
	}

	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", result.VersionID)
	}
	writeXMLResponse(w, d, http.StatusOK, nil)
}

// DeleteObjectTaggingHandler removes the tag set for an object.
// DELETE /{bucket}/{key}?tagging
func (s *MetadataServer) DeleteObjectTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	result, err := s.svc.Config().DeleteObjectTagging(d.Ctx, d.S3Info.Bucket, d.S3Info.Key)
	if handleServiceError(w, d, err) {
		return
	}

	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", result.VersionID)
	}
	writeNoContent(w, d)
}
