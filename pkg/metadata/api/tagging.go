package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"

	"zapfs/pkg/logger"
	"zapfs/pkg/metadata/data"
	"zapfs/pkg/metadata/service/config"
	"zapfs/pkg/s3api/s3consts"
	"zapfs/pkg/s3api/s3err"
	"zapfs/pkg/s3api/s3types"
)

// GetBucketTaggingHandler returns the tag set for a bucket.
// GET /{bucket}?tagging
func (s *MetadataServer) GetBucketTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	tagSet, err := s.svc.Config().GetBucketTagging(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get bucket tagging")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(tagSet)
}

// PutBucketTaggingHandler sets the tag set for a bucket.
// PUT /{bucket}?tagging
func (s *MetadataServer) PutBucketTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Parse request body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil || len(body) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	var tagSet s3types.TagSet
	if err := xml.Unmarshal(body, &tagSet); err != nil {
		logger.Warn().Err(err).Str("bucket", d.S3Info.Bucket).Msg("invalid tagging XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Call service
	if err := s.svc.Config().SetBucketTagging(d.Ctx, d.S3Info.Bucket, &tagSet); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to set bucket tagging")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)

	logger.Info().Str("bucket", d.S3Info.Bucket).Int("tags", len(tagSet.Tags)).Msg("bucket tagging updated")
}

// DeleteBucketTaggingHandler removes the tag set for a bucket.
// DELETE /{bucket}?tagging
func (s *MetadataServer) DeleteBucketTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	if err := s.svc.Config().DeleteBucketTagging(d.Ctx, d.S3Info.Bucket); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to delete bucket tagging")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)

	logger.Info().Str("bucket", d.S3Info.Bucket).Msg("bucket tagging deleted")
}

// GetObjectTaggingHandler returns the tag set for an object.
// GET /{bucket}/{key}?tagging
func (s *MetadataServer) GetObjectTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	result, err := s.svc.Config().GetObjectTagging(d.Ctx, d.S3Info.Bucket, d.S3Info.Key)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("failed to get object tagging")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", result.VersionID)
	}
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result.Tags)
}

// PutObjectTaggingHandler sets the tag set for an object.
// PUT /{bucket}/{key}?tagging
func (s *MetadataServer) PutObjectTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Parse request body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil || len(body) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	var tagSet s3types.TagSet
	if err := xml.Unmarshal(body, &tagSet); err != nil {
		logger.Warn().Err(err).Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("invalid tagging XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Call service
	result, err := s.svc.Config().SetObjectTagging(d.Ctx, d.S3Info.Bucket, d.S3Info.Key, &tagSet)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("failed to set object tagging")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", result.VersionID)
	}
	w.WriteHeader(http.StatusOK)

	logger.Info().Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Int("tags", len(tagSet.Tags)).Msg("object tagging updated")
}

// DeleteObjectTaggingHandler removes the tag set for an object.
// DELETE /{bucket}/{key}?tagging
func (s *MetadataServer) DeleteObjectTaggingHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	result, err := s.svc.Config().DeleteObjectTagging(d.Ctx, d.S3Info.Bucket, d.S3Info.Key)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("failed to delete object tagging")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", result.VersionID)
	}
	w.WriteHeader(http.StatusNoContent)

	logger.Info().Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("object tagging deleted")
}
