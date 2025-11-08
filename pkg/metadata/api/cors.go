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

// GetBucketCorsHandler returns the CORS configuration for a bucket.
// GET /{bucket}?cors
func (s *MetadataServer) GetBucketCorsHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	cors, err := s.svc.Config().GetBucketCORS(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get bucket CORS")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(cors)
}

// PutBucketCorsHandler sets the CORS configuration for a bucket.
// PUT /{bucket}?cors
func (s *MetadataServer) PutBucketCorsHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Read CORS config from body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil || len(body) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Parse CORS configuration
	var cors s3types.CORSConfiguration
	if err := xml.Unmarshal(body, &cors); err != nil {
		logger.Warn().Err(err).Str("bucket", d.S3Info.Bucket).Msg("invalid CORS XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Call service (validation is done in service)
	if err := s.svc.Config().SetBucketCORS(d.Ctx, d.S3Info.Bucket, &cors); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to set bucket CORS")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	logger.Info().Str("bucket", d.S3Info.Bucket).Int("rules", len(cors.Rules)).Msg("bucket CORS updated")
}

// DeleteBucketCorsHandler removes the CORS configuration for a bucket.
// DELETE /{bucket}?cors
func (s *MetadataServer) DeleteBucketCorsHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	if err := s.svc.Config().DeleteBucketCORS(d.Ctx, d.S3Info.Bucket); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to delete bucket CORS")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)

	logger.Info().Str("bucket", d.S3Info.Bucket).Msg("bucket CORS deleted")
}
