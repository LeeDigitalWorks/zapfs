package api

import (
	"encoding/json"
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

// GetBucketPolicyHandler returns the policy of a bucket.
// GET /{bucket}?policy
func (s *MetadataServer) GetBucketPolicyHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	policy, err := s.svc.Config().GetBucketPolicy(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get bucket policy")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Return policy as JSON
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		logger.Error().Err(err).Msg("failed to marshal policy")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	w.Write(policyJSON)
}

// PutBucketPolicyHandler sets the policy of a bucket.
// PUT /{bucket}?policy
func (s *MetadataServer) PutBucketPolicyHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	bucket := d.S3Info.Bucket

	// Read policy JSON from body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil || len(body) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedPolicy)
		return
	}

	// Parse policy
	var policy s3types.BucketPolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		logger.Warn().Err(err).Str("bucket", bucket).Msg("invalid policy JSON")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedPolicy)
		return
	}

	// Call service (validation is done in service)
	if err := s.svc.Config().SetBucketPolicy(d.Ctx, bucket, &policy); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to set bucket policy")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)

	logger.Info().Str("bucket", bucket).Int("statements", len(policy.Statements)).Msg("bucket policy updated")
}

// DeleteBucketPolicyHandler removes the policy of a bucket.
// DELETE /{bucket}?policy
func (s *MetadataServer) DeleteBucketPolicyHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	if err := s.svc.Config().DeleteBucketPolicy(d.Ctx, d.S3Info.Bucket); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to delete bucket policy")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)

	logger.Info().Str("bucket", d.S3Info.Bucket).Msg("bucket policy deleted")
}
