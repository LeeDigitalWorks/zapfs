// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
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

// GetBucketPolicyStatusHandler checks if bucket policy grants public access.
// GET /{bucket}?policyStatus
//
// Analyzes the bucket policy to determine if it allows public access by checking
// for Principal: "*" or Principal: {"AWS": "*"} in Allow statements.
//
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicyStatus.html
func (s *MetadataServer) GetBucketPolicyStatusHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Check if bucket has a policy
	policy, err := s.svc.Config().GetBucketPolicy(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			// No policy means not public
			if cfgErr.Code == config.ErrCodeNoSuchBucketPolicy {
				writePolicyStatusResponse(w, d, false)
				return
			}
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get bucket policy")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Analyze policy for public access
	isPublic := isPolicyPublic(policy)
	writePolicyStatusResponse(w, d, isPublic)
}

// writePolicyStatusResponse writes a PolicyStatus XML response
func writePolicyStatusResponse(w http.ResponseWriter, d *data.Data, isPublic bool) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	publicStr := "false"
	if isPublic {
		publicStr = "true"
	}
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><PolicyStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsPublic>` + publicStr + `</IsPublic></PolicyStatus>`))
}

// isPolicyPublic checks if a bucket policy grants public access
func isPolicyPublic(policy *s3types.BucketPolicy) bool {
	if policy == nil || len(policy.Statements) == 0 {
		return false
	}

	for _, stmt := range policy.Statements {
		// Skip Deny statements - they restrict access, not grant it
		if stmt.Effect == "Deny" {
			continue
		}

		// Check Principal for public access
		if isPublicPrincipal(stmt.Principal) {
			// If no conditions, it's public
			// If conditions exist, we conservatively say it's not public
			// (conditions like aws:SourceVpc restrict access)
			if len(stmt.Condition) == 0 {
				return true
			}
		}
	}

	return false
}

// isPublicPrincipal checks if a principal represents public access
func isPublicPrincipal(principal interface{}) bool {
	switch p := principal.(type) {
	case string:
		return p == "*"
	case map[string]interface{}:
		if aws, ok := p["AWS"]; ok {
			switch a := aws.(type) {
			case string:
				return a == "*"
			case []interface{}:
				for _, v := range a {
					if s, ok := v.(string); ok && s == "*" {
						return true
					}
				}
			}
		}
	}
	return false
}
