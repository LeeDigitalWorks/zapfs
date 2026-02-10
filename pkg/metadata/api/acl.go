// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

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

// GetBucketAclHandler returns the ACL of a bucket.
// GET /{bucket}?acl
func (s *MetadataServer) GetBucketAclHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	acl, err := s.svc.Config().GetBucketACL(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get bucket ACL")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Build response
	resp := buildACLResponse(acl)

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(resp)
}

// PutBucketAclHandler sets the ACL of a bucket.
// PUT /{bucket}?acl
func (s *MetadataServer) PutBucketAclHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	bucket := d.S3Info.Bucket

	// Get bucket info to get owner ID
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchBucket)
		return
	}

	// Check if ACLs are disabled (BucketOwnerEnforced)
	if isBucketACLDisabled(&bucketInfo) {
		writeXMLErrorResponse(w, d, s3err.ErrAccessControlListNotSupported)
		return
	}

	var acl *s3types.AccessControlList

	// Try to build ACL from headers (canned ACL or grant headers)
	headerACL, errCode := ACLFromRequest(d.Req, bucketInfo.OwnerID)
	if errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}

	if headerACL != nil {
		acl = headerACL
	} else {
		// No headers - parse ACL from body
		body, err := io.ReadAll(io.LimitReader(d.Req.Body, maxXMLBodySize+1))
		if err != nil || len(body) == 0 {
			// No body and no headers - use private
			acl = s3types.NewPrivateACL(bucketInfo.OwnerID, bucketInfo.OwnerID)
		} else if int64(len(body)) > maxXMLBodySize {
			writeXMLErrorResponse(w, d, s3err.ErrEntityTooLarge)
			return
		} else {
			acl, err = parseACLXML(body, bucketInfo.OwnerID)
			if err != nil {
				writeXMLErrorResponse(w, d, s3err.ErrMalformedACLError)
				return
			}
		}
	}

	// Call service
	if err := s.svc.Config().SetBucketACL(d.Ctx, bucket, acl); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to set bucket ACL")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
}

// GetObjectAclHandler returns the ACL of an object.
// GET /{bucket}/{key}?acl
func (s *MetadataServer) GetObjectAclHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	acl, err := s.svc.Config().GetObjectACL(d.Ctx, d.S3Info.Bucket, d.S3Info.Key)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("failed to get object ACL")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Build response
	resp := buildACLResponse(acl)

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(resp)
}

// PutObjectAclHandler sets the ACL of an object.
// PUT /{bucket}/{key}?acl
func (s *MetadataServer) PutObjectAclHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	bucket := d.S3Info.Bucket
	key := d.S3Info.Key
	ownerID := d.S3Info.OwnerID

	// Check if ACLs are disabled (BucketOwnerEnforced)
	if bucketInfo, exists := s.bucketStore.GetBucket(bucket); exists {
		if isBucketACLDisabled(&bucketInfo) {
			writeXMLErrorResponse(w, d, s3err.ErrAccessControlListNotSupported)
			return
		}
	}

	var acl *s3types.AccessControlList

	// Try to build ACL from headers (canned ACL or grant headers)
	headerACL, errCode := ACLFromRequest(d.Req, ownerID)
	if errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}

	if headerACL != nil {
		acl = headerACL
	} else {
		// No headers - parse ACL from body
		body, err := io.ReadAll(io.LimitReader(d.Req.Body, maxXMLBodySize+1))
		if err != nil || len(body) == 0 {
			// No body and no headers - use private
			acl = s3types.NewPrivateACL(ownerID, ownerID)
		} else if int64(len(body)) > maxXMLBodySize {
			writeXMLErrorResponse(w, d, s3err.ErrEntityTooLarge)
			return
		} else {
			acl, err = parseACLXML(body, ownerID)
			if err != nil {
				writeXMLErrorResponse(w, d, s3err.ErrMalformedACLError)
				return
			}
		}
	}

	// Call service
	if err := s.svc.Config().SetObjectACL(d.Ctx, bucket, key, acl); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("failed to set object ACL")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
}

// Helper functions

// isBucketACLDisabled checks if ACLs are disabled for the bucket.
// ACLs are disabled when BucketOwnerEnforced ownership control is set.
func isBucketACLDisabled(bucket *s3types.Bucket) bool {
	if bucket == nil || bucket.OwnershipControls == nil {
		return false
	}
	if len(bucket.OwnershipControls.Rules) == 0 {
		return false
	}
	return bucket.OwnershipControls.Rules[0].ObjectOwnership == s3types.ObjectOwnershipBucketOwnerEnforced
}
