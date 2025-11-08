package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"zapfs/pkg/logger"
	"zapfs/pkg/metadata/data"
	"zapfs/pkg/metadata/service/config"
	"zapfs/pkg/s3api/s3consts"
	"zapfs/pkg/s3api/s3err"
	"zapfs/pkg/s3api/s3types"
)

// GetBucketVersioningHandler returns the versioning state of a bucket.
// GET /{bucket}?versioning
func (s *MetadataServer) GetBucketVersioningHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	result, err := s.svc.Config().GetBucketVersioning(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get bucket versioning")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)
}

// PutBucketVersioningHandler sets the versioning state of a bucket.
// PUT /{bucket}?versioning
func (s *MetadataServer) PutBucketVersioningHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	bucket := d.S3Info.Bucket

	// Parse request body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	var req s3types.VersioningConfiguration
	if err := xml.Unmarshal(body, &req); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// MFA Delete not supported
	if req.MFADelete == "Enabled" {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	// Call service (validation is done in service)
	if err := s.svc.Config().SetBucketVersioning(d.Ctx, bucket, &req); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to update bucket versioning")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	logger.Info().
		Str("bucket", bucket).
		Str("versioning", req.Status).
		Msg("bucket versioning updated")
}

// ListObjectVersionsHandler lists all versions of objects in a bucket.
// GET /{bucket}?versions
// Note: This handler remains mostly unchanged as it's a complex list operation
// that should eventually be moved to object service, not config service.
func (s *MetadataServer) ListObjectVersionsHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket

	// Parse query parameters
	query := d.Req.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	keyMarker := query.Get("key-marker")
	versionIDMarker := query.Get("version-id-marker")
	maxKeysStr := query.Get("max-keys")

	maxKeys := 1000
	if maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 && mk <= 1000 {
			maxKeys = mk
		}
	}

	// List object versions from database
	versions, isTruncated, nextKeyMarker, nextVersionIDMarker, err := s.db.ListObjectVersions(
		ctx, bucket, prefix, keyMarker, versionIDMarker, delimiter, maxKeys,
	)
	if err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to list object versions")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Build response
	result := s3types.ListVersionsResult{
		Xmlns:               "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                bucket,
		Prefix:              prefix,
		KeyMarker:           keyMarker,
		VersionIDMarker:     versionIDMarker,
		NextKeyMarker:       nextKeyMarker,
		NextVersionIDMarker: nextVersionIDMarker,
		MaxKeys:             maxKeys,
		Delimiter:           delimiter,
		IsTruncated:         isTruncated,
	}

	// Handle delimiter for folder simulation
	seenPrefixes := make(map[string]bool)

	for _, v := range versions {
		// Check if this should be a common prefix
		if delimiter != "" && prefix != "" {
			afterPrefix := strings.TrimPrefix(v.Key, prefix)
			if idx := strings.Index(afterPrefix, delimiter); idx >= 0 {
				commonPrefix := prefix + afterPrefix[:idx+len(delimiter)]
				if !seenPrefixes[commonPrefix] {
					seenPrefixes[commonPrefix] = true
					result.CommonPrefixes = append(result.CommonPrefixes, s3types.CommonPrefix{
						Prefix: commonPrefix,
					})
				}
				continue
			}
		}

		if v.IsDeleteMarker {
			result.DeleteMarkers = append(result.DeleteMarkers, s3types.DeleteMarker{
				Key:          v.Key,
				VersionID:    v.VersionID,
				IsLatest:     v.IsLatest,
				LastModified: time.Unix(0, v.LastModified).UTC().Format(time.RFC3339),
				Owner: &s3types.Owner{
					ID:          d.S3Info.OwnerID,
					DisplayName: d.S3Info.OwnerID,
				},
			})
		} else {
			result.Versions = append(result.Versions, s3types.ObjectVersion{
				Key:          v.Key,
				VersionID:    v.VersionID,
				IsLatest:     v.IsLatest,
				LastModified: time.Unix(0, v.LastModified).UTC().Format(time.RFC3339),
				ETag:         "\"" + v.ETag + "\"",
				Size:         v.Size,
				StorageClass: "STANDARD",
				Owner: &s3types.Owner{
					ID:          d.S3Info.OwnerID,
					DisplayName: d.S3Info.OwnerID,
				},
			})
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ")
	encoder.Encode(result)
}
