//go:build !enterprise

package api

import (
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// GetBucketIntelligentTieringConfigurationHandler returns an error in community edition.
// GET /{bucket}?intelligent-tiering&id={id}
//
// Enterprise feature: requires FeatureLifecycle license.
func (s *MetadataServer) GetBucketIntelligentTieringConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// TODO: Enterprise implementation in intelligent_tiering_enterprise.go
	// - Load configuration by ID from bucket metadata
	// - Return IntelligentTieringConfiguration with AccessTier settings
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("intelligent tiering requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNoSuchConfiguration)
}

// PutBucketIntelligentTieringConfigurationHandler returns an error in community edition.
// PUT /{bucket}?intelligent-tiering&id={id}
//
// Enterprise feature: requires FeatureLifecycle license.
func (s *MetadataServer) PutBucketIntelligentTieringConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// TODO: Enterprise implementation in intelligent_tiering_enterprise.go
	// - Parse IntelligentTieringConfiguration XML
	// - Validate AccessTier settings (ARCHIVE_ACCESS, DEEP_ARCHIVE_ACCESS)
	// - Store configuration in bucket metadata
	// - Set up automatic tiering based on access patterns
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("intelligent tiering requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// DeleteBucketIntelligentTieringConfigurationHandler is a no-op in community edition.
// DELETE /{bucket}?intelligent-tiering&id={id}
//
// Enterprise feature: requires FeatureLifecycle license.
func (s *MetadataServer) DeleteBucketIntelligentTieringConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// No-op - nothing to delete in community edition
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)
}

// ListBucketIntelligentTieringConfigurationsHandler returns empty list in community edition.
// GET /{bucket}?intelligent-tiering
//
// Enterprise feature: requires FeatureLifecycle license.
func (s *MetadataServer) ListBucketIntelligentTieringConfigurationsHandler(d *data.Data, w http.ResponseWriter) {
	// Return empty list - no configurations in community edition
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketIntelligentTieringConfigurationsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated></ListBucketIntelligentTieringConfigurationsResult>`))
}
