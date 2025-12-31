//go:build !enterprise

package api

import (
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// GetObjectLockConfigurationHandler returns an error in community edition.
// GET /{bucket}?object-lock
//
// Enterprise feature: requires FeatureObjectLock license.
func (s *MetadataServer) GetObjectLockConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("object lock requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrObjectLockConfigurationNotFoundError)
}

// PutObjectLockConfigurationHandler returns an error in community edition.
// PUT /{bucket}?object-lock
//
// Enterprise feature: requires FeatureObjectLock license.
func (s *MetadataServer) PutObjectLockConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("object lock requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// GetObjectRetentionHandler returns an error in community edition.
// GET /{bucket}/{key}?retention
//
// Enterprise feature: requires FeatureObjectLock license.
func (s *MetadataServer) GetObjectRetentionHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("object lock requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNoSuchObjectLockConfiguration)
}

// PutObjectRetentionHandler returns an error in community edition.
// PUT /{bucket}/{key}?retention
//
// Enterprise feature: requires FeatureObjectLock license.
func (s *MetadataServer) PutObjectRetentionHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("object lock requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// GetObjectLegalHoldHandler returns an error in community edition.
// GET /{bucket}/{key}?legal-hold
//
// Enterprise feature: requires FeatureObjectLock license.
func (s *MetadataServer) GetObjectLegalHoldHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("object lock requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNoSuchObjectLockConfiguration)
}

// PutObjectLegalHoldHandler returns an error in community edition.
// PUT /{bucket}/{key}?legal-hold
//
// Enterprise feature: requires FeatureObjectLock license.
func (s *MetadataServer) PutObjectLegalHoldHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("object lock requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}
