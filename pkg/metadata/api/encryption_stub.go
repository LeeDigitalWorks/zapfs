//go:build !enterprise

package api

import (
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// GetBucketEncryptionHandler returns an error in community edition.
// GET /{bucket}?encryption
//
// Enterprise feature: requires FeatureKMS license for KMS encryption.
// Note: AES256 (SSE-S3) could be community, but KMS requires enterprise.
func (s *MetadataServer) GetBucketEncryptionHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("bucket encryption requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNoSuchBucketEncryptionConfiguration)
}

// PutBucketEncryptionHandler returns an error in community edition.
// PUT /{bucket}?encryption
//
// Enterprise feature: requires FeatureKMS license for KMS encryption.
func (s *MetadataServer) PutBucketEncryptionHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("bucket encryption requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// DeleteBucketEncryptionHandler returns an error in community edition.
// DELETE /{bucket}?encryption
//
// Enterprise feature: requires FeatureKMS license.
func (s *MetadataServer) DeleteBucketEncryptionHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("bucket encryption requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}
