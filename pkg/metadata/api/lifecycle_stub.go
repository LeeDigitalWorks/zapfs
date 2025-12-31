//go:build !enterprise

package api

import (
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// GetBucketLifecycleConfigurationHandler returns an error in community edition.
// GET /{bucket}?lifecycle
//
// Enterprise feature: requires FeatureLifecycle license.
func (s *MetadataServer) GetBucketLifecycleConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("lifecycle requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// PutBucketLifecycleConfigurationHandler returns an error in community edition.
// PUT /{bucket}?lifecycle
//
// Enterprise feature: requires FeatureLifecycle license.
func (s *MetadataServer) PutBucketLifecycleConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("lifecycle requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// DeleteBucketLifecycleHandler returns an error in community edition.
// DELETE /{bucket}?lifecycle
//
// Enterprise feature: requires FeatureLifecycle license.
func (s *MetadataServer) DeleteBucketLifecycleHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("lifecycle requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}
