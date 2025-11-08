//go:build !enterprise

package api

import (
	"net/http"

	"zapfs/pkg/logger"
	"zapfs/pkg/metadata/data"
	"zapfs/pkg/s3api/s3err"
)

// GetBucketReplicationHandler returns an error in community edition.
func (s *MetadataServer) GetBucketReplicationHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Msg("bucket replication requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// PutBucketReplicationHandler returns an error in community edition.
func (s *MetadataServer) PutBucketReplicationHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Msg("bucket replication requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// DeleteBucketReplicationHandler returns an error in community edition.
func (s *MetadataServer) DeleteBucketReplicationHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Msg("bucket replication requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}
