//go:build !enterprise

package api

import (
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// GetBucketLoggingHandler returns empty logging status in community edition.
// GET /{bucket}?logging
//
// Enterprise feature: requires FeatureAuditLog license.
// Community edition returns empty BucketLoggingStatus (logging not configured).
func (s *MetadataServer) GetBucketLoggingHandler(d *data.Data, w http.ResponseWriter) {
	// Return empty logging status - this is valid S3 behavior indicating logging is disabled
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></BucketLoggingStatus>`))
}

// PutBucketLoggingHandler returns an error in community edition.
// PUT /{bucket}?logging
//
// Enterprise feature: requires FeatureAuditLog license.
func (s *MetadataServer) PutBucketLoggingHandler(d *data.Data, w http.ResponseWriter) {
	logger.Warn().Str("bucket", d.S3Info.Bucket).Msg("bucket logging requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}
