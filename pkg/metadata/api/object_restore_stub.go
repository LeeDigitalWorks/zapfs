//go:build !enterprise

package api

import (
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// RestoreObjectHandler returns an error in community edition.
// POST /{bucket}/{key}?restore
//
// Enterprise feature: requires FeatureLifecycle license.
// Used to restore objects from archive storage classes (Glacier, Deep Archive).
func (s *MetadataServer) RestoreObjectHandler(d *data.Data, w http.ResponseWriter) {
	// TODO: Enterprise implementation in object_restore_enterprise.go
	// - Verify object exists and is in archive storage class
	// - Parse RestoreRequest XML (Days, GlacierJobParameters)
	// - Queue restoration job with specified tier (Expedited, Standard, Bulk)
	// - Return 202 Accepted with x-amz-restore header
	// - Track restoration progress and update x-amz-restore header
	logger.Warn().Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("restore requires enterprise edition")
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}
