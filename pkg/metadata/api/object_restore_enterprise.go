//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package api

import (
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// RestoreObjectHandler restores an object from archive storage class.
// POST /{bucket}/{key}?restore
//
// Enterprise feature: requires FeatureLifecycle license.
// Used to restore objects from archive storage classes (Glacier, Deep Archive).
func (s *MetadataServer) RestoreObjectHandler(d *data.Data, w http.ResponseWriter) {
	if !checkLifecycleLicense() {
		logger.Warn().Str("bucket", d.S3Info.Bucket).Str("key", d.S3Info.Key).Msg("object restore feature requires enterprise license")
		writeXMLErrorResponse(w, d, s3err.ErrAccessDenied)
		return
	}

	// TODO: Implement restore from archive
	// Implementation steps:
	// 1. Verify object exists and is in archive storage class (GLACIER, DEEP_ARCHIVE)
	// 2. Parse RestoreRequest XML from body:
	//    - Days: number of days to keep restored copy
	//    - GlacierJobParameters.Tier: Expedited, Standard, or Bulk
	// 3. Check if restoration already in progress (return 409 if so)
	// 4. Queue restoration job via task queue
	// 5. Return 202 Accepted
	// 6. When restore completes, set x-amz-restore header on object:
	//    - ongoing-request="true" while in progress
	//    - ongoing-request="false", expiry-date="..." when complete
	// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html

	// No archive tier implemented yet
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}
