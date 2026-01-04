// Copyright 2025 ZapFS Authors. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the LICENSE file.

package api

import (
	"encoding/xml"
	"io"
	"net/http"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/rs/zerolog/log"
)

// RestoreRequest is the XML body for POST /{bucket}/{key}?restore
type RestoreRequest struct {
	XMLName              xml.Name              `xml:"RestoreRequest"`
	Days                 int                   `xml:"Days"`
	GlacierJobParameters *GlacierJobParameters `xml:"GlacierJobParameters,omitempty"`
}

// GlacierJobParameters specifies the retrieval tier for the restore
type GlacierJobParameters struct {
	Tier string `xml:"Tier"` // Expedited, Standard, or Bulk
}

// RestoreObjectHandler restores an object from archive storage class.
// POST /{bucket}/{key}?restore
//
// Requires FeatureLifecycle license.
// Used to restore objects from archive storage classes (Glacier, Deep Archive).
func (s *MetadataServer) RestoreObjectHandler(d *data.Data, w http.ResponseWriter) {
	if !license.CheckLifecycle() {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Get the object to verify it exists and check storage class
	obj, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		if err == db.ErrObjectNotFound {
			writeXMLErrorResponse(w, d, s3err.ErrNoSuchKey)
			return
		}
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("Failed to get object")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Check if object is in an archive storage class
	if !isArchiveStorageClass(obj.StorageClass) {
		writeXMLErrorResponse(w, d, s3err.ErrObjectNotInArchiveTier)
		return
	}

	// Check if restore is already in progress
	if obj.RestoreStatus == "pending" || obj.RestoreStatus == "in_progress" {
		writeXMLErrorResponse(w, d, s3err.ErrRestoreAlreadyInProgress)
		return
	}

	// If restore is already completed and not expired, return 200 OK
	if obj.RestoreStatus == "completed" && obj.RestoreExpiryDate > time.Now().UnixNano() {
		// Restore is already active - could optionally extend the expiry
		// For now, just return 200 OK to indicate restoration exists
		w.WriteHeader(http.StatusOK)
		return
	}

	// Parse the restore request
	body, err := io.ReadAll(io.LimitReader(d.Req.Body, 1024*1024)) // 1MB limit
	if err != nil {
		log.Error().Err(err).Msg("Failed to read restore request body")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	var restoreReq RestoreRequest
	if len(body) > 0 {
		if err := xml.Unmarshal(body, &restoreReq); err != nil {
			writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
			return
		}
	}

	// Validate request
	if restoreReq.Days <= 0 {
		restoreReq.Days = 1 // Default to 1 day
	}
	if restoreReq.Days > 30 {
		restoreReq.Days = 30 // Cap at 30 days
	}

	tier := taskqueue.RestoreTierStandard // Default tier
	if restoreReq.GlacierJobParameters != nil {
		switch restoreReq.GlacierJobParameters.Tier {
		case "Expedited":
			tier = taskqueue.RestoreTierExpedited
		case "Standard":
			tier = taskqueue.RestoreTierStandard
		case "Bulk":
			tier = taskqueue.RestoreTierBulk
		default:
			if restoreReq.GlacierJobParameters.Tier != "" {
				writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
				return
			}
		}
	}

	// Expedited tier not available for DEEP_ARCHIVE
	if obj.StorageClass == "DEEP_ARCHIVE" && tier == taskqueue.RestoreTierExpedited {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	requestedAt := time.Now().UnixNano()

	// Update object status to pending
	if err := s.db.UpdateRestoreStatus(ctx, obj.ID.String(), "pending", tier, requestedAt); err != nil {
		log.Error().Err(err).Str("object_id", obj.ID.String()).Msg("Failed to update restore status")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Enqueue restore task if task queue is available
	if s.taskQueue != nil {
		payload := taskqueue.RestorePayload{
			ObjectID:        obj.ID.String(),
			Bucket:          bucket,
			Key:             key,
			Days:            restoreReq.Days,
			Tier:            tier,
			StorageClass:    obj.StorageClass,
			TransitionedRef: obj.TransitionedRef,
			RequestedAt:     requestedAt,
		}

		payloadJSON, err := taskqueue.MarshalPayload(payload)
		if err != nil {
			log.Error().Err(err).Msg("Failed to marshal restore payload")
			// Status is already set to pending, so just log the error
			// The restore handler will pick it up eventually
		} else {
			task := &taskqueue.Task{
				Type:        taskqueue.TaskTypeRestore,
				Payload:     payloadJSON,
				Priority:    taskqueue.PriorityNormal,
				ScheduledAt: time.Now(),
				MaxRetries:  3,
			}

			if err := s.taskQueue.Enqueue(ctx, task); err != nil {
				log.Error().Err(err).Msg("Failed to enqueue restore task")
				// Status is already set to pending, continue
			}
		}
	}

	// Return 202 Accepted to indicate the restore is in progress
	w.WriteHeader(http.StatusAccepted)
}

// isArchiveStorageClass checks if a storage class is an archive tier
func isArchiveStorageClass(storageClass string) bool {
	return storageClass == "GLACIER" ||
		storageClass == "GLACIER_IR" ||
		storageClass == "DEEP_ARCHIVE"
}
