// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package handlers provides task handlers for the task queue.
package handlers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/lifecycle"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

// LifecycleHandler handles lifecycle task execution
type LifecycleHandler struct {
	db         db.DB
	fileClient client.File
}

// NewLifecycleHandler creates a new lifecycle handler
func NewLifecycleHandler(database db.DB, fileClient client.File) *LifecycleHandler {
	return &LifecycleHandler{
		db:         database,
		fileClient: fileClient,
	}
}

// Type returns the task type this handler processes
func (h *LifecycleHandler) Type() taskqueue.TaskType {
	return taskqueue.TaskTypeLifecycle
}

// Handle processes a lifecycle task
func (h *LifecycleHandler) Handle(ctx context.Context, task *taskqueue.Task) error {
	payload, err := taskqueue.UnmarshalPayload[taskqueue.LifecyclePayload](task.Payload)
	if err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	logger.Debug().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Str("action", payload.Action).
		Str("rule_id", payload.RuleID).
		Msg("Processing lifecycle action")

	var handleErr error
	switch payload.Action {
	case taskqueue.LifecycleActionDelete:
		handleErr = h.handleExpiration(ctx, payload)
	case taskqueue.LifecycleActionDeleteVersion:
		handleErr = h.handleVersionExpiration(ctx, payload)
	case taskqueue.LifecycleActionAbortMPU:
		handleErr = h.handleAbortMultipart(ctx, payload)
	case taskqueue.LifecycleActionTransition:
		handleErr = h.handleTransition(ctx, payload)
	default:
		return fmt.Errorf("unknown action: %s", payload.Action)
	}

	if handleErr != nil {
		lifecycle.ActionsExecuted.WithLabelValues(payload.Action, "failed").Inc()
		return handleErr
	}

	lifecycle.ActionsExecuted.WithLabelValues(payload.Action, "success").Inc()
	return nil
}

// handleExpiration deletes the current version of an object
func (h *LifecycleHandler) handleExpiration(ctx context.Context, payload taskqueue.LifecyclePayload) error {
	// Get object to verify it still exists and hasn't changed
	obj, err := h.db.GetObject(ctx, payload.Bucket, payload.Key)
	if err != nil {
		if errors.Is(err, db.ErrObjectNotFound) {
			// Already deleted - success
			logger.Debug().
				Str("bucket", payload.Bucket).
				Str("key", payload.Key).
				Msg("Object already deleted")
			return nil
		}
		return fmt.Errorf("get object: %w", err)
	}

	// Verify object hasn't been modified since evaluation
	if payload.ExpectedModTime > 0 {
		objModTime := time.Unix(0, obj.CreatedAt)
		expectedModTime := time.Unix(0, payload.ExpectedModTime)

		// Allow 1 second tolerance for clock skew
		if objModTime.Sub(expectedModTime).Abs() > time.Second {
			logger.Info().
				Str("bucket", payload.Bucket).
				Str("key", payload.Key).
				Time("expected", expectedModTime).
				Time("actual", objModTime).
				Msg("Object modified since evaluation, skipping")
			return nil
		}
	}

	// Soft-delete the object (mark as deleted)
	if err := h.db.DeleteObject(ctx, payload.Bucket, payload.Key); err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	// Decrement chunk ref counts via file service
	if len(obj.ChunkRefs) > 0 && h.fileClient != nil {
		for _, chunk := range obj.ChunkRefs {
			if chunk.FileServerAddr != "" {
				_, err := h.fileClient.DecrementRefCount(ctx, chunk.FileServerAddr, chunk.ChunkID.String(), 0)
				if err != nil {
					// Log but don't fail - chunks will be cleaned up by GC
					logger.Warn().
						Err(err).
						Str("chunk_id", chunk.ChunkID.String()).
						Str("address", chunk.FileServerAddr).
						Msg("Failed to decrement chunk ref count")
				}
			}
		}
	}

	lifecycle.BytesExpired.Add(float64(obj.Size))

	logger.Info().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Uint64("size", obj.Size).
		Str("rule_id", payload.RuleID).
		Msg("Lifecycle: deleted object")

	return nil
}

// handleVersionExpiration deletes a specific version of an object
func (h *LifecycleHandler) handleVersionExpiration(ctx context.Context, payload taskqueue.LifecyclePayload) error {
	if payload.VersionID == "" {
		return fmt.Errorf("version_id required for delete_version action")
	}

	// Get the specific version
	obj, err := h.db.GetObjectVersion(ctx, payload.Bucket, payload.Key, payload.VersionID)
	if err != nil {
		if errors.Is(err, db.ErrObjectNotFound) {
			// Already deleted - success
			logger.Debug().
				Str("bucket", payload.Bucket).
				Str("key", payload.Key).
				Str("version", payload.VersionID).
				Msg("Version already deleted")
			return nil
		}
		return fmt.Errorf("get object version: %w", err)
	}

	// Delete the specific version
	if err := h.db.DeleteObjectVersion(ctx, payload.Bucket, payload.Key, payload.VersionID); err != nil {
		return fmt.Errorf("delete object version: %w", err)
	}

	// Decrement chunk ref counts
	if len(obj.ChunkRefs) > 0 && h.fileClient != nil {
		for _, chunk := range obj.ChunkRefs {
			if chunk.FileServerAddr != "" {
				_, err := h.fileClient.DecrementRefCount(ctx, chunk.FileServerAddr, chunk.ChunkID.String(), 0)
				if err != nil {
					logger.Warn().
						Err(err).
						Str("chunk_id", chunk.ChunkID.String()).
						Msg("Failed to decrement chunk ref count")
				}
			}
		}
	}

	lifecycle.BytesExpired.Add(float64(obj.Size))

	logger.Info().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Str("version", payload.VersionID).
		Uint64("size", obj.Size).
		Str("rule_id", payload.RuleID).
		Msg("Lifecycle: deleted object version")

	return nil
}

// handleAbortMultipart aborts an incomplete multipart upload
func (h *LifecycleHandler) handleAbortMultipart(ctx context.Context, payload taskqueue.LifecyclePayload) error {
	if payload.UploadID == "" {
		return fmt.Errorf("upload_id required for abort_mpu action")
	}

	// Get the multipart upload
	upload, err := h.db.GetMultipartUpload(ctx, payload.Bucket, payload.Key, payload.UploadID)
	if err != nil {
		if errors.Is(err, db.ErrObjectNotFound) {
			// Already completed or aborted
			logger.Debug().
				Str("bucket", payload.Bucket).
				Str("key", payload.Key).
				Str("upload_id", payload.UploadID).
				Msg("Multipart upload already aborted/completed")
			return nil
		}
		return fmt.Errorf("get multipart upload: %w", err)
	}

	// Get and clean up any uploaded parts
	parts, _, err := h.db.ListParts(ctx, payload.UploadID, 0, 10000)
	if err != nil {
		logger.Warn().Err(err).Str("upload_id", payload.UploadID).Msg("Failed to list parts")
	} else {
		// Decrement ref counts for uploaded chunks
		for _, part := range parts {
			for _, chunk := range part.ChunkRefs {
				if chunk.FileServerAddr != "" && h.fileClient != nil {
					_, err := h.fileClient.DecrementRefCount(ctx, chunk.FileServerAddr, chunk.ChunkID.String(), 0)
					if err != nil {
						logger.Warn().
							Err(err).
							Str("chunk_id", chunk.ChunkID.String()).
							Msg("Failed to decrement part chunk ref count")
					}
				}
			}
		}
	}

	// Delete the upload record and parts
	if err := h.db.DeleteMultipartUpload(ctx, payload.Bucket, payload.Key, payload.UploadID); err != nil {
		return fmt.Errorf("delete multipart upload: %w", err)
	}

	logger.Info().
		Str("bucket", upload.Bucket).
		Str("key", upload.Key).
		Str("upload_id", payload.UploadID).
		Str("rule_id", payload.RuleID).
		Msg("Lifecycle: aborted incomplete multipart upload")

	return nil
}

// handleTransition transitions an object to a different storage class
// This is a stub in community edition - enterprise edition provides the full implementation
func (h *LifecycleHandler) handleTransition(_ context.Context, payload taskqueue.LifecyclePayload) error {
	// Community edition: log warning and skip
	logger.Warn().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Str("storage_class", payload.StorageClass).
		Msg("Storage transitions require enterprise edition, skipping")
	return nil
}
