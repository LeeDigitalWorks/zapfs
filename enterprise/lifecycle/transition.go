// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE file.

//go:build enterprise

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"

	pkglicense "github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/storage"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

var (
	// ErrLicenseRequired indicates that a valid enterprise license is required
	ErrLicenseRequired = errors.New("storage transitions require enterprise license")

	// ErrNoBackendManager indicates the backend manager is not configured
	ErrNoBackendManager = errors.New("backend manager not configured")

	// ErrNoProfiles indicates the profiles are not configured
	ErrNoProfiles = errors.New("profiles not configured")

	// ErrProfileNotFound indicates the target storage class profile was not found
	ErrProfileNotFound = errors.New("profile not found for storage class")

	// ErrObjectModified indicates the object was modified since evaluation
	ErrObjectModified = errors.New("object modified since lifecycle evaluation")

	// ErrAlreadyTransitioned indicates the object is already at the target storage class
	ErrAlreadyTransitioned = errors.New("object already transitioned")
)

// TransitionDeps contains dependencies for transition execution.
// Transitions use the standard pools/profiles configuration - a profile
// named after the target storage class (e.g., "GLACIER") determines
// which backend to use. Profiles used for transitions should typically
// have read_only: true on their pools to prevent new object placement.
type TransitionDeps struct {
	DB             db.DB
	FileClient     client.File
	BackendManager *backend.Manager
	Profiles       *types.ProfileSet
	Pools          *types.PoolSet
	Coordinator    *storage.Coordinator // For promotion: writes data back to file servers
}

// ExecuteTransition performs the storage class transition for an object.
// It streams the object data from file servers to the tier backend,
// updates metadata, and decrements chunk reference counts.
//
// The target storage class must have a corresponding profile defined
// (e.g., storage class "GLACIER" requires a profile named "GLACIER").
func ExecuteTransition(ctx context.Context, deps *TransitionDeps, payload taskqueue.LifecyclePayload) error {
	// Validate dependencies
	if deps.BackendManager == nil {
		return ErrNoBackendManager
	}
	if deps.Profiles == nil {
		return ErrNoProfiles
	}

	// Check license using global license checker
	if !pkglicense.CheckLifecycle() {
		return ErrLicenseRequired
	}

	// Get object metadata
	obj, err := deps.DB.GetObject(ctx, payload.Bucket, payload.Key)
	if err != nil {
		if errors.Is(err, db.ErrObjectNotFound) {
			logger.Debug().
				Str("bucket", payload.Bucket).
				Str("key", payload.Key).
				Msg("Object not found, skipping transition")
			return nil
		}
		return fmt.Errorf("get object: %w", err)
	}

	// Skip if already at target storage class
	if obj.StorageClass == payload.StorageClass {
		logger.Debug().
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Str("storage_class", payload.StorageClass).
			Msg("Object already at target storage class")
		return nil
	}

	// Skip if already transitioned (has remote reference)
	if obj.TransitionedRef != "" {
		logger.Debug().
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Str("transitioned_ref", obj.TransitionedRef).
			Msg("Object already transitioned")
		return nil
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
				Msg("Object modified since evaluation, skipping transition")
			return nil
		}
	}

	// Look up the profile for the target storage class
	profile, ok := deps.Profiles.Get(payload.StorageClass)
	if !ok {
		return fmt.Errorf("%w: %s", ErrProfileNotFound, payload.StorageClass)
	}

	if len(profile.Pools) == 0 {
		return fmt.Errorf("profile %s has no pools configured", payload.StorageClass)
	}

	// Get the first pool's backend (for tier storage, typically one pool)
	poolTarget := profile.Pools[0]
	tierBackend, ok := deps.BackendManager.Get(poolTarget.PoolID.String())
	if !ok {
		return fmt.Errorf("backend not found for pool %s", poolTarget.PoolID)
	}

	// Generate remote key for tier storage
	// Format: ab/cd/uuid (first 4 chars of UUID split for distribution)
	remoteKey := generateRemoteKey(obj.ID)

	// Stream object to tier backend
	if err := streamToTier(ctx, deps.FileClient, obj, tierBackend, remoteKey); err != nil {
		return fmt.Errorf("stream to tier: %w", err)
	}

	// Update object metadata atomically
	now := time.Now().UnixNano()
	if err := deps.DB.UpdateObjectTransition(ctx, obj.ID.String(), payload.StorageClass, now, remoteKey); err != nil {
		// Attempt to clean up the tier object on failure
		cleanupErr := tierBackend.Delete(ctx, remoteKey)
		if cleanupErr != nil {
			logger.Warn().
				Err(cleanupErr).
				Str("remote_key", remoteKey).
				Msg("Failed to cleanup tier object after metadata update failure")
		}
		return fmt.Errorf("update object metadata: %w", err)
	}

	// Record metrics
	TransitionsTotal.WithLabelValues(payload.StorageClass, "success").Inc()
	TransitionBytesTotal.WithLabelValues(payload.StorageClass).Add(float64(obj.Size))

	logger.Info().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Str("storage_class", payload.StorageClass).
		Str("remote_key", remoteKey).
		Uint64("size", obj.Size).
		Str("rule_id", payload.RuleID).
		Msg("Lifecycle: transitioned object to tier storage")

	return nil
}

// generateRemoteKey creates a distributed key path for tier storage
// Format: ab/cd/uuid where ab and cd are first 4 chars of the UUID
func generateRemoteKey(id uuid.UUID) string {
	uuidStr := strings.ReplaceAll(id.String(), "-", "")
	return fmt.Sprintf("%s/%s/%s", uuidStr[:2], uuidStr[2:4], id.String())
}

// streamToTier streams object data from file servers to the tier backend
func streamToTier(ctx context.Context, fileClient client.File, obj *types.ObjectRef, tierBackend types.BackendStorage, remoteKey string) error {
	if len(obj.ChunkRefs) == 0 {
		// Empty object or no chunks - write empty data
		return tierBackend.Write(ctx, remoteKey, strings.NewReader(""), 0)
	}

	// Create a pipe to stream data from file servers to tier backend
	pr, pw := io.Pipe()

	// Channel for error from writer goroutine
	errChan := make(chan error, 1)

	// Writer goroutine: reads chunks from file servers and writes to pipe
	go func() {
		defer pw.Close()

		for _, chunk := range obj.ChunkRefs {
			if chunk.FileServerAddr == "" {
				errChan <- fmt.Errorf("chunk %s has no file server address", chunk.ChunkID)
				return
			}

			// Read chunk from file server
			err := fileClient.GetChunk(ctx, chunk.FileServerAddr, chunk.ChunkID.String(), func(data []byte) error {
				_, writeErr := pw.Write(data)
				return writeErr
			})
			if err != nil {
				errChan <- fmt.Errorf("read chunk %s: %w", chunk.ChunkID, err)
				return
			}
		}

		errChan <- nil
	}()

	// Write to tier backend (this reads from the pipe)
	writeErr := tierBackend.Write(ctx, remoteKey, pr, int64(obj.Size))

	// Wait for reader goroutine to complete
	readErr := <-errChan

	// Close the pipe reader to signal completion
	pr.Close()

	// Return first error encountered
	if readErr != nil {
		return readErr
	}
	return writeErr
}

// ExecutePromotion moves an object from tier storage back to hot storage.
// This is triggered when an INTELLIGENT_TIERING object that was demoted to
// cold tier storage is accessed - it should be promoted back to hot storage.
//
// The process:
// 1. Read data from tier backend using TransitionedRef
// 2. Write data back to file servers using Coordinator.WriteObject
// 3. Update object metadata (new ChunkRefs, clear TransitionedRef)
// 4. Delete the tier copy
func ExecutePromotion(ctx context.Context, deps *TransitionDeps, payload taskqueue.LifecyclePayload) error {
	// Check license
	if !pkglicense.CheckLifecycle() {
		return ErrLicenseRequired
	}

	// Validate dependencies
	if deps.Coordinator == nil {
		logger.Warn().
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Msg("Coordinator not configured, skipping promotion")
		return nil
	}
	if deps.BackendManager == nil {
		return ErrNoBackendManager
	}

	// Get object metadata
	obj, err := deps.DB.GetObject(ctx, payload.Bucket, payload.Key)
	if err != nil {
		if errors.Is(err, db.ErrObjectNotFound) {
			logger.Debug().
				Str("bucket", payload.Bucket).
				Str("key", payload.Key).
				Msg("Object not found, skipping promotion")
			return nil
		}
		return fmt.Errorf("get object: %w", err)
	}

	// Skip if not transitioned (already in hot storage)
	if obj.TransitionedRef == "" {
		logger.Debug().
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Msg("Object not transitioned, skipping promotion")
		return nil
	}

	// Skip if not INTELLIGENT_TIERING
	if obj.StorageClass != "INTELLIGENT_TIERING" {
		logger.Debug().
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Str("storage_class", obj.StorageClass).
			Msg("Object not INTELLIGENT_TIERING, skipping promotion")
		return nil
	}

	// Find the tier backend that holds this object
	remoteKey := obj.TransitionedRef
	var tierBackend types.BackendStorage

	// Iterate through backends to find the one with this object
	for _, id := range deps.BackendManager.List() {
		b, ok := deps.BackendManager.Get(id)
		if !ok {
			continue
		}
		exists, _ := b.Exists(ctx, remoteKey)
		if exists {
			tierBackend = b
			break
		}
	}

	if tierBackend == nil {
		logger.Warn().
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Str("transitioned_ref", obj.TransitionedRef).
			Msg("Tier backend not found for transitioned object, clearing transition ref")
		// Clear the transitioned ref since we can't find the data
		_ = deps.DB.UpdateObjectTransition(ctx, obj.ID.String(), "INTELLIGENT_TIERING", 0, "")
		return nil
	}

	// Read from tier backend
	reader, err := tierBackend.Read(ctx, remoteKey)
	if err != nil {
		return fmt.Errorf("read from tier backend: %w", err)
	}
	defer reader.Close()

	// Write back to file servers using Coordinator
	writeResult, err := deps.Coordinator.WriteObject(ctx, &storage.WriteRequest{
		Bucket:   payload.Bucket,
		ObjectID: obj.ID.String(),
		Body:     reader,
		Size:     obj.Size,
	})
	if err != nil {
		return fmt.Errorf("write to file servers: %w", err)
	}

	// Update object metadata atomically - clear transitioned ref and update chunk refs
	now := time.Now().UnixNano()
	err = deps.DB.WithTx(ctx, func(tx db.TxStore) error {
		// Get fresh copy in transaction
		txObj, err := tx.GetObject(ctx, payload.Bucket, payload.Key)
		if err != nil {
			return err
		}

		// Verify object hasn't changed (still same version)
		if txObj.ID != obj.ID {
			return fmt.Errorf("object changed during promotion")
		}

		// Update object with new chunk refs and clear transitioned ref
		txObj.ChunkRefs = writeResult.ChunkRefs
		txObj.TransitionedRef = ""
		txObj.TransitionedAt = 0
		txObj.LastAccessedAt = now

		return tx.PutObject(ctx, txObj)
	})
	if err != nil {
		return fmt.Errorf("update object metadata: %w", err)
	}

	// Clean up tier storage (best effort)
	if err := tierBackend.Delete(ctx, remoteKey); err != nil {
		logger.Warn().
			Err(err).
			Str("remote_key", remoteKey).
			Msg("Failed to delete tier copy after promotion")
	}

	// Record metrics
	PromotionsTotal.WithLabelValues("success").Inc()
	PromotionBytesTotal.Add(float64(obj.Size))

	logger.Info().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Uint64("size", obj.Size).
		Int("chunks", len(writeResult.ChunkRefs)).
		Msg("Lifecycle: promoted object back to hot storage")

	return nil
}
