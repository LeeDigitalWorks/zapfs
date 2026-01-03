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

	// Decrement old chunk ref counts (GC will clean up when refs reach 0)
	if len(obj.ChunkRefs) > 0 && deps.FileClient != nil {
		for _, chunk := range obj.ChunkRefs {
			if chunk.FileServerAddr != "" {
				_, err := deps.FileClient.DecrementRefCount(ctx, chunk.FileServerAddr, chunk.ChunkID.String(), 0)
				if err != nil {
					// Log but don't fail - chunks will be cleaned up by GC
					logger.Warn().
						Err(err).
						Str("chunk_id", chunk.ChunkID.String()).
						Str("address", chunk.FileServerAddr).
						Msg("Failed to decrement chunk ref count during transition")
				}
			}
		}
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
