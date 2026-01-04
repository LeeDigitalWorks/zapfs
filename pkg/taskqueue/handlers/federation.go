// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/federation"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for federation handlers
var (
	federationIngestHandledTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_ingest_handled_total",
			Help: "Total number of federation ingest tasks handled",
		},
		[]string{"bucket", "status"},
	)
	federationIngestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "zapfs_federation_ingest_duration_seconds",
			Help:    "Duration of federation ingest task handling in seconds",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300},
		},
		[]string{"bucket"},
	)
	federationDiscoveryHandledTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_discovery_handled_total",
			Help: "Total number of federation discovery tasks handled",
		},
		[]string{"bucket", "status"},
	)
	federationDiscoveryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "zapfs_federation_discovery_task_duration_seconds",
			Help:    "Duration of federation discovery task handling in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"bucket"},
	)
)

// ObjectWriter handles writing object data to local storage.
// This abstracts the storage coordinator for the federation handler.
type ObjectWriter interface {
	// WriteObject writes object data to local storage and returns chunk references.
	WriteObject(ctx context.Context, bucket, key string, body io.Reader, size int64, contentType string) (*types.ObjectRef, error)
}

// FederationIngestHandler handles federation ingest tasks.
// It fetches objects from external S3 and stores them in local storage.
type FederationIngestHandler struct {
	db           db.DB
	clientPool   *federation.ClientPool
	objectWriter ObjectWriter
}

// NewFederationIngestHandler creates a new federation ingest handler.
func NewFederationIngestHandler(database db.DB, clientPool *federation.ClientPool, objectWriter ObjectWriter) *FederationIngestHandler {
	return &FederationIngestHandler{
		db:           database,
		clientPool:   clientPool,
		objectWriter: objectWriter,
	}
}

// Type returns the task type this handler processes.
func (h *FederationIngestHandler) Type() taskqueue.TaskType {
	return taskqueue.TaskTypeFederationIngest
}

// Handle processes a federation ingest task.
func (h *FederationIngestHandler) Handle(ctx context.Context, task *taskqueue.Task) error {
	startTime := time.Now()

	payload, err := taskqueue.UnmarshalPayload[taskqueue.FederationIngestPayload](task.Payload)
	if err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	logger.Debug().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Str("version_id", payload.VersionID).
		Int64("size", payload.Size).
		Msg("processing federation ingest task")

	// Get federation config for this bucket
	fedConfig, err := h.db.GetFederationConfig(ctx, payload.Bucket)
	if err != nil {
		federationIngestHandledTotal.WithLabelValues(payload.Bucket, "error").Inc()
		return fmt.Errorf("get federation config: %w", err)
	}

	// Check if object already exists locally with chunks
	existingObj, err := h.db.GetObject(ctx, payload.Bucket, payload.Key)
	if err == nil && existingObj != nil && len(existingObj.ChunkRefs) > 0 {
		// Object already ingested - skip
		logger.Debug().
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Msg("object already ingested, skipping")
		federationIngestHandledTotal.WithLabelValues(payload.Bucket, "skipped").Inc()
		return nil
	}

	// Fetch object from external S3
	extCfg := &federation.ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}

	var output *federation.FetchResult
	if payload.VersionID != "" {
		// Fetch specific version
		ingestor := federation.NewLazyIngestor(h.clientPool, nil) // No task queue needed here
		output, err = ingestor.FetchWithVersion(ctx, fedConfig, payload.Key, payload.VersionID)
	} else {
		ingestor := federation.NewLazyIngestor(h.clientPool, nil)
		output, err = ingestor.FetchAndQueue(ctx, fedConfig, payload.Key)
	}

	if err != nil {
		federationIngestHandledTotal.WithLabelValues(payload.Bucket, "error").Inc()
		return fmt.Errorf("fetch from external S3: %w", err)
	}
	defer output.Body.Close()

	// Write object to local storage
	objRef, err := h.objectWriter.WriteObject(ctx, payload.Bucket, payload.Key, output.Body, output.ContentLength, output.ContentType)
	if err != nil {
		federationIngestHandledTotal.WithLabelValues(payload.Bucket, "error").Inc()
		return fmt.Errorf("write object to local storage: %w", err)
	}

	// Update object metadata with ETag from external S3
	objRef.ETag = payload.ETag
	objRef.StorageClass = payload.StorageClass

	// Keep TransitionedRef for reference (shows where it came from)
	objRef.TransitionedRef = fmt.Sprintf("%s/%s", extCfg.Bucket, payload.Key)

	// Save updated object metadata
	if err := h.db.PutObject(ctx, objRef); err != nil {
		federationIngestHandledTotal.WithLabelValues(payload.Bucket, "error").Inc()
		return fmt.Errorf("update object metadata: %w", err)
	}

	// Update migration progress
	if err := h.db.UpdateMigrationProgress(ctx, payload.Bucket, 1, payload.Size, payload.Key); err != nil {
		logger.Warn().
			Err(err).
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Msg("failed to update migration progress")
	}

	federationIngestHandledTotal.WithLabelValues(payload.Bucket, "success").Inc()
	federationIngestDuration.WithLabelValues(payload.Bucket).Observe(time.Since(startTime).Seconds())

	logger.Debug().
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Int64("size", payload.Size).
		Dur("duration", time.Since(startTime)).
		Msg("federation ingest task completed")

	return nil
}

// FederationDiscoveryHandler handles federation discovery tasks.
// It lists objects in external S3 and creates ingest tasks for them.
type FederationDiscoveryHandler struct {
	db         db.DB
	clientPool *federation.ClientPool
	taskQueue  taskqueue.Queue
}

// NewFederationDiscoveryHandler creates a new federation discovery handler.
func NewFederationDiscoveryHandler(database db.DB, clientPool *federation.ClientPool, taskQueue taskqueue.Queue) *FederationDiscoveryHandler {
	return &FederationDiscoveryHandler{
		db:         database,
		clientPool: clientPool,
		taskQueue:  taskQueue,
	}
}

// Type returns the task type this handler processes.
func (h *FederationDiscoveryHandler) Type() taskqueue.TaskType {
	return taskqueue.TaskTypeFederationDiscovery
}

// Handle processes a federation discovery task.
func (h *FederationDiscoveryHandler) Handle(ctx context.Context, task *taskqueue.Task) error {
	startTime := time.Now()

	payload, err := taskqueue.UnmarshalPayload[taskqueue.FederationDiscoveryPayload](task.Payload)
	if err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	logger.Debug().
		Str("bucket", payload.Bucket).
		Str("start_after", payload.StartAfter).
		Int("batch_size", payload.BatchSize).
		Bool("include_versions", payload.IncludeVersions).
		Msg("processing federation discovery task")

	// Get federation config for this bucket
	fedConfig, err := h.db.GetFederationConfig(ctx, payload.Bucket)
	if err != nil {
		federationDiscoveryHandledTotal.WithLabelValues(payload.Bucket, "error").Inc()
		return fmt.Errorf("get federation config: %w", err)
	}

	// Check if migration is paused
	if fedConfig.MigrationPaused {
		logger.Debug().
			Str("bucket", payload.Bucket).
			Msg("migration paused, skipping discovery")
		federationDiscoveryHandledTotal.WithLabelValues(payload.Bucket, "paused").Inc()
		return nil
	}

	// Create discoverer
	discoverer := federation.NewDiscoverer(h.clientPool, h.taskQueue, federation.DiscoveryConfig{
		BatchSize:       payload.BatchSize,
		IncludeVersions: payload.IncludeVersions,
		Prefix:          payload.Prefix,
	})

	// Discover batch
	result, err := discoverer.DiscoverBatch(ctx, fedConfig, payload.StartAfter)
	if err != nil {
		federationDiscoveryHandledTotal.WithLabelValues(payload.Bucket, "error").Inc()
		return fmt.Errorf("discover batch: %w", err)
	}

	// Create metadata entries for discovered objects
	for _, obj := range result.Objects {
		if obj.IsDeleteMarker {
			continue // Skip delete markers
		}

		// Check if object already exists
		existing, err := h.db.GetObject(ctx, payload.Bucket, obj.Key)
		if err == nil && existing != nil {
			continue // Object already exists
		}

		// Create object metadata with TransitionedRef pointing to external S3
		objRef := &types.ObjectRef{
			ID:              uuid.New(),
			Bucket:          payload.Bucket,
			Key:             obj.Key,
			Size:            uint64(obj.Size),
			ETag:            obj.ETag,
			StorageClass:    obj.StorageClass,
			CreatedAt:       obj.LastModified.UnixNano(),
			TransitionedRef: fmt.Sprintf("%s/%s", fedConfig.ExternalBucket, obj.Key),
			// ChunkRefs left empty - object data is still external
		}

		if err := h.db.PutObject(ctx, objRef); err != nil {
			logger.Warn().
				Err(err).
				Str("bucket", payload.Bucket).
				Str("key", obj.Key).
				Msg("failed to create object metadata during discovery")
		}
	}

	// Update objects discovered count
	if err := h.updateDiscoveryProgress(ctx, payload.Bucket, int64(result.TotalFound)); err != nil {
		logger.Warn().
			Err(err).
			Str("bucket", payload.Bucket).
			Msg("failed to update discovery progress")
	}

	// If more objects to discover, queue continuation task
	if result.IsTruncated && result.NextMarker != "" {
		if err := discoverer.QueueDiscoveryTask(ctx, payload.Bucket, result.NextMarker, payload.IncludeVersions); err != nil {
			logger.Warn().
				Err(err).
				Str("bucket", payload.Bucket).
				Str("next_marker", result.NextMarker).
				Msg("failed to queue continuation discovery task")
		}
	}

	federationDiscoveryHandledTotal.WithLabelValues(payload.Bucket, "success").Inc()
	federationDiscoveryDuration.WithLabelValues(payload.Bucket).Observe(time.Since(startTime).Seconds())

	logger.Debug().
		Str("bucket", payload.Bucket).
		Int("objects_found", result.TotalFound).
		Int("tasks_queued", result.TasksQueued).
		Bool("has_more", result.IsTruncated).
		Dur("duration", time.Since(startTime)).
		Msg("federation discovery task completed")

	return nil
}

// updateDiscoveryProgress updates the objects_discovered counter in the federation config.
func (h *FederationDiscoveryHandler) updateDiscoveryProgress(ctx context.Context, bucket string, objectsFound int64) error {
	// Get current config to add to discovered count
	config, err := h.db.GetFederationConfig(ctx, bucket)
	if err != nil {
		return err
	}

	// Note: This is a simplified update. In production, you'd want atomic increment.
	config.ObjectsDiscovered += objectsFound
	config.UpdatedAt = time.Now().UnixNano()

	return h.db.SetFederationConfig(ctx, config)
}
