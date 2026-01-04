// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package federation

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for lazy ingest operations
var (
	lazyIngestQueuedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_lazy_ingest_queued_total",
			Help: "Total number of objects queued for lazy ingest",
		},
		[]string{"bucket"},
	)
	lazyIngestFetchedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_lazy_ingest_fetched_total",
			Help: "Total number of objects fetched from external S3 for lazy ingest",
		},
		[]string{"bucket"},
	)
	lazyIngestFetchDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "zapfs_federation_lazy_ingest_fetch_duration_seconds",
			Help:    "Duration of lazy ingest fetch operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"bucket"},
	)
	lazyIngestBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_lazy_ingest_bytes_total",
			Help: "Total bytes fetched from external S3 for lazy ingest",
		},
		[]string{"bucket"},
	)
)

// LazyIngestor handles lazy migration of objects from external S3.
// When a GET request hits an object that only exists externally (no local chunks),
// the LazyIngestor fetches the object, returns it to the client, and queues an
// async task to ingest the object into local storage.
type LazyIngestor struct {
	clientPool *ClientPool
	taskQueue  taskqueue.Queue
}

// NewLazyIngestor creates a new lazy ingestor.
func NewLazyIngestor(clientPool *ClientPool, taskQueue taskqueue.Queue) *LazyIngestor {
	return &LazyIngestor{
		clientPool: clientPool,
		taskQueue:  taskQueue,
	}
}

// FetchResult contains the result of fetching an object from external S3.
type FetchResult struct {
	// Body is the object content stream. Caller must close.
	Body io.ReadCloser

	// Metadata
	ContentType        string
	ContentLength      int64
	ETag               string
	LastModified       time.Time
	ContentEncoding    string
	ContentDisposition string
	CacheControl       string
	StorageClass       string
	Metadata           map[string]string
	VersionID          string
}

// FetchAndQueue fetches an object from external S3 and queues an async ingest task.
// This is the main entry point for lazy migration in MIGRATING mode.
//
// The flow is:
// 1. Fetch object from external S3
// 2. Return the body stream to caller (for immediate response to client)
// 3. Queue an async task to ingest the object into local storage
//
// Note: The actual ingest happens asynchronously via the task queue.
// On subsequent GETs, the object will be served from local storage.
func (l *LazyIngestor) FetchAndQueue(ctx context.Context, fedConfig *s3types.FederationConfig, key string) (*FetchResult, error) {
	startTime := time.Now()

	// Build external S3 config
	extCfg := &ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}

	// Fetch from external S3
	output, err := l.clientPool.GetObject(ctx, extCfg, key)
	if err != nil {
		return nil, fmt.Errorf("fetch from external S3: %w", err)
	}

	// Record fetch metrics
	lazyIngestFetchedTotal.WithLabelValues(fedConfig.Bucket).Inc()
	lazyIngestFetchDuration.WithLabelValues(fedConfig.Bucket).Observe(time.Since(startTime).Seconds())

	contentLength := int64(0)
	if output.ContentLength != nil {
		contentLength = *output.ContentLength
		lazyIngestBytesTotal.WithLabelValues(fedConfig.Bucket).Add(float64(contentLength))
	}

	// Build fetch result
	result := &FetchResult{
		Body:          output.Body,
		ContentLength: contentLength,
		StorageClass:  string(output.StorageClass),
		Metadata:      output.Metadata,
	}

	if output.ContentType != nil {
		result.ContentType = *output.ContentType
	}
	if output.ETag != nil {
		result.ETag = *output.ETag
	}
	if output.LastModified != nil {
		result.LastModified = *output.LastModified
	}
	if output.ContentEncoding != nil {
		result.ContentEncoding = *output.ContentEncoding
	}
	if output.ContentDisposition != nil {
		result.ContentDisposition = *output.ContentDisposition
	}
	if output.CacheControl != nil {
		result.CacheControl = *output.CacheControl
	}
	if output.VersionId != nil {
		result.VersionID = *output.VersionId
	}

	// Queue async ingest task (non-blocking)
	// This happens in the background after we return the response to the client
	if l.taskQueue != nil {
		if err := l.queueIngestTask(ctx, fedConfig.Bucket, key, result); err != nil {
			// Log but don't fail the request - the object was fetched successfully
			logger.Warn().
				Err(err).
				Str("bucket", fedConfig.Bucket).
				Str("key", key).
				Msg("failed to queue ingest task for lazy migration")
		} else {
			lazyIngestQueuedTotal.WithLabelValues(fedConfig.Bucket).Inc()
		}
	}

	return result, nil
}

// FetchWithVersion fetches a specific version of an object from external S3.
func (l *LazyIngestor) FetchWithVersion(ctx context.Context, fedConfig *s3types.FederationConfig, key, versionID string) (*FetchResult, error) {
	startTime := time.Now()

	extCfg := &ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}

	output, err := l.clientPool.GetObjectWithVersion(ctx, extCfg, key, versionID)
	if err != nil {
		return nil, fmt.Errorf("fetch version from external S3: %w", err)
	}

	lazyIngestFetchedTotal.WithLabelValues(fedConfig.Bucket).Inc()
	lazyIngestFetchDuration.WithLabelValues(fedConfig.Bucket).Observe(time.Since(startTime).Seconds())

	contentLength := int64(0)
	if output.ContentLength != nil {
		contentLength = *output.ContentLength
		lazyIngestBytesTotal.WithLabelValues(fedConfig.Bucket).Add(float64(contentLength))
	}

	result := &FetchResult{
		Body:          output.Body,
		ContentLength: contentLength,
		StorageClass:  string(output.StorageClass),
		Metadata:      output.Metadata,
	}

	if output.ContentType != nil {
		result.ContentType = *output.ContentType
	}
	if output.ETag != nil {
		result.ETag = *output.ETag
	}
	if output.LastModified != nil {
		result.LastModified = *output.LastModified
	}
	if output.ContentEncoding != nil {
		result.ContentEncoding = *output.ContentEncoding
	}
	if output.ContentDisposition != nil {
		result.ContentDisposition = *output.ContentDisposition
	}
	if output.CacheControl != nil {
		result.CacheControl = *output.CacheControl
	}
	if output.VersionId != nil {
		result.VersionID = *output.VersionId
	}

	// Queue async ingest task for versioned object
	if l.taskQueue != nil {
		if err := l.queueIngestTaskWithVersion(ctx, fedConfig.Bucket, key, versionID, result); err != nil {
			logger.Warn().
				Err(err).
				Str("bucket", fedConfig.Bucket).
				Str("key", key).
				Str("version_id", versionID).
				Msg("failed to queue versioned ingest task for lazy migration")
		} else {
			lazyIngestQueuedTotal.WithLabelValues(fedConfig.Bucket).Inc()
		}
	}

	return result, nil
}

// queueIngestTask creates and enqueues a federation ingest task.
func (l *LazyIngestor) queueIngestTask(ctx context.Context, bucket, key string, result *FetchResult) error {
	payload := taskqueue.FederationIngestPayload{
		Bucket:       bucket,
		Key:          key,
		Size:         result.ContentLength,
		ETag:         result.ETag,
		ContentType:  result.ContentType,
		StorageClass: result.StorageClass,
		LastModified: result.LastModified.UnixNano(),
		DiscoveredAt: time.Now().UnixNano(),
	}

	payloadBytes, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	task := &taskqueue.Task{
		Type:        taskqueue.TaskTypeFederationIngest,
		Priority:    taskqueue.PriorityNormal,
		Payload:     payloadBytes,
		MaxRetries:  3,
		ScheduledAt: time.Now(),
	}

	return l.taskQueue.Enqueue(ctx, task)
}

// queueIngestTaskWithVersion creates and enqueues a federation ingest task for a versioned object.
func (l *LazyIngestor) queueIngestTaskWithVersion(ctx context.Context, bucket, key, versionID string, result *FetchResult) error {
	payload := taskqueue.FederationIngestPayload{
		Bucket:       bucket,
		Key:          key,
		VersionID:    versionID,
		Size:         result.ContentLength,
		ETag:         result.ETag,
		ContentType:  result.ContentType,
		StorageClass: result.StorageClass,
		LastModified: result.LastModified.UnixNano(),
		DiscoveredAt: time.Now().UnixNano(),
	}

	payloadBytes, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	task := &taskqueue.Task{
		Type:        taskqueue.TaskTypeFederationIngest,
		Priority:    taskqueue.PriorityNormal,
		Payload:     payloadBytes,
		MaxRetries:  3,
		ScheduledAt: time.Now(),
	}

	return l.taskQueue.Enqueue(ctx, task)
}

// HeadObject checks if an object exists in external S3 and returns its metadata.
// Used to verify object existence before attempting lazy fetch.
func (l *LazyIngestor) HeadObject(ctx context.Context, fedConfig *s3types.FederationConfig, key string) (*s3.HeadObjectOutput, error) {
	extCfg := &ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}

	return l.clientPool.HeadObject(ctx, extCfg, key)
}
