// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package federation

import (
	"context"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awstypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for discovery operations
var (
	discoveryObjectsFoundTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_discovery_objects_found_total",
			Help: "Total number of objects discovered in external S3",
		},
		[]string{"bucket"},
	)
	discoveryBatchesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_discovery_batches_total",
			Help: "Total number of discovery batches processed",
		},
		[]string{"bucket"},
	)
	discoveryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "zapfs_federation_discovery_duration_seconds",
			Help:    "Duration of discovery operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"bucket"},
	)
	discoveryTasksQueuedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_discovery_tasks_queued_total",
			Help: "Total number of ingest tasks queued from discovery",
		},
		[]string{"bucket"},
	)
)

// DiscoveryConfig holds configuration for object discovery.
type DiscoveryConfig struct {
	// BatchSize is the max number of objects to list per API call (max 1000)
	BatchSize int

	// IncludeVersions determines whether to discover all versions (for versioned buckets)
	IncludeVersions bool

	// Prefix filters objects by key prefix
	Prefix string

	// RateLimit is the max objects/sec to process (0 = unlimited)
	RateLimit int
}

// DefaultDiscoveryConfig returns sensible defaults for discovery.
func DefaultDiscoveryConfig() DiscoveryConfig {
	return DiscoveryConfig{
		BatchSize:       1000,
		IncludeVersions: false,
		RateLimit:       100, // 100 objects/sec default
	}
}

// DiscoveredObject contains metadata about a discovered external object.
type DiscoveredObject struct {
	Key            string
	Size           int64
	ETag           string
	LastModified   time.Time
	StorageClass   string
	VersionID      string
	IsLatest       bool
	IsDeleteMarker bool
}

// DiscoveryResult contains the result of a discovery batch.
type DiscoveryResult struct {
	Objects     []DiscoveredObject
	IsTruncated bool
	NextMarker  string // For continuation
	TotalFound  int
	TasksQueued int
}

// Discoverer handles discovery of objects in external S3 buckets.
// It lists objects and creates ingest tasks for migration.
type Discoverer struct {
	clientPool *ClientPool
	taskQueue  taskqueue.Queue
	config     DiscoveryConfig
}

// NewDiscoverer creates a new discoverer.
func NewDiscoverer(clientPool *ClientPool, taskQueue taskqueue.Queue, config DiscoveryConfig) *Discoverer {
	if config.BatchSize <= 0 || config.BatchSize > 1000 {
		config.BatchSize = 1000
	}
	return &Discoverer{
		clientPool: clientPool,
		taskQueue:  taskQueue,
		config:     config,
	}
}

// DiscoverBatch discovers a batch of objects from external S3.
// Returns the discovered objects and a marker for continuation.
// This is the main entry point for active migration discovery.
func (d *Discoverer) DiscoverBatch(ctx context.Context, fedConfig *s3types.FederationConfig, startAfter string) (*DiscoveryResult, error) {
	startTime := time.Now()

	extCfg := &ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}

	var result *DiscoveryResult
	var err error

	if d.config.IncludeVersions {
		result, err = d.discoverVersions(ctx, extCfg, fedConfig.Bucket, startAfter)
	} else {
		result, err = d.discoverObjects(ctx, extCfg, fedConfig.Bucket, startAfter)
	}

	if err != nil {
		return nil, err
	}

	// Record metrics
	discoveryBatchesTotal.WithLabelValues(fedConfig.Bucket).Inc()
	discoveryObjectsFoundTotal.WithLabelValues(fedConfig.Bucket).Add(float64(result.TotalFound))
	discoveryDuration.WithLabelValues(fedConfig.Bucket).Observe(time.Since(startTime).Seconds())
	discoveryTasksQueuedTotal.WithLabelValues(fedConfig.Bucket).Add(float64(result.TasksQueued))

	return result, nil
}

// discoverObjects lists objects (current versions only) from external S3.
func (d *Discoverer) discoverObjects(ctx context.Context, extCfg *ExternalS3Config, bucket, startAfter string) (*DiscoveryResult, error) {
	client, err := d.clientPool.GetClient(ctx, extCfg)
	if err != nil {
		return nil, fmt.Errorf("get client: %w", err)
	}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(extCfg.Bucket),
		MaxKeys: aws.Int32(int32(d.config.BatchSize)),
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}
	if d.config.Prefix != "" {
		input.Prefix = aws.String(d.config.Prefix)
	}

	output, err := client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	result := &DiscoveryResult{
		Objects:     make([]DiscoveredObject, 0, len(output.Contents)),
		IsTruncated: aws.ToBool(output.IsTruncated),
	}

	for _, obj := range output.Contents {
		discovered := DiscoveredObject{
			Key:          aws.ToString(obj.Key),
			Size:         aws.ToInt64(obj.Size),
			ETag:         aws.ToString(obj.ETag),
			StorageClass: string(obj.StorageClass),
			IsLatest:     true,
		}
		if obj.LastModified != nil {
			discovered.LastModified = *obj.LastModified
		}

		result.Objects = append(result.Objects, discovered)

		// Queue ingest task
		if d.taskQueue != nil {
			if err := d.queueIngestTask(ctx, bucket, discovered); err != nil {
				logger.Warn().
					Err(err).
					Str("bucket", bucket).
					Str("key", discovered.Key).
					Msg("failed to queue ingest task during discovery")
			} else {
				result.TasksQueued++
			}
		}
	}

	result.TotalFound = len(result.Objects)

	// Set next marker for continuation
	if result.IsTruncated && len(result.Objects) > 0 {
		result.NextMarker = result.Objects[len(result.Objects)-1].Key
	}

	return result, nil
}

// discoverVersions lists all versions of objects from external S3.
func (d *Discoverer) discoverVersions(ctx context.Context, extCfg *ExternalS3Config, bucket, keyMarker string) (*DiscoveryResult, error) {
	client, err := d.clientPool.GetClient(ctx, extCfg)
	if err != nil {
		return nil, fmt.Errorf("get client: %w", err)
	}

	input := &s3.ListObjectVersionsInput{
		Bucket:  aws.String(extCfg.Bucket),
		MaxKeys: aws.Int32(int32(d.config.BatchSize)),
	}
	if keyMarker != "" {
		input.KeyMarker = aws.String(keyMarker)
	}
	if d.config.Prefix != "" {
		input.Prefix = aws.String(d.config.Prefix)
	}

	output, err := client.ListObjectVersions(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("list object versions: %w", err)
	}

	result := &DiscoveryResult{
		Objects:     make([]DiscoveredObject, 0, len(output.Versions)+len(output.DeleteMarkers)),
		IsTruncated: aws.ToBool(output.IsTruncated),
	}

	// Process versions
	for _, v := range output.Versions {
		discovered := DiscoveredObject{
			Key:          aws.ToString(v.Key),
			Size:         aws.ToInt64(v.Size),
			ETag:         aws.ToString(v.ETag),
			VersionID:    aws.ToString(v.VersionId),
			IsLatest:     aws.ToBool(v.IsLatest),
			StorageClass: string(v.StorageClass),
		}
		if v.LastModified != nil {
			discovered.LastModified = *v.LastModified
		}

		result.Objects = append(result.Objects, discovered)

		// Queue ingest task for version
		if d.taskQueue != nil {
			if err := d.queueIngestTask(ctx, bucket, discovered); err != nil {
				logger.Warn().
					Err(err).
					Str("bucket", bucket).
					Str("key", discovered.Key).
					Str("version_id", discovered.VersionID).
					Msg("failed to queue versioned ingest task during discovery")
			} else {
				result.TasksQueued++
			}
		}
	}

	// Process delete markers (we track them but don't ingest data)
	for _, dm := range output.DeleteMarkers {
		discovered := DiscoveredObject{
			Key:            aws.ToString(dm.Key),
			VersionID:      aws.ToString(dm.VersionId),
			IsLatest:       aws.ToBool(dm.IsLatest),
			IsDeleteMarker: true,
		}
		if dm.LastModified != nil {
			discovered.LastModified = *dm.LastModified
		}

		result.Objects = append(result.Objects, discovered)
		// Note: We don't queue ingest tasks for delete markers
	}

	result.TotalFound = len(result.Objects)

	// Set next marker for continuation
	if result.IsTruncated {
		result.NextMarker = aws.ToString(output.NextKeyMarker)
	}

	return result, nil
}

// queueIngestTask creates and enqueues a federation ingest task for a discovered object.
func (d *Discoverer) queueIngestTask(ctx context.Context, bucket string, obj DiscoveredObject) error {
	// Skip delete markers - they don't have data to ingest
	if obj.IsDeleteMarker {
		return nil
	}

	payload := taskqueue.FederationIngestPayload{
		Bucket:       bucket,
		Key:          obj.Key,
		VersionID:    obj.VersionID,
		Size:         obj.Size,
		ETag:         obj.ETag,
		StorageClass: obj.StorageClass,
		LastModified: obj.LastModified.UnixNano(),
		DiscoveredAt: time.Now().UnixNano(),
	}

	payloadBytes, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	task := &taskqueue.Task{
		Type:        taskqueue.TaskTypeFederationIngest,
		Priority:    taskqueue.PriorityLow, // Discovery tasks are low priority (background)
		Payload:     payloadBytes,
		MaxRetries:  3,
		ScheduledAt: time.Now(),
	}

	return d.taskQueue.Enqueue(ctx, task)
}

// QueueDiscoveryTask creates and enqueues a federation discovery task.
// Used to continue discovery in batches via the task queue.
func (d *Discoverer) QueueDiscoveryTask(ctx context.Context, bucket, startAfter string, includeVersions bool) error {
	payload := taskqueue.FederationDiscoveryPayload{
		Bucket:          bucket,
		StartAfter:      startAfter,
		Prefix:          d.config.Prefix,
		BatchSize:       d.config.BatchSize,
		IncludeVersions: includeVersions,
		StartedAt:       time.Now().UnixNano(),
	}

	payloadBytes, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	task := &taskqueue.Task{
		Type:        taskqueue.TaskTypeFederationDiscovery,
		Priority:    taskqueue.PriorityLow,
		Payload:     payloadBytes,
		MaxRetries:  5, // More retries for discovery (pagination state)
		ScheduledAt: time.Now(),
	}

	return d.taskQueue.Enqueue(ctx, task)
}

// CountObjects returns the total number of objects in the external bucket.
// Used for migration progress estimation.
func (d *Discoverer) CountObjects(ctx context.Context, fedConfig *s3types.FederationConfig) (int64, error) {
	extCfg := &ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}

	client, err := d.clientPool.GetClient(ctx, extCfg)
	if err != nil {
		return 0, fmt.Errorf("get client: %w", err)
	}

	var count int64
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(extCfg.Bucket),
			MaxKeys:           aws.Int32(1000),
			ContinuationToken: continuationToken,
		}
		if d.config.Prefix != "" {
			input.Prefix = aws.String(d.config.Prefix)
		}

		output, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			return count, fmt.Errorf("list objects: %w", err)
		}

		count += int64(len(output.Contents))

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return count, nil
}

// HeadBucket verifies the external bucket exists and is accessible.
func (d *Discoverer) HeadBucket(ctx context.Context, fedConfig *s3types.FederationConfig) error {
	extCfg := &ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}

	client, err := d.clientPool.GetClient(ctx, extCfg)
	if err != nil {
		return fmt.Errorf("get client: %w", err)
	}

	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(extCfg.Bucket),
	})
	if err != nil {
		return fmt.Errorf("head bucket: %w", err)
	}

	return nil
}

// Ensure awstypes is used
var _ awstypes.StorageClass
