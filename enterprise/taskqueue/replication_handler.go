//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"

	"github.com/google/uuid"
)

// ReplicationPayload contains the data for a replication task.
type ReplicationPayload struct {
	// Source
	SourceRegion string `json:"source_region"`
	SourceBucket string `json:"source_bucket"`
	SourceKey    string `json:"source_key"`
	SourceETag   string `json:"source_etag"`
	SourceSize   int64  `json:"source_size"`

	// Destination
	DestRegion string `json:"dest_region"`
	DestBucket string `json:"dest_bucket"`
	DestKey    string `json:"dest_key,omitempty"` // Defaults to source key

	// Operation
	Operation string `json:"operation"` // "PUT", "DELETE", "COPY"

	// Optional metadata
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// NewReplicationTask creates a task for object replication.
func NewReplicationTask(payload ReplicationPayload) (*taskqueue.Task, error) {
	if payload.SourceBucket == "" || payload.SourceKey == "" {
		return nil, errors.New("source bucket and key are required")
	}
	if payload.DestRegion == "" {
		return nil, errors.New("destination region is required")
	}
	if payload.DestBucket == "" {
		payload.DestBucket = payload.SourceBucket
	}
	if payload.DestKey == "" {
		payload.DestKey = payload.SourceKey
	}
	if payload.Operation == "" {
		payload.Operation = "PUT"
	}

	data, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	return &taskqueue.Task{
		ID:          uuid.New().String(),
		Type:        TaskTypeReplication,
		Status:      taskqueue.StatusPending,
		Priority:    taskqueue.PriorityNormal,
		Payload:     data,
		ScheduledAt: now,
		MaxRetries:  3,
		CreatedAt:   now,
		UpdatedAt:   now,
		Region:      payload.SourceRegion,
	}, nil
}

// ObjectReader provides read access to objects in local storage.
type ObjectReader interface {
	// ReadObject reads an object's content. Caller must close the returned ReadCloser.
	ReadObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectMeta, error)
}

// ObjectMeta contains metadata about an object.
type ObjectMeta struct {
	Size        int64
	ContentType string
	ETag        string
	Metadata    map[string]string
}

// RegionEndpoints provides S3 endpoints for remote regions.
type RegionEndpoints interface {
	// GetS3Endpoint returns the S3 endpoint URL for a region.
	GetS3Endpoint(region string) string
}

// ReplicationCredentials provides credentials for cross-region replication.
type ReplicationCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

// ReplicationHandler processes replication tasks.
type ReplicationHandler struct {
	objectReader ObjectReader
	endpoints    RegionEndpoints
	creds        ReplicationCredentials
	httpClient   *http.Client

	// Cache of S3 clients per region
	clientCache map[string]*s3.Client
}

// ReplicationHandlerConfig configures the replication handler.
type ReplicationHandlerConfig struct {
	ObjectReader ObjectReader
	Endpoints    RegionEndpoints
	Credentials  ReplicationCredentials
	HTTPClient   *http.Client // Optional, defaults to http.DefaultClient
}

// NewReplicationHandler creates a new replication handler.
func NewReplicationHandler(cfg ReplicationHandlerConfig) *ReplicationHandler {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 5 * time.Minute, // Long timeout for large objects
		}
	}

	return &ReplicationHandler{
		objectReader: cfg.ObjectReader,
		endpoints:    cfg.Endpoints,
		creds:        cfg.Credentials,
		httpClient:   httpClient,
		clientCache:  make(map[string]*s3.Client),
	}
}

func (h *ReplicationHandler) Type() taskqueue.TaskType {
	return TaskTypeReplication
}

func (h *ReplicationHandler) Handle(ctx context.Context, task *taskqueue.Task) error {
	payload, err := taskqueue.UnmarshalPayload[ReplicationPayload](task.Payload)
	if err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	logger.Info().
		Str("task_id", task.ID).
		Str("operation", payload.Operation).
		Str("source_bucket", payload.SourceBucket).
		Str("source_key", payload.SourceKey).
		Str("dest_region", payload.DestRegion).
		Str("dest_bucket", payload.DestBucket).
		Msg("processing replication task")

	switch payload.Operation {
	case "PUT":
		return h.handlePut(ctx, &payload)
	case "DELETE":
		return h.handleDelete(ctx, &payload)
	case "COPY":
		return h.handleCopy(ctx, &payload)
	default:
		return fmt.Errorf("unknown operation: %s", payload.Operation)
	}
}

func (h *ReplicationHandler) handlePut(ctx context.Context, p *ReplicationPayload) error {
	// Validate dependencies
	if h.objectReader == nil {
		return errors.New("object reader not configured")
	}

	// 1. Read object from local storage
	body, meta, err := h.objectReader.ReadObject(ctx, p.SourceBucket, p.SourceKey)
	if err != nil {
		return fmt.Errorf("read source object: %w", err)
	}
	defer body.Close()

	// 2. Get S3 client for destination region
	client, err := h.getS3Client(ctx, p.DestRegion)
	if err != nil {
		return fmt.Errorf("get S3 client for region %s: %w", p.DestRegion, err)
	}

	// 3. PUT object to destination bucket
	contentType := p.ContentType
	if contentType == "" && meta != nil {
		contentType = meta.ContentType
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	putInput := &s3.PutObjectInput{
		Bucket:      aws.String(p.DestBucket),
		Key:         aws.String(p.DestKey),
		Body:        body,
		ContentType: aws.String(contentType),
	}

	// Copy metadata if available
	if len(p.Metadata) > 0 {
		putInput.Metadata = p.Metadata
	} else if meta != nil && len(meta.Metadata) > 0 {
		putInput.Metadata = meta.Metadata
	}

	// Set content length if known
	if meta != nil && meta.Size > 0 {
		putInput.ContentLength = aws.Int64(meta.Size)
	} else if p.SourceSize > 0 {
		putInput.ContentLength = aws.Int64(p.SourceSize)
	}

	result, err := client.PutObject(ctx, putInput)
	if err != nil {
		return fmt.Errorf("put object to destination: %w", err)
	}

	// 4. Verify ETag matches (optional - destination may have different encryption)
	destETag := ""
	if result.ETag != nil {
		destETag = *result.ETag
	}

	logger.Info().
		Str("source", p.SourceBucket+"/"+p.SourceKey).
		Str("dest", p.DestRegion+":"+p.DestBucket+"/"+p.DestKey).
		Str("source_etag", p.SourceETag).
		Str("dest_etag", destETag).
		Msg("replicated object successfully")

	return nil
}

func (h *ReplicationHandler) handleDelete(ctx context.Context, p *ReplicationPayload) error {
	// Get S3 client for destination region
	client, err := h.getS3Client(ctx, p.DestRegion)
	if err != nil {
		return fmt.Errorf("get S3 client for region %s: %w", p.DestRegion, err)
	}

	// Delete object from destination bucket
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(p.DestBucket),
		Key:    aws.String(p.DestKey),
	})
	if err != nil {
		return fmt.Errorf("delete object from destination: %w", err)
	}

	logger.Info().
		Str("dest", p.DestRegion+":"+p.DestBucket+"/"+p.DestKey).
		Msg("replicated delete successfully")

	return nil
}

func (h *ReplicationHandler) handleCopy(ctx context.Context, p *ReplicationPayload) error {
	// Same as PUT for cross-region
	return h.handlePut(ctx, p)
}

// getS3Client returns an S3 client for the specified region, creating one if needed.
func (h *ReplicationHandler) getS3Client(ctx context.Context, region string) (*s3.Client, error) {
	// Check cache first
	if client, ok := h.clientCache[region]; ok {
		return client, nil
	}

	// Get endpoint for region
	if h.endpoints == nil {
		return nil, errors.New("region endpoints not configured")
	}

	endpoint := h.endpoints.GetS3Endpoint(region)
	if endpoint == "" {
		return nil, fmt.Errorf("no S3 endpoint configured for region %s", region)
	}

	// Create S3 client with custom endpoint
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			h.creds.AccessKeyID,
			h.creds.SecretAccessKey,
			"", // session token
		)),
		config.WithHTTPClient(h.httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // Use path-style for custom endpoints
	})

	// Cache the client
	h.clientCache[region] = client

	return client, nil
}
