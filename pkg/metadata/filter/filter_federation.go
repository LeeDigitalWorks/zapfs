// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/federation"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3action"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awstypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for federation operations
var (
	federationRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_requests_total",
			Help: "Total number of federation requests by bucket, action, and mode",
		},
		[]string{"bucket", "action", "mode"},
	)
	federationRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "zapfs_federation_request_duration_seconds",
			Help:    "Duration of federation requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"bucket", "action"},
	)
	federationErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_errors_total",
			Help: "Total number of federation errors by bucket and error type",
		},
		[]string{"bucket", "error_type"},
	)
)

// FederationConfigStore retrieves federation configurations from the metadata database.
type FederationConfigStore interface {
	// GetFederationConfig returns the federation config for a bucket, or nil if not federated.
	GetFederationConfig(ctx context.Context, bucket string) (*s3types.FederationConfig, error)
}

// FederationFilterConfig holds configuration for the federation filter.
type FederationFilterConfig struct {
	// Enabled controls whether federation features are active
	Enabled bool

	// ConfigStore retrieves federation configurations
	ConfigStore FederationConfigStore

	// ClientPool manages external S3 client connections
	ClientPool *federation.ClientPool

	// CacheTTL is how long to cache federation configs (default: 1 minute)
	CacheTTL time.Duration

	// ResponseWriter is used to write HTTP responses (for passthrough mode)
	// If nil, the filter will use the ResponseWriter from the request context
	ResponseWriter http.ResponseWriter
}

// FederationFilter handles federated bucket requests by routing them appropriately:
// - LOCAL mode: pass through to normal handlers
// - PASSTHROUGH mode: proxy to external S3
// - MIGRATING mode: check local first, lazy-fetch from external if needed
//
// Position in filter chain: After Authorization, Before RateLimit
type FederationFilter struct {
	enabled     bool
	configStore FederationConfigStore
	clientPool  *federation.ClientPool
	cacheTTL    time.Duration

	// In-memory cache for federation configs
	cache   map[string]*cachedConfig
	cacheMu sync.RWMutex
}

type cachedConfig struct {
	config    *s3types.FederationConfig
	expiresAt time.Time
}

// NewFederationFilter creates a new federation filter.
func NewFederationFilter(cfg FederationFilterConfig) *FederationFilter {
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = time.Minute
	}

	return &FederationFilter{
		enabled:     cfg.Enabled,
		configStore: cfg.ConfigStore,
		clientPool:  cfg.ClientPool,
		cacheTTL:    cfg.CacheTTL,
		cache:       make(map[string]*cachedConfig),
	}
}

func (f *FederationFilter) Type() string {
	return "federation"
}

func (f *FederationFilter) Run(d *data.Data) (Response, error) {
	// Skip if federation is disabled
	if !f.enabled {
		return Next{}, nil
	}

	// Skip if no bucket in request
	bucket := d.S3Info.Bucket
	if bucket == "" {
		return Next{}, nil
	}

	// Check context cancellation
	if d.Ctx.Err() != nil {
		return nil, d.Ctx.Err()
	}

	// Get federation config for this bucket
	fedConfig, err := f.getFederationConfig(d.Ctx, bucket)
	if err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("error getting federation config")
		federationErrorsTotal.WithLabelValues(bucket, "config_lookup").Inc()
		return nil, s3err.ErrInternalError
	}

	// No federation config means local bucket - continue to normal handlers
	if fedConfig == nil {
		return Next{}, nil
	}

	// Determine bucket mode from federation config
	// If MigrationStartedAt > 0, it's MIGRATING; otherwise PASSTHROUGH
	var mode s3types.BucketMode
	if fedConfig.MigrationStartedAt > 0 {
		mode = s3types.BucketModeMigrating
	} else {
		mode = s3types.BucketModePassthrough
	}

	action := d.S3Info.Action
	actionStr := action.String()

	// Record metrics
	federationRequestsTotal.WithLabelValues(bucket, actionStr, mode.String()).Inc()
	startTime := time.Now()
	defer func() {
		federationRequestDuration.WithLabelValues(bucket, actionStr).Observe(time.Since(startTime).Seconds())
	}()

	// Handle based on bucket mode
	switch mode {
	case s3types.BucketModePassthrough:
		return f.handlePassthrough(d, fedConfig)
	case s3types.BucketModeMigrating:
		return f.handleMigrating(d, fedConfig)
	default:
		// Unknown mode - continue to normal handlers
		return Next{}, nil
	}
}

// getFederationConfig retrieves the federation config from cache or database.
func (f *FederationFilter) getFederationConfig(ctx context.Context, bucket string) (*s3types.FederationConfig, error) {
	// Check cache first
	f.cacheMu.RLock()
	cached, ok := f.cache[bucket]
	f.cacheMu.RUnlock()

	if ok && time.Now().Before(cached.expiresAt) {
		return cached.config, nil
	}

	// Cache miss or expired - fetch from store
	config, err := f.configStore.GetFederationConfig(ctx, bucket)
	if err != nil {
		// Check if it's "not found" - that's not an error, just means not federated
		if err.Error() == "federation configuration not found" {
			// Cache the negative result
			f.cacheMu.Lock()
			f.cache[bucket] = &cachedConfig{
				config:    nil,
				expiresAt: time.Now().Add(f.cacheTTL),
			}
			f.cacheMu.Unlock()
			return nil, nil
		}
		return nil, err
	}

	// Cache the result
	f.cacheMu.Lock()
	f.cache[bucket] = &cachedConfig{
		config:    config,
		expiresAt: time.Now().Add(f.cacheTTL),
	}
	f.cacheMu.Unlock()

	return config, nil
}

// handlePassthrough proxies the request to the external S3 bucket.
// All operations go directly to external S3, metadata is stored locally.
func (f *FederationFilter) handlePassthrough(d *data.Data, fedConfig *s3types.FederationConfig) (Response, error) {
	action := d.S3Info.Action

	// Build external S3 config
	extCfg := &federation.ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}

	// Route based on action type
	switch action {
	case s3action.GetObject:
		return f.proxyGetObject(d, extCfg)
	case s3action.HeadObject:
		return f.proxyHeadObject(d, extCfg)
	case s3action.PutObject:
		return f.proxyPutObject(d, extCfg)
	case s3action.DeleteObject:
		return f.proxyDeleteObject(d, extCfg)
	case s3action.CopyObject:
		return f.proxyCopyObject(d, extCfg)
	default:
		// For list operations and other actions, use local metadata
		// ListObjects uses local metadata for fast queries
		return Next{}, nil
	}
}

// handleMigrating handles requests for buckets being migrated from external S3.
// GET/HEAD: Check local first, fetch from external if not available
// PUT/DELETE: Write to local only (or both if dual-write enabled)
func (f *FederationFilter) handleMigrating(d *data.Data, fedConfig *s3types.FederationConfig) (Response, error) {
	action := d.S3Info.Action

	// Build external S3 config
	extCfg := &federation.ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}

	switch action {
	case s3action.GetObject, s3action.HeadObject:
		// For MIGRATING mode, we need to check if the object exists locally first.
		// This requires coordination with the object handler which has access to the database.
		// For now, we'll continue to the handler and let it do the lazy fetch if needed.
		// The handler can check d.FederationConfig to know it should try external on 404.
		d.FederationConfig = fedConfig
		d.FederationExtConfig = extCfg
		return Next{}, nil

	case s3action.PutObject:
		// Writes go to local storage
		// If dual-write is enabled, also write to external (handled by coordinator)
		if fedConfig.DualWriteEnabled {
			d.FederationConfig = fedConfig
			d.FederationExtConfig = extCfg
		}
		return Next{}, nil

	case s3action.DeleteObject:
		// Deletes go to local storage
		// If dual-write is enabled, also delete from external (handled by coordinator)
		if fedConfig.DualWriteEnabled {
			d.FederationConfig = fedConfig
			d.FederationExtConfig = extCfg
		}
		return Next{}, nil

	default:
		// For other actions, continue to normal handlers
		return Next{}, nil
	}
}

// proxyGetObject proxies a GetObject request to external S3.
func (f *FederationFilter) proxyGetObject(d *data.Data, extCfg *federation.ExternalS3Config) (Response, error) {
	key := d.S3Info.Key

	// Get object from external S3
	output, err := f.clientPool.GetObject(d.Ctx, extCfg, key)
	if err != nil {
		federationErrorsTotal.WithLabelValues(d.S3Info.Bucket, "get_object").Inc()
		return nil, f.mapS3Error(err)
	}
	defer output.Body.Close()

	// Write response headers
	w := d.ResponseWriter
	if w == nil {
		return nil, s3err.ErrInternalError
	}

	if output.ContentType != nil {
		w.Header().Set("Content-Type", *output.ContentType)
	}
	if output.ContentLength != nil {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", *output.ContentLength))
	}
	if output.ETag != nil {
		w.Header().Set("ETag", *output.ETag)
	}
	if output.LastModified != nil {
		w.Header().Set("Last-Modified", output.LastModified.Format(time.RFC1123))
	}
	if output.ContentEncoding != nil {
		w.Header().Set("Content-Encoding", *output.ContentEncoding)
	}
	if output.CacheControl != nil {
		w.Header().Set("Cache-Control", *output.CacheControl)
	}
	if output.ContentDisposition != nil {
		w.Header().Set("Content-Disposition", *output.ContentDisposition)
	}
	if output.StorageClass != "" {
		w.Header().Set("x-amz-storage-class", string(output.StorageClass))
	}

	// Copy metadata headers
	for k, v := range output.Metadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}

	w.WriteHeader(http.StatusOK)

	// Stream body to client
	_, err = io.Copy(w, output.Body)
	if err != nil {
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Str("key", key).Msg("error streaming object body")
		// Response already started, can't return error
	}

	return End{}, nil
}

// proxyHeadObject proxies a HeadObject request to external S3.
func (f *FederationFilter) proxyHeadObject(d *data.Data, extCfg *federation.ExternalS3Config) (Response, error) {
	key := d.S3Info.Key

	output, err := f.clientPool.HeadObject(d.Ctx, extCfg, key)
	if err != nil {
		federationErrorsTotal.WithLabelValues(d.S3Info.Bucket, "head_object").Inc()
		return nil, f.mapS3Error(err)
	}

	w := d.ResponseWriter
	if w == nil {
		return nil, s3err.ErrInternalError
	}

	if output.ContentType != nil {
		w.Header().Set("Content-Type", *output.ContentType)
	}
	if output.ContentLength != nil {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", *output.ContentLength))
	}
	if output.ETag != nil {
		w.Header().Set("ETag", *output.ETag)
	}
	if output.LastModified != nil {
		w.Header().Set("Last-Modified", output.LastModified.Format(time.RFC1123))
	}
	if output.StorageClass != "" {
		w.Header().Set("x-amz-storage-class", string(output.StorageClass))
	}

	for k, v := range output.Metadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}

	w.WriteHeader(http.StatusOK)
	return End{}, nil
}

// proxyPutObject proxies a PutObject request to external S3.
func (f *FederationFilter) proxyPutObject(d *data.Data, extCfg *federation.ExternalS3Config) (Response, error) {
	key := d.S3Info.Key
	contentType := d.Req.Header.Get("Content-Type")
	contentLength := d.Req.ContentLength

	output, err := f.clientPool.PutObject(d.Ctx, extCfg, key, d.Req.Body, contentLength, contentType)
	if err != nil {
		federationErrorsTotal.WithLabelValues(d.S3Info.Bucket, "put_object").Inc()
		return nil, f.mapS3Error(err)
	}

	w := d.ResponseWriter
	if w == nil {
		return nil, s3err.ErrInternalError
	}

	if output.ETag != nil {
		w.Header().Set("ETag", *output.ETag)
	}
	if output.VersionId != nil {
		w.Header().Set("x-amz-version-id", *output.VersionId)
	}

	w.WriteHeader(http.StatusOK)
	return End{}, nil
}

// proxyDeleteObject proxies a DeleteObject request to external S3.
func (f *FederationFilter) proxyDeleteObject(d *data.Data, extCfg *federation.ExternalS3Config) (Response, error) {
	key := d.S3Info.Key

	output, err := f.clientPool.DeleteObject(d.Ctx, extCfg, key)
	if err != nil {
		federationErrorsTotal.WithLabelValues(d.S3Info.Bucket, "delete_object").Inc()
		return nil, f.mapS3Error(err)
	}

	w := d.ResponseWriter
	if w == nil {
		return nil, s3err.ErrInternalError
	}

	if output.DeleteMarker != nil && *output.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	}
	if output.VersionId != nil {
		w.Header().Set("x-amz-version-id", *output.VersionId)
	}

	w.WriteHeader(http.StatusNoContent)
	return End{}, nil
}

// proxyCopyObject proxies a CopyObject request within the external S3 bucket.
func (f *FederationFilter) proxyCopyObject(d *data.Data, extCfg *federation.ExternalS3Config) (Response, error) {
	destKey := d.S3Info.Key
	sourceKey := d.Req.Header.Get("x-amz-copy-source")

	// Strip bucket prefix from source if present
	// Format: /bucket/key or bucket/key
	if len(sourceKey) > 0 && sourceKey[0] == '/' {
		sourceKey = sourceKey[1:]
	}
	// Find first / and take everything after it as the key
	for i := 0; i < len(sourceKey); i++ {
		if sourceKey[i] == '/' {
			sourceKey = sourceKey[i+1:]
			break
		}
	}

	output, err := f.clientPool.CopyObject(d.Ctx, extCfg, sourceKey, destKey)
	if err != nil {
		federationErrorsTotal.WithLabelValues(d.S3Info.Bucket, "copy_object").Inc()
		return nil, f.mapS3Error(err)
	}

	w := d.ResponseWriter
	if w == nil {
		return nil, s3err.ErrInternalError
	}

	// CopyObject returns XML body with CopyObjectResult
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	result := &s3types.CopyObjectResult{}
	if output.CopyObjectResult != nil {
		if output.CopyObjectResult.ETag != nil {
			result.ETag = *output.CopyObjectResult.ETag
		}
		if output.CopyObjectResult.LastModified != nil {
			result.LastModified = *output.CopyObjectResult.LastModified
		}
	}

	xmlData, _ := result.ToXML()
	w.Write(xmlData)

	return End{}, nil
}

// mapS3Error maps AWS SDK S3 errors to ZapFS S3 errors.
func (f *FederationFilter) mapS3Error(err error) s3err.ErrorCode {
	// Check for specific S3 error types
	var noSuchKey *awstypes.NoSuchKey
	var noSuchBucket *awstypes.NoSuchBucket
	var notFound *awstypes.NotFound

	if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
		return s3err.ErrNoSuchKey
	}
	if errors.As(err, &noSuchBucket) {
		return s3err.ErrNoSuchBucket
	}

	// Default to internal error
	logger.Error().Err(err).Msg("unmapped S3 error from external service")
	return s3err.ErrInternalError
}

// InvalidateCache removes a bucket from the federation config cache.
// Called when federation config is updated via admin API.
func (f *FederationFilter) InvalidateCache(bucket string) {
	f.cacheMu.Lock()
	delete(f.cache, bucket)
	f.cacheMu.Unlock()
}

// Close releases resources held by the filter.
func (f *FederationFilter) Close() error {
	if f.clientPool != nil {
		return f.clientPool.Close()
	}
	return nil
}

// FetchFromExternal fetches an object from external S3.
// Used by object handlers for lazy migration in MIGRATING mode.
func (f *FederationFilter) FetchFromExternal(ctx context.Context, fedConfig *s3types.FederationConfig, key string) (*s3.GetObjectOutput, error) {
	extCfg := &federation.ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}
	return f.clientPool.GetObject(ctx, extCfg, key)
}

// CopyObjectResult is the XML response for CopyObject.
// Defined in s3types with a ToXML method for serialization.
func init() {
	// Ensure aws SDK types are used
	_ = aws.String("")
}
