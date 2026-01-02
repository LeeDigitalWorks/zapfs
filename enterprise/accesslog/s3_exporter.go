//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package accesslog

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// BucketLoggingConfig holds the logging configuration for a bucket.
type BucketLoggingConfig struct {
	ID           string
	SourceBucket string
	TargetBucket string
	TargetPrefix string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// LoggingConfigStore defines the interface for accessing bucket logging configurations.
type LoggingConfigStore interface {
	GetBucketLogging(ctx context.Context, bucket string) (*BucketLoggingConfig, error)
	SetBucketLogging(ctx context.Context, config *BucketLoggingConfig) error
	DeleteBucketLogging(ctx context.Context, bucket string) error
	ListLoggingConfigs(ctx context.Context) ([]*BucketLoggingConfig, error)
}

// ObjectWriter defines the interface for writing objects to S3.
type ObjectWriter interface {
	WriteObject(ctx context.Context, bucket, key string, data []byte, contentType string) error
}

// S3Exporter exports access logs to S3 buckets in AWS format.
type S3Exporter struct {
	store        Store
	loggingStore LoggingConfigStore
	writer       ObjectWriter
	cfg          Config
	metrics      *Metrics

	// Track last export time per bucket
	lastExport   map[string]time.Time
	lastExportMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewS3Exporter creates a new S3 log exporter.
func NewS3Exporter(cfg Config, store Store, loggingStore LoggingConfigStore, writer ObjectWriter) *S3Exporter {
	cfg.Validate()
	return &S3Exporter{
		store:        store,
		loggingStore: loggingStore,
		writer:       writer,
		cfg:          cfg,
		metrics:      NewMetrics(),
		lastExport:   make(map[string]time.Time),
	}
}

// Start begins the background export loop.
func (e *S3Exporter) Start(ctx context.Context) {
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.wg.Add(1)
	go e.exportLoop()
	log.Info().
		Dur("interval", e.cfg.ExportInterval).
		Msg("S3 access log exporter started")
}

// Stop gracefully shuts down the exporter.
func (e *S3Exporter) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	log.Info().Msg("S3 access log exporter stopped")
}

// exportLoop runs the periodic export job.
func (e *S3Exporter) exportLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.cfg.ExportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			// Final export on shutdown
			e.exportAll(context.Background())
			return
		case <-ticker.C:
			e.exportAll(e.ctx)
		}
	}
}

// exportAll exports logs for all buckets with logging enabled.
func (e *S3Exporter) exportAll(ctx context.Context) {
	configs, err := e.loggingStore.ListLoggingConfigs(ctx)
	if err != nil {
		log.Error().Err(err).Msg("failed to list logging configs")
		return
	}

	for _, config := range configs {
		if err := e.exportBucket(ctx, config); err != nil {
			log.Error().Err(err).
				Str("source", config.SourceBucket).
				Str("target", config.TargetBucket).
				Msg("failed to export logs")
			e.metrics.ExportErrors.Inc()
		}
	}
}

// exportBucket exports logs for a single bucket.
func (e *S3Exporter) exportBucket(ctx context.Context, config *BucketLoggingConfig) error {
	start := time.Now()

	// Get last export time for this bucket
	e.lastExportMu.RLock()
	since := e.lastExport[config.SourceBucket]
	e.lastExportMu.RUnlock()

	if since.IsZero() {
		// First export - start from one interval ago
		since = time.Now().Add(-e.cfg.ExportInterval)
	}

	// Query logs since last export
	logs, err := e.store.QueryLogsForExport(ctx, config.SourceBucket, since, e.cfg.ExportBatch)
	if err != nil {
		return fmt.Errorf("query logs: %w", err)
	}

	if len(logs) == 0 {
		return nil
	}

	// Format logs in AWS format
	formatted := FormatAWSLogs(logs)

	// Generate object key: [prefix]YYYY-MM-DD-HH-MM-SS-UniqueString
	key := fmt.Sprintf("%s%s-%s",
		config.TargetPrefix,
		time.Now().UTC().Format("2006-01-02-15-04-05"),
		uuid.New().String()[:8],
	)

	// Upload to target bucket
	if err := e.writer.WriteObject(ctx, config.TargetBucket, key, formatted, "text/plain"); err != nil {
		return fmt.Errorf("write log object: %w", err)
	}

	// Update last export time
	var lastEventTime time.Time
	if len(logs) > 0 {
		lastEventTime = logs[len(logs)-1].EventTime
	}
	e.lastExportMu.Lock()
	e.lastExport[config.SourceBucket] = lastEventTime
	e.lastExportMu.Unlock()

	// Update metrics
	e.metrics.ExportsTotal.Inc()
	e.metrics.ExportedLogs.Add(float64(len(logs)))
	e.metrics.ExportDuration.Observe(time.Since(start).Seconds())

	log.Info().
		Str("source", config.SourceBucket).
		Str("target", config.TargetBucket).
		Str("key", key).
		Int("count", len(logs)).
		Int("bytes", len(formatted)).
		Dur("duration", time.Since(start)).
		Msg("exported access logs")

	return nil
}

// ExportNow triggers an immediate export for all buckets.
func (e *S3Exporter) ExportNow(ctx context.Context) {
	e.exportAll(ctx)
}

// SimpleObjectWriter is a simple implementation of ObjectWriter using a function.
type SimpleObjectWriter struct {
	WriteFn func(ctx context.Context, bucket, key string, data []byte, contentType string) error
}

func (w *SimpleObjectWriter) WriteObject(ctx context.Context, bucket, key string, data []byte, contentType string) error {
	return w.WriteFn(ctx, bucket, key, data, contentType)
}

// BufferedObjectWriter buffers writes for testing.
type BufferedObjectWriter struct {
	mu      sync.Mutex
	Objects map[string][]byte // key: bucket/key
}

func NewBufferedObjectWriter() *BufferedObjectWriter {
	return &BufferedObjectWriter{
		Objects: make(map[string][]byte),
	}
}

func (w *BufferedObjectWriter) WriteObject(_ context.Context, bucket, key string, data []byte, _ string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	fullKey := bucket + "/" + key
	w.Objects[fullKey] = bytes.Clone(data)
	return nil
}
