// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/federation"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for external writer
var (
	externalWriteTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_external_write_total",
			Help: "Total number of writes to external S3",
		},
		[]string{"bucket", "status"},
	)
	externalWriteBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_external_write_bytes_total",
			Help: "Total bytes written to external S3",
		},
		[]string{"bucket"},
	)
	externalWriteDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "zapfs_federation_external_write_duration_seconds",
			Help:    "Duration of external S3 write operations in seconds",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		},
		[]string{"bucket"},
	)
)

// ExternalWriter provides streaming writes to external S3.
// It uses io.Pipe to allow concurrent streaming while PutObject uploads.
type ExternalWriter struct {
	clientPool *federation.ClientPool
}

// NewExternalWriter creates a new external writer.
func NewExternalWriter(clientPool *federation.ClientPool) *ExternalWriter {
	return &ExternalWriter{
		clientPool: clientPool,
	}
}

// ExternalWriteRequest contains parameters for writing to external S3.
type ExternalWriteRequest struct {
	Config      *federation.ExternalS3Config
	Key         string
	Size        int64
	ContentType string
}

// ExternalWriteResult contains the result of an external write.
type ExternalWriteResult struct {
	ETag      string
	VersionID string
}

// WriteAsync starts an asynchronous write to external S3.
// Returns a pipe writer that should be written to, and a result channel.
// The caller should close the writer when done, then wait on the result channel.
func (e *ExternalWriter) WriteAsync(ctx context.Context, req *ExternalWriteRequest) (io.WriteCloser, <-chan ExternalWriteAsyncResult) {
	pr, pw := io.Pipe()
	resultCh := make(chan ExternalWriteAsyncResult, 1)

	go func() {
		defer close(resultCh)

		startTime := time.Now()
		output, err := e.clientPool.PutObject(ctx, req.Config, req.Key, pr, req.Size, req.ContentType)
		duration := time.Since(startTime).Seconds()

		var result ExternalWriteAsyncResult
		if err != nil {
			result.Err = fmt.Errorf("external S3 write: %w", err)
			externalWriteTotal.WithLabelValues(req.Config.Bucket, "error").Inc()
		} else {
			if output.ETag != nil {
				result.ETag = *output.ETag
			}
			if output.VersionId != nil {
				result.VersionID = *output.VersionId
			}
			externalWriteTotal.WithLabelValues(req.Config.Bucket, "success").Inc()
			externalWriteBytesTotal.WithLabelValues(req.Config.Bucket).Add(float64(req.Size))
			externalWriteDuration.WithLabelValues(req.Config.Bucket).Observe(duration)
		}

		resultCh <- result
	}()

	return pw, resultCh
}

// ExternalWriteAsyncResult contains the async result of an external write.
type ExternalWriteAsyncResult struct {
	ETag      string
	VersionID string
	Err       error
}

// DualWriter writes to both local storage and external S3 simultaneously.
// It uses io.MultiWriter to tee the data to both destinations.
type DualWriter struct {
	localCoordinator *Coordinator
	externalWriter   *ExternalWriter
}

// NewDualWriter creates a new dual writer.
func NewDualWriter(coordinator *Coordinator, clientPool *federation.ClientPool) *DualWriter {
	return &DualWriter{
		localCoordinator: coordinator,
		externalWriter:   NewExternalWriter(clientPool),
	}
}

// DualWriteRequest contains parameters for dual-write operations.
type DualWriteRequest struct {
	// Local write parameters
	Bucket      string
	ObjectID    string
	Key         string // The object key (for external S3)
	Body        io.Reader
	Size        uint64
	ProfileName string
	Replication int
	ContentType string

	// External S3 configuration
	ExternalConfig *federation.ExternalS3Config
}

// DualWriteResult contains the result of a dual-write operation.
type DualWriteResult struct {
	// Local write result
	LocalResult *WriteResult

	// External write result (nil if external write disabled or failed)
	ExternalETag      string
	ExternalVersionID string
	ExternalError     error // Non-nil if external write failed (local still succeeded)
}

// Write performs a dual-write to both local storage and external S3.
// Local write is required to succeed; external write failure is logged but not fatal.
// Uses io.MultiWriter to stream data to both destinations simultaneously.
func (d *DualWriter) Write(ctx context.Context, req *DualWriteRequest) (*DualWriteResult, error) {
	// Create pipe for external S3 upload
	extReq := &ExternalWriteRequest{
		Config:      req.ExternalConfig,
		Key:         req.Key,
		Size:        int64(req.Size),
		ContentType: req.ContentType,
	}
	extWriter, extResultCh := d.externalWriter.WriteAsync(ctx, extReq)

	// Create a pipe for local storage
	localPR, localPW := io.Pipe()

	// Use MultiWriter to tee data to both local and external
	multiWriter := io.MultiWriter(localPW, extWriter)

	// Track results
	var wg sync.WaitGroup
	var localResult *WriteResult
	var localErr error

	// Start local write in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer localPR.Close()

		localResult, localErr = d.localCoordinator.WriteObject(ctx, &WriteRequest{
			Bucket:      req.Bucket,
			ObjectID:    req.ObjectID,
			Body:        localPR,
			Size:        req.Size,
			ProfileName: req.ProfileName,
			Replication: req.Replication,
		})
	}()

	// Copy input to both destinations
	buf := make([]byte, 64*1024) // 64KB buffer
	copyErr := func() error {
		defer localPW.Close()
		defer extWriter.Close()

		for {
			n, err := req.Body.Read(buf)
			if n > 0 {
				if _, writeErr := multiWriter.Write(buf[:n]); writeErr != nil {
					return fmt.Errorf("multi-write: %w", writeErr)
				}
			}
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("read input: %w", err)
			}
		}
	}()

	// Wait for local write to complete
	wg.Wait()

	// Wait for external write result
	extResult := <-extResultCh

	// If copy failed, return error
	if copyErr != nil {
		return nil, copyErr
	}

	// If local write failed, return error (this is required to succeed)
	if localErr != nil {
		return nil, localErr
	}

	// Build result
	result := &DualWriteResult{
		LocalResult: localResult,
	}

	// External write is optional - log errors but don't fail
	if extResult.Err != nil {
		result.ExternalError = extResult.Err
		logger.Warn().
			Err(extResult.Err).
			Str("bucket", req.Bucket).
			Str("key", req.Key).
			Str("external_bucket", req.ExternalConfig.Bucket).
			Msg("External S3 write failed during dual-write (local succeeded)")
	} else {
		result.ExternalETag = extResult.ETag
		result.ExternalVersionID = extResult.VersionID
	}

	return result, nil
}
