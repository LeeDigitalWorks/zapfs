//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package accesslog

import (
	"context"
	"time"
)

// Store defines the interface for access log persistence.
type Store interface {
	// InsertEvents batch inserts access log events.
	InsertEvents(ctx context.Context, events []AccessLogEvent) error

	// QueryLogs retrieves logs for a bucket within a time range.
	// Results are ordered by event_time descending.
	QueryLogs(ctx context.Context, bucket string, start, end time.Time, limit int) ([]AccessLogEvent, error)

	// QueryLogsForExport retrieves logs for a bucket since lastExport for S3 delivery.
	// Results are ordered by event_time ascending for sequential export.
	QueryLogsForExport(ctx context.Context, bucket string, since time.Time, limit int) ([]AccessLogEvent, error)

	// GetStats returns aggregate statistics for a bucket.
	GetStats(ctx context.Context, bucket string, start, end time.Time) (*BucketLogStats, error)

	// Close releases resources.
	Close() error
}

// BucketLogStats holds aggregate statistics for a bucket.
type BucketLogStats struct {
	TotalRequests   int64
	TotalBytesSent  int64
	UniqueRequesters int64
	ErrorCount      int64

	// Request counts by operation type
	GetRequests    int64
	PutRequests    int64
	DeleteRequests int64
	ListRequests   int64
	OtherRequests  int64
}

// Collector defines the interface for buffering and batching access log events.
type Collector interface {
	// Record captures an access log event asynchronously.
	// This method is non-blocking and safe for concurrent use.
	Record(event *AccessLogEvent)

	// Start begins background event flushing.
	Start(ctx context.Context)

	// Stop gracefully shuts down, flushing any pending events.
	Stop()

	// Flush immediately flushes pending events to the store.
	Flush(ctx context.Context) error
}

// NopStore is a no-op implementation for when access logging is disabled.
type NopStore struct{}

func (NopStore) InsertEvents(_ context.Context, _ []AccessLogEvent) error { return nil }
func (NopStore) QueryLogs(_ context.Context, _ string, _, _ time.Time, _ int) ([]AccessLogEvent, error) {
	return nil, nil
}
func (NopStore) QueryLogsForExport(_ context.Context, _ string, _ time.Time, _ int) ([]AccessLogEvent, error) {
	return nil, nil
}
func (NopStore) GetStats(_ context.Context, _ string, _, _ time.Time) (*BucketLogStats, error) {
	return &BucketLogStats{}, nil
}
func (NopStore) Close() error { return nil }

// NopCollector is a no-op implementation for when access logging is disabled.
type NopCollector struct{}

func (NopCollector) Record(_ *AccessLogEvent)         {}
func (NopCollector) Start(_ context.Context)          {}
func (NopCollector) Stop()                            {}
func (NopCollector) Flush(_ context.Context) error    { return nil }
