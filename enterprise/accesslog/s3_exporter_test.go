//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package accesslog

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Mock Implementations
// =============================================================================

// exporterMockStore implements Store for S3Exporter testing
type exporterMockStore struct {
	logs             []AccessLogEvent
	queryErr         error
	insertErr        error
	queryForExportFn func(ctx context.Context, bucket string, since time.Time, limit int) ([]AccessLogEvent, error)
}

func (m *exporterMockStore) InsertEvents(_ context.Context, events []AccessLogEvent) error {
	if m.insertErr != nil {
		return m.insertErr
	}
	m.logs = append(m.logs, events...)
	return nil
}

func (m *exporterMockStore) QueryLogs(_ context.Context, bucket string, _, _ time.Time, limit int) ([]AccessLogEvent, error) {
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	var result []AccessLogEvent
	for _, log := range m.logs {
		if log.Bucket == bucket {
			result = append(result, log)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *exporterMockStore) QueryLogsForExport(ctx context.Context, bucket string, since time.Time, limit int) ([]AccessLogEvent, error) {
	if m.queryForExportFn != nil {
		return m.queryForExportFn(ctx, bucket, since, limit)
	}
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	var result []AccessLogEvent
	for _, log := range m.logs {
		if log.Bucket == bucket && log.EventTime.After(since) {
			result = append(result, log)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *exporterMockStore) GetStats(_ context.Context, _ string, _, _ time.Time) (*BucketLogStats, error) {
	return &BucketLogStats{}, nil
}

func (m *exporterMockStore) Close() error {
	return nil
}

// mockLoggingConfigStore implements LoggingConfigStore for testing
type mockLoggingConfigStore struct {
	configs   map[string]*BucketLoggingConfig
	listErr   error
	setErr    error
	getErr    error
	deleteErr error
}

func newMockLoggingConfigStore() *mockLoggingConfigStore {
	return &mockLoggingConfigStore{
		configs: make(map[string]*BucketLoggingConfig),
	}
}

func (m *mockLoggingConfigStore) GetBucketLogging(_ context.Context, bucket string) (*BucketLoggingConfig, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	cfg, ok := m.configs[bucket]
	if !ok {
		return nil, nil
	}
	return cfg, nil
}

func (m *mockLoggingConfigStore) SetBucketLogging(_ context.Context, config *BucketLoggingConfig) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.configs[config.SourceBucket] = config
	return nil
}

func (m *mockLoggingConfigStore) DeleteBucketLogging(_ context.Context, bucket string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.configs, bucket)
	return nil
}

func (m *mockLoggingConfigStore) ListLoggingConfigs(_ context.Context) ([]*BucketLoggingConfig, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	result := make([]*BucketLoggingConfig, 0, len(m.configs))
	for _, cfg := range m.configs {
		result = append(result, cfg)
	}
	return result, nil
}

// mockObjectWriter implements ObjectWriter for testing
type mockObjectWriter struct {
	mu       sync.Mutex
	objects  map[string][]byte
	writeErr error
}

func newMockObjectWriter() *mockObjectWriter {
	return &mockObjectWriter{
		objects: make(map[string][]byte),
	}
}

func (m *mockObjectWriter) WriteObject(_ context.Context, bucket, key string, data []byte, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	m.objects[bucket+"/"+key] = data
	return nil
}

func (m *mockObjectWriter) getWrittenCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.objects)
}

// =============================================================================
// NewS3Exporter Tests
// =============================================================================

func TestNewS3Exporter(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExportInterval: time.Minute,
		ExportBatch:    1000,
	}
	store := &exporterMockStore{}
	loggingStore := newMockLoggingConfigStore()
	writer := newMockObjectWriter()

	exporter := NewS3Exporter(cfg, store, loggingStore, writer)

	assert.NotNil(t, exporter)
	assert.NotNil(t, exporter.lastExport)
	assert.NotNil(t, exporter.metrics)
}

// =============================================================================
// Start/Stop Tests
// =============================================================================

func TestS3Exporter_StartStop(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExportInterval: 50 * time.Millisecond,
		ExportBatch:    1000,
	}
	store := &exporterMockStore{}
	loggingStore := newMockLoggingConfigStore()
	writer := newMockObjectWriter()

	exporter := NewS3Exporter(cfg, store, loggingStore, writer)

	ctx := context.Background()
	exporter.Start(ctx)

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)

	exporter.Stop()

	// Should not panic or hang
}

// =============================================================================
// ExportNow Tests
// =============================================================================

func TestS3Exporter_ExportNow_NoConfigs(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExportInterval: time.Minute,
		ExportBatch:    1000,
	}
	store := &exporterMockStore{}
	loggingStore := newMockLoggingConfigStore() // Empty
	writer := newMockObjectWriter()

	exporter := NewS3Exporter(cfg, store, loggingStore, writer)
	exporter.ExportNow(context.Background())

	// No objects written since no configs
	assert.Equal(t, 0, writer.getWrittenCount())
}

func TestS3Exporter_ExportNow_ListConfigsError(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExportInterval: time.Minute,
		ExportBatch:    1000,
	}
	store := &exporterMockStore{}
	loggingStore := newMockLoggingConfigStore()
	loggingStore.listErr = errors.New("list failed")
	writer := newMockObjectWriter()

	exporter := NewS3Exporter(cfg, store, loggingStore, writer)
	exporter.ExportNow(context.Background())

	// Should handle error gracefully
	assert.Equal(t, 0, writer.getWrittenCount())
}

func TestS3Exporter_ExportNow_WithLogs(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExportInterval: time.Minute,
		ExportBatch:    1000,
	}

	eventTime := time.Now()
	store := &exporterMockStore{
		logs: []AccessLogEvent{
			{
				RequestID: "event-1",
				Bucket:    "source-bucket",
				ObjectKey: "test/object.txt",
				Operation: "REST.GET.OBJECT",
				EventTime: eventTime,
			},
		},
	}

	loggingStore := newMockLoggingConfigStore()
	loggingStore.configs["source-bucket"] = &BucketLoggingConfig{
		ID:           "config-1",
		SourceBucket: "source-bucket",
		TargetBucket: "logs-bucket",
		TargetPrefix: "logs/",
	}

	writer := newMockObjectWriter()

	exporter := NewS3Exporter(cfg, store, loggingStore, writer)
	exporter.ExportNow(context.Background())

	// Should have written one log file
	assert.Equal(t, 1, writer.getWrittenCount())
}

func TestS3Exporter_ExportNow_NoLogs(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExportInterval: time.Minute,
		ExportBatch:    1000,
	}
	store := &exporterMockStore{logs: []AccessLogEvent{}} // No logs

	loggingStore := newMockLoggingConfigStore()
	loggingStore.configs["source-bucket"] = &BucketLoggingConfig{
		ID:           "config-1",
		SourceBucket: "source-bucket",
		TargetBucket: "logs-bucket",
		TargetPrefix: "logs/",
	}

	writer := newMockObjectWriter()

	exporter := NewS3Exporter(cfg, store, loggingStore, writer)
	exporter.ExportNow(context.Background())

	// No objects written since no logs
	assert.Equal(t, 0, writer.getWrittenCount())
}

func TestS3Exporter_ExportNow_QueryError(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExportInterval: time.Minute,
		ExportBatch:    1000,
	}
	store := &exporterMockStore{queryErr: errors.New("query failed")}

	loggingStore := newMockLoggingConfigStore()
	loggingStore.configs["source-bucket"] = &BucketLoggingConfig{
		ID:           "config-1",
		SourceBucket: "source-bucket",
		TargetBucket: "logs-bucket",
		TargetPrefix: "logs/",
	}

	writer := newMockObjectWriter()

	exporter := NewS3Exporter(cfg, store, loggingStore, writer)
	exporter.ExportNow(context.Background())

	// No objects written due to error
	assert.Equal(t, 0, writer.getWrittenCount())
}

func TestS3Exporter_ExportNow_WriteError(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExportInterval: time.Minute,
		ExportBatch:    1000,
	}

	eventTime := time.Now()
	store := &exporterMockStore{
		logs: []AccessLogEvent{
			{
				RequestID: "event-1",
				Bucket:    "source-bucket",
				Operation: "REST.GET.OBJECT",
				EventTime: eventTime,
			},
		},
	}

	loggingStore := newMockLoggingConfigStore()
	loggingStore.configs["source-bucket"] = &BucketLoggingConfig{
		ID:           "config-1",
		SourceBucket: "source-bucket",
		TargetBucket: "logs-bucket",
		TargetPrefix: "logs/",
	}

	writer := newMockObjectWriter()
	writer.writeErr = errors.New("write failed")

	exporter := NewS3Exporter(cfg, store, loggingStore, writer)
	exporter.ExportNow(context.Background())

	// No objects written due to write error
	assert.Equal(t, 0, writer.getWrittenCount())
}

func TestS3Exporter_ExportNow_MultipleBuckets(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExportInterval: time.Minute,
		ExportBatch:    1000,
	}

	eventTime := time.Now()
	store := &exporterMockStore{
		logs: []AccessLogEvent{
			{RequestID: "event-1", Bucket: "bucket-a", Operation: "REST.GET.OBJECT", EventTime: eventTime},
			{RequestID: "event-2", Bucket: "bucket-b", Operation: "REST.PUT.OBJECT", EventTime: eventTime},
		},
	}

	loggingStore := newMockLoggingConfigStore()
	loggingStore.configs["bucket-a"] = &BucketLoggingConfig{
		ID:           "config-a",
		SourceBucket: "bucket-a",
		TargetBucket: "logs-bucket",
		TargetPrefix: "a/",
	}
	loggingStore.configs["bucket-b"] = &BucketLoggingConfig{
		ID:           "config-b",
		SourceBucket: "bucket-b",
		TargetBucket: "logs-bucket",
		TargetPrefix: "b/",
	}

	writer := newMockObjectWriter()

	exporter := NewS3Exporter(cfg, store, loggingStore, writer)
	exporter.ExportNow(context.Background())

	// Should have written log files for both buckets
	assert.Equal(t, 2, writer.getWrittenCount())
}

// =============================================================================
// Last Export Time Tracking Tests
// =============================================================================

func TestS3Exporter_TracksLastExportTime(t *testing.T) {
	t.Parallel()

	cfg := Config{
		ExportInterval: time.Minute,
		ExportBatch:    1000,
	}

	eventTime := time.Now()
	store := &exporterMockStore{
		logs: []AccessLogEvent{
			{RequestID: "event-1", Bucket: "source-bucket", Operation: "REST.GET.OBJECT", EventTime: eventTime},
		},
	}

	loggingStore := newMockLoggingConfigStore()
	loggingStore.configs["source-bucket"] = &BucketLoggingConfig{
		ID:           "config-1",
		SourceBucket: "source-bucket",
		TargetBucket: "logs-bucket",
		TargetPrefix: "logs/",
	}

	writer := newMockObjectWriter()

	exporter := NewS3Exporter(cfg, store, loggingStore, writer)

	// First export
	exporter.ExportNow(context.Background())
	assert.Equal(t, 1, writer.getWrittenCount())

	// Check last export time was set
	exporter.lastExportMu.RLock()
	lastExport := exporter.lastExport["source-bucket"]
	exporter.lastExportMu.RUnlock()

	assert.Equal(t, eventTime, lastExport)

	// Second export - no new logs after lastExport
	exporter.ExportNow(context.Background())

	// Should still be 1 since no new logs
	assert.Equal(t, 1, writer.getWrittenCount())
}

// =============================================================================
// SimpleObjectWriter Tests
// =============================================================================

func TestSimpleObjectWriter(t *testing.T) {
	t.Parallel()

	var called bool
	writer := &SimpleObjectWriter{
		WriteFn: func(ctx context.Context, bucket, key string, data []byte, contentType string) error {
			called = true
			assert.Equal(t, "my-bucket", bucket)
			assert.Equal(t, "my-key", key)
			assert.Equal(t, []byte("test data"), data)
			assert.Equal(t, "text/plain", contentType)
			return nil
		},
	}

	err := writer.WriteObject(context.Background(), "my-bucket", "my-key", []byte("test data"), "text/plain")
	require.NoError(t, err)
	assert.True(t, called)
}

func TestSimpleObjectWriter_Error(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("write failed")
	writer := &SimpleObjectWriter{
		WriteFn: func(ctx context.Context, bucket, key string, data []byte, contentType string) error {
			return expectedErr
		},
	}

	err := writer.WriteObject(context.Background(), "bucket", "key", nil, "")
	assert.Equal(t, expectedErr, err)
}

// =============================================================================
// BufferedObjectWriter Tests
// =============================================================================

func TestBufferedObjectWriter(t *testing.T) {
	t.Parallel()

	writer := NewBufferedObjectWriter()

	err := writer.WriteObject(context.Background(), "bucket1", "key1", []byte("data1"), "text/plain")
	require.NoError(t, err)

	err = writer.WriteObject(context.Background(), "bucket2", "key2", []byte("data2"), "text/plain")
	require.NoError(t, err)

	assert.Len(t, writer.Objects, 2)
	assert.Equal(t, []byte("data1"), writer.Objects["bucket1/key1"])
	assert.Equal(t, []byte("data2"), writer.Objects["bucket2/key2"])
}

func TestBufferedObjectWriter_ConcurrentWrites(t *testing.T) {
	t.Parallel()

	writer := NewBufferedObjectWriter()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := "key-" + string(rune('a'+idx%26))
			writer.WriteObject(context.Background(), "bucket", key, []byte("data"), "text/plain")
		}(i)
	}
	wg.Wait()

	// All writes should complete without race
	assert.NotEmpty(t, writer.Objects)
}
