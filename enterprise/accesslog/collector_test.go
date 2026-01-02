//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package accesslog

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStore is a simple mock for testing the collector.
type mockStore struct {
	events      []AccessLogEvent
	insertCount atomic.Int64
}

func (m *mockStore) InsertEvents(ctx context.Context, events []AccessLogEvent) error {
	m.events = append(m.events, events...)
	m.insertCount.Add(int64(len(events)))
	return nil
}

func (m *mockStore) QueryLogs(ctx context.Context, bucket string, start, end time.Time, limit int) ([]AccessLogEvent, error) {
	return nil, nil
}

func (m *mockStore) QueryLogsForExport(ctx context.Context, bucket string, since time.Time, limit int) ([]AccessLogEvent, error) {
	return nil, nil
}

func (m *mockStore) GetStats(ctx context.Context, bucket string, start, end time.Time) (*BucketLogStats, error) {
	return nil, nil
}

func (m *mockStore) Close() error {
	return nil
}

func TestCollector_Record(t *testing.T) {
	store := &mockStore{}
	cfg := DefaultConfig()
	cfg.BatchSize = 5
	cfg.FlushInterval = 100 * time.Millisecond

	c := NewCollector(cfg, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	// Record some events
	for i := 0; i < 3; i++ {
		c.Record(&AccessLogEvent{
			RequestID: "req-" + string(rune('0'+i)),
			Bucket:    "test-bucket",
			Operation: "REST.GET.OBJECT",
		})
	}

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	// Should have been flushed
	assert.Equal(t, int64(3), store.insertCount.Load())
}

func TestCollector_BatchFlush(t *testing.T) {
	store := &mockStore{}
	cfg := DefaultConfig()
	cfg.BatchSize = 3
	cfg.FlushInterval = 10 * time.Second // Long interval, batch should trigger

	c := NewCollector(cfg, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Start(ctx)
	defer c.Stop()

	// Record exactly batch size
	for i := 0; i < 3; i++ {
		c.Record(&AccessLogEvent{
			RequestID: "req-" + string(rune('0'+i)),
			Bucket:    "test-bucket",
		})
	}

	// Wait a bit for batch flush
	time.Sleep(100 * time.Millisecond)

	// Should have been flushed due to batch size
	assert.Equal(t, int64(3), store.insertCount.Load())
}

func TestCollector_Stop(t *testing.T) {
	store := &mockStore{}
	cfg := DefaultConfig()
	cfg.BatchSize = 100
	cfg.FlushInterval = 10 * time.Second

	c := NewCollector(cfg, store)
	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)

	// Record some events
	c.Record(&AccessLogEvent{
		RequestID: "req-1",
		Bucket:    "test-bucket",
	})

	// Stop should flush remaining events
	c.Stop()
	cancel()

	// Should have been flushed on stop
	assert.Equal(t, int64(1), store.insertCount.Load())
}

func TestNopCollector(t *testing.T) {
	c := &NopCollector{}
	ctx := context.Background()

	// Should not panic
	c.Record(&AccessLogEvent{})
	c.Start(ctx)
	c.Stop()
	require.NoError(t, c.Flush(ctx))
}

func TestNopStore(t *testing.T) {
	s := &NopStore{}
	ctx := context.Background()

	// Should not error
	require.NoError(t, s.InsertEvents(ctx, nil))
	require.NoError(t, s.Close())

	logs, err := s.QueryLogs(ctx, "bucket", time.Now(), time.Now(), 100)
	require.NoError(t, err)
	assert.Empty(t, logs)
}
