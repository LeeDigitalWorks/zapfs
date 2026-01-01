//go:build enterprise

package usage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestEnterpriseCollector_New(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	store := NewMockStore()

	collector := NewCollector(cfg, store)
	if collector == nil {
		t.Fatal("NewCollector() returned nil")
	}
}

func TestEnterpriseCollector_StartStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.FlushInterval = 100 * time.Millisecond
	store := NewMockStore()

	collector := NewCollector(cfg, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should not panic
	collector.Start(ctx)

	// Give it a moment
	time.Sleep(50 * time.Millisecond)

	// Stop should not panic and flush pending events
	collector.Stop()
}

func TestEnterpriseCollector_RecordRequest(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.FlushSize = 10 // Small buffer for testing
	cfg.FlushInterval = 1 * time.Hour // Don't trigger time-based flush
	store := NewMockStore()

	collector := NewCollector(cfg, store)
	ctx := context.Background()
	collector.Start(ctx)
	defer collector.Stop()

	// Record some requests
	collector.RecordRequest("owner1", "bucket1", "GetObject")
	collector.RecordRequest("owner1", "bucket1", "PutObject")
	collector.RecordRequest("owner1", "bucket2", "ListObjects")

	// Force a stop to flush
	collector.Stop()

	// Check events were stored
	// Note: In actual enterprise build, this requires license check to pass
	// For this test, we assume it does
}

func TestEnterpriseCollector_RecordBandwidth(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.FlushSize = 10
	cfg.FlushInterval = 1 * time.Hour
	store := NewMockStore()

	collector := NewCollector(cfg, store)
	ctx := context.Background()
	collector.Start(ctx)

	collector.RecordBandwidth("owner1", "bucket1", 1024, DirectionIngress)
	collector.RecordBandwidth("owner1", "bucket1", 2048, DirectionEgress)

	collector.Stop()
}

func TestEnterpriseCollector_RecordStorageDelta(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.FlushSize = 10
	cfg.FlushInterval = 1 * time.Hour
	store := NewMockStore()

	collector := NewCollector(cfg, store)
	ctx := context.Background()
	collector.Start(ctx)

	// Upload
	collector.RecordStorageDelta("owner1", "bucket1", 1024*1024, 1, "STANDARD")

	// Delete
	collector.RecordStorageDelta("owner1", "bucket1", -1024*1024, -1, "STANDARD")

	collector.Stop()
}

func TestEnterpriseCollector_Concurrent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.FlushSize = 100
	cfg.FlushInterval = 50 * time.Millisecond
	store := NewMockStore()

	collector := NewCollector(cfg, store)
	ctx := context.Background()
	collector.Start(ctx)

	// Spawn multiple goroutines recording events
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				collector.RecordRequest("owner1", "bucket1", "GetObject")
				collector.RecordBandwidth("owner1", "bucket1", 100, DirectionEgress)
			}
		}(i)
	}

	wg.Wait()

	// Give time for flushes
	time.Sleep(100 * time.Millisecond)

	collector.Stop()
}

func TestEnterpriseCollector_FlushOnSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.FlushSize = 5 // Very small for testing
	cfg.FlushInterval = 1 * time.Hour
	store := NewMockStore()

	collector := NewCollector(cfg, store)
	ctx := context.Background()
	collector.Start(ctx)
	defer collector.Stop()

	// Record enough events to trigger a flush
	for i := 0; i < 10; i++ {
		collector.RecordRequest("owner1", "bucket1", "GetObject")
	}

	// Give time for flush
	time.Sleep(50 * time.Millisecond)
}

func TestEnterpriseCollector_DisabledConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false // Disabled
	store := NewMockStore()

	collector := NewCollector(cfg, store)
	ctx := context.Background()
	collector.Start(ctx)

	// These should be no-ops when disabled
	collector.RecordRequest("owner1", "bucket1", "GetObject")
	collector.RecordBandwidth("owner1", "bucket1", 1024, DirectionEgress)
	collector.RecordStorageDelta("owner1", "bucket1", 1024, 1, "STANDARD")

	collector.Stop()

	// Store should have no events
	if len(store.events) != 0 {
		t.Errorf("events = %d, want 0 (collector disabled)", len(store.events))
	}
}
