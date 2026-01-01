//go:build !enterprise

package usage

import (
	"context"
)

// stubCollector is a no-op collector for community edition.
type stubCollector struct{}

// NewCollector returns a no-op collector for community edition.
func NewCollector(cfg Config, store Store) Collector {
	return &stubCollector{}
}

func (s *stubCollector) RecordRequest(ownerID, bucket, operation string) {}

func (s *stubCollector) RecordBandwidth(ownerID, bucket string, bytes int64, direction Direction) {}

func (s *stubCollector) RecordStorageDelta(ownerID, bucket string, bytesDelta int64, objectDelta int, storageClass string) {
}

func (s *stubCollector) Start(ctx context.Context) {}

func (s *stubCollector) Stop() {}
