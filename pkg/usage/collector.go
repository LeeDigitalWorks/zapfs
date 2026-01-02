// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package usage

import (
	"context"
)

// Collector defines the interface for collecting usage events.
// The implementation differs between enterprise (real collection with buffering)
// and community (no-op stub) editions.
type Collector interface {
	// RecordRequest records an API request event.
	RecordRequest(ownerID, bucket, operation string)

	// RecordBandwidth records data transfer (ingress/egress).
	RecordBandwidth(ownerID, bucket string, bytes int64, direction Direction)

	// RecordStorageDelta records a change in storage.
	// bytesDelta can be positive (upload) or negative (delete).
	// objectDelta is typically +1 (create) or -1 (delete).
	RecordStorageDelta(ownerID, bucket string, bytesDelta int64, objectDelta int, storageClass string)

	// Start begins background processing (flushing, streaming).
	// Should be called once at service startup.
	Start(ctx context.Context)

	// Stop gracefully shuts down the collector, flushing any pending events.
	Stop()
}
