package store

import (
	"zapfs/pkg/debug"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ChunkTotalCount tracks the total number of chunks in the index
	ChunkTotalCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "storage",
		Name:      "chunks_total",
		Help:      "Total number of chunks in the index",
	})

	// ChunkTotalBytes tracks the total bytes across all chunks
	ChunkTotalBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "storage",
		Name:      "chunks_bytes_total",
		Help:      "Total bytes across all chunks",
	})

	// ChunkZeroRefCount tracks chunks with RefCount=0 (eligible for GC)
	ChunkZeroRefCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "storage",
		Name:      "chunks_zero_ref_total",
		Help:      "Number of chunks with zero reference count (eligible for GC)",
	})

	// ChunkZeroRefBytes tracks bytes in chunks with RefCount=0
	ChunkZeroRefBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "storage",
		Name:      "chunks_zero_ref_bytes_total",
		Help:      "Bytes in chunks with zero reference count",
	})

	// ChunkOperations tracks chunk operations by type
	ChunkOperations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "storage",
		Name:      "chunk_operations_total",
		Help:      "Total number of chunk operations",
	}, []string{"operation"}) // operation: "create", "deduplicate", "delete"

	// ChunkDedupeHits tracks successful deduplication hits
	ChunkDedupeHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "storage",
		Name:      "chunk_dedupe_hits_total",
		Help:      "Number of times a chunk was deduplicated instead of stored",
	})
)

func init() {
	// Register metrics with the global registry
	debug.Registry().MustRegister(
		ChunkTotalCount,
		ChunkTotalBytes,
		ChunkZeroRefCount,
		ChunkZeroRefBytes,
		ChunkOperations,
		ChunkDedupeHits,
	)
}
