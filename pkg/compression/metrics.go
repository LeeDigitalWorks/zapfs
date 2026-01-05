// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// CompressionRatioHist tracks compression ratios (original_size / compressed_size)
	CompressionRatioHist = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "zapfs",
			Subsystem: "compression",
			Name:      "ratio",
			Help:      "Compression ratio (original_size / compressed_size)",
			Buckets:   []float64{1.0, 1.25, 1.5, 2.0, 3.0, 4.0, 5.0, 10.0},
		},
		[]string{"algorithm"},
	)

	// CompressionDuration tracks time spent compressing/decompressing
	CompressionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "zapfs",
			Subsystem: "compression",
			Name:      "duration_seconds",
			Help:      "Time spent compressing/decompressing data",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"algorithm", "operation"}, // operation: compress, decompress
	)

	// CompressionBytesIn tracks original bytes before compression
	CompressionBytesIn = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "compression",
			Name:      "bytes_in_total",
			Help:      "Total bytes before compression (original size)",
		},
		[]string{"algorithm"},
	)

	// CompressionBytesOut tracks compressed bytes after compression
	CompressionBytesOut = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "compression",
			Name:      "bytes_out_total",
			Help:      "Total bytes after compression (compressed size)",
		},
		[]string{"algorithm"},
	)

	// CompressionSkipped tracks chunks where compression was skipped
	CompressionSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "compression",
			Name:      "skipped_total",
			Help:      "Chunks where compression was skipped (no space savings)",
		},
		[]string{"algorithm"},
	)

	// DecompressionBytesIn tracks compressed bytes read for decompression
	DecompressionBytesIn = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "decompression",
			Name:      "bytes_in_total",
			Help:      "Total compressed bytes read for decompression",
		},
		[]string{"algorithm"},
	)

	// DecompressionBytesOut tracks decompressed bytes output
	DecompressionBytesOut = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "decompression",
			Name:      "bytes_out_total",
			Help:      "Total bytes after decompression (original size)",
		},
		[]string{"algorithm"},
	)
)

// RecordCompression records metrics for a compression operation
func RecordCompression(algo Algorithm, originalSize, compressedSize int, skipped bool) {
	algoStr := algo.String()

	if skipped {
		CompressionSkipped.WithLabelValues(algoStr).Inc()
		return
	}

	CompressionBytesIn.WithLabelValues(algoStr).Add(float64(originalSize))
	CompressionBytesOut.WithLabelValues(algoStr).Add(float64(compressedSize))

	ratio := CompressionRatio(originalSize, compressedSize)
	CompressionRatioHist.WithLabelValues(algoStr).Observe(ratio)
}

// RecordDecompression records metrics for a decompression operation
func RecordDecompression(algo Algorithm, compressedSize, originalSize int) {
	algoStr := algo.String()

	DecompressionBytesIn.WithLabelValues(algoStr).Add(float64(compressedSize))
	DecompressionBytesOut.WithLabelValues(algoStr).Add(float64(originalSize))
}
