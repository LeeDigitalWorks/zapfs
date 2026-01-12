// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/debug"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Request metrics
	selectRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "s3select",
			Name:      "requests_total",
			Help:      "Total number of S3 Select requests",
		},
		[]string{"input_format", "status"},
	)

	selectRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "zapfs",
			Subsystem: "s3select",
			Name:      "request_duration_seconds",
			Help:      "Duration of S3 Select requests",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 15), // 10ms to ~5min
		},
		[]string{"input_format"},
	)

	// Data metrics
	selectBytesScanned = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "s3select",
			Name:      "bytes_scanned_total",
			Help:      "Total bytes scanned by S3 Select",
		},
		[]string{"input_format"},
	)

	selectBytesReturned = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "s3select",
			Name:      "bytes_returned_total",
			Help:      "Total bytes returned by S3 Select",
		},
		[]string{"input_format"},
	)

	selectRecordsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "s3select",
			Name:      "records_processed_total",
			Help:      "Total records processed by S3 Select",
		},
		[]string{"input_format"},
	)

	// Error metrics
	selectErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "s3select",
			Name:      "errors_total",
			Help:      "Total S3 Select errors",
		},
		[]string{"input_format", "error_code"},
	)
)

func init() {
	// Register metrics with the global registry
	debug.Registry().MustRegister(
		selectRequestsTotal,
		selectRequestDuration,
		selectBytesScanned,
		selectBytesReturned,
		selectRecordsProcessed,
		selectErrors,
	)
}

// RecordRequest records metrics for a completed S3 Select request.
func RecordRequest(inputFormat string, duration time.Duration, bytesScanned, bytesReturned, recordsProcessed int64, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}

	selectRequestsTotal.WithLabelValues(inputFormat, status).Inc()
	selectRequestDuration.WithLabelValues(inputFormat).Observe(duration.Seconds())
	selectBytesScanned.WithLabelValues(inputFormat).Add(float64(bytesScanned))
	selectBytesReturned.WithLabelValues(inputFormat).Add(float64(bytesReturned))
	selectRecordsProcessed.WithLabelValues(inputFormat).Add(float64(recordsProcessed))
}

// RecordError records an S3 Select error.
func RecordError(inputFormat, errorCode string) {
	selectErrors.WithLabelValues(inputFormat, errorCode).Inc()
}

// GetInputFormat returns the format string for metrics labels.
func GetInputFormat(csv, json, parquet bool) string {
	switch {
	case csv:
		return "csv"
	case json:
		return "json"
	case parquet:
		return "parquet"
	default:
		return "unknown"
	}
}
