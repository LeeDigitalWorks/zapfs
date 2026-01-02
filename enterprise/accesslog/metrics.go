//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package accesslog

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds Prometheus metrics for the access log system.
type Metrics struct {
	// Collector metrics
	EventsBuffered prometheus.Counter
	EventsDropped  prometheus.Counter
	EventsFlushed  prometheus.Counter
	FlushErrors    prometheus.Counter
	FlushDuration  prometheus.Histogram

	// Exporter metrics
	ExportsTotal   prometheus.Counter
	ExportErrors   prometheus.Counter
	ExportDuration prometheus.Histogram
	ExportedLogs   prometheus.Counter

	// Store metrics
	InsertDuration prometheus.Histogram
	QueryDuration  prometheus.Histogram
}

var (
	metricsOnce     sync.Once
	metricsInstance *Metrics
)

// NewMetrics creates a new Metrics instance with registered Prometheus metrics.
// Metrics are only registered once (singleton pattern to avoid double registration).
func NewMetrics() *Metrics {
	metricsOnce.Do(func() {
		metricsInstance = &Metrics{
			EventsBuffered: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "events_buffered_total",
				Help:      "Total number of access log events buffered",
			}),
			EventsDropped: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "events_dropped_total",
				Help:      "Total number of access log events dropped due to full buffer",
			}),
			EventsFlushed: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "events_flushed_total",
				Help:      "Total number of access log events flushed to storage",
			}),
			FlushErrors: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "flush_errors_total",
				Help:      "Total number of access log flush errors",
			}),
			FlushDuration: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "flush_duration_seconds",
				Help:      "Duration of access log batch flushes",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
			}),
			ExportsTotal: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "exports_total",
				Help:      "Total number of S3 log exports",
			}),
			ExportErrors: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "export_errors_total",
				Help:      "Total number of S3 log export errors",
			}),
			ExportDuration: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "export_duration_seconds",
				Help:      "Duration of S3 log exports",
				Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12), // 100ms to ~200s
			}),
			ExportedLogs: promauto.NewCounter(prometheus.CounterOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "exported_logs_total",
				Help:      "Total number of log entries exported to S3",
			}),
			InsertDuration: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "insert_duration_seconds",
				Help:      "Duration of ClickHouse batch inserts",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
			}),
			QueryDuration: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: "zapfs",
				Subsystem: "accesslog",
				Name:      "query_duration_seconds",
				Help:      "Duration of ClickHouse queries",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
			}),
		}
	})
	return metricsInstance
}
