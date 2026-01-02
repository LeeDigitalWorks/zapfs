package lifecycle

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Scanner metrics
	scansTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_lifecycle_scans_total",
		Help: "Total number of lifecycle scan runs",
	})

	scanDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "zapfs_lifecycle_scan_duration_seconds",
		Help:    "Duration of lifecycle scans per bucket",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s to ~6.8min
	})

	objectsEvaluated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_lifecycle_objects_evaluated_total",
		Help: "Total objects evaluated for lifecycle rules",
	})

	actionsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zapfs_lifecycle_actions_enqueued_total",
		Help: "Lifecycle actions enqueued by type",
	}, []string{"action"})

	scanErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_lifecycle_scan_errors_total",
		Help: "Total number of scan errors",
	})

	bucketsScanned = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "zapfs_lifecycle_buckets_with_config",
		Help: "Number of buckets with lifecycle configuration",
	})

	// Handler metrics (will be used by the handler)
	ActionsExecuted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zapfs_lifecycle_actions_executed_total",
		Help: "Lifecycle actions executed by type and status",
	}, []string{"action", "status"})

	BytesExpired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_lifecycle_bytes_expired_total",
		Help: "Total bytes deleted by lifecycle expiration",
	})

	BytesTransitioned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_lifecycle_bytes_transitioned_total",
		Help: "Total bytes transitioned by lifecycle (enterprise)",
	})
)
