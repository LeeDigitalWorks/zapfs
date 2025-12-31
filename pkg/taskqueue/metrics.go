package taskqueue

import (
	"github.com/LeeDigitalWorks/zapfs/pkg/debug"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TasksProcessedTotal tracks total tasks processed by type and status
	TasksProcessedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "taskqueue",
		Name:      "tasks_processed_total",
		Help:      "Total number of tasks processed",
	}, []string{"type", "status"}) // status: "completed", "failed", "no_handler"

	// TaskProcessingDuration tracks task processing time by type
	TaskProcessingDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "zapfs",
		Subsystem: "taskqueue",
		Name:      "task_processing_duration_seconds",
		Help:      "Time spent processing tasks",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"type"})

	// TasksEnqueuedTotal tracks total tasks enqueued by type
	TasksEnqueuedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "taskqueue",
		Name:      "tasks_enqueued_total",
		Help:      "Total number of tasks enqueued",
	}, []string{"type"})

	// TaskRetries tracks task retry counts
	TaskRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "taskqueue",
		Name:      "task_retries_total",
		Help:      "Total number of task retries",
	}, []string{"type"})

	// QueueDepth tracks current queue depth by status
	QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "taskqueue",
		Name:      "queue_depth",
		Help:      "Current number of tasks in queue by status",
	}, []string{"status"}) // status: "pending", "running", "failed"

	// WorkerActive tracks number of active workers
	WorkerActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "taskqueue",
		Name:      "workers_active",
		Help:      "Number of active worker goroutines",
	})

	// DequeueErrors tracks dequeue operation errors
	DequeueErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "taskqueue",
		Name:      "dequeue_errors_total",
		Help:      "Total number of dequeue errors",
	})
)

func init() {
	debug.Registry().MustRegister(
		TasksProcessedTotal,
		TaskProcessingDuration,
		TasksEnqueuedTotal,
		TaskRetries,
		QueueDepth,
		WorkerActive,
		DequeueErrors,
	)
}
