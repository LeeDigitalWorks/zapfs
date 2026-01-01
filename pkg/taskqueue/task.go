// Package taskqueue provides a durable task queue for background processing.
//
// Supported backends:
// - Database (Vitess/MySQL) - default, uses existing infrastructure
// - In-memory - for testing only
//
// Community use cases:
// - GC RefCount decrements (retry failed operations)
// - Object/version cleanup
// - Lifecycle transitions
//
// Enterprise use cases (requires license):
// - Cross-region replication (CRR)
// - Audit log shipping
// - Webhook delivery
package taskqueue

import (
	"encoding/json"
	"time"
)

// Default configuration values
const (
	DefaultPollInterval       = time.Second
	DefaultConcurrency        = 5
	DefaultVisibilityTimeout  = 5 * time.Minute
	DefaultMaxRetries         = 3
)

// TaskType identifies the type of task for routing to handlers.
type TaskType string

// Community task types
const (
	TaskTypeGCDecrement TaskType = "gc_decrement" // RefCount decrements
	TaskTypeCleanup     TaskType = "cleanup"      // Object/version cleanup
	TaskTypeLifecycle   TaskType = "lifecycle"    // Lifecycle transitions
	TaskTypeEvent       TaskType = "event"        // S3 event notification
)

// EventPayload is the payload for TaskTypeEvent tasks.
// Contains S3 event metadata for delivery to notification destinations.
type EventPayload struct {
	// EventName is the S3 event type (e.g., "s3:ObjectCreated:Put").
	EventName string `json:"event_name"`

	// Bucket is the bucket name where the event occurred.
	Bucket string `json:"bucket"`

	// Key is the object key (empty for bucket-level events).
	Key string `json:"key"`

	// Size is the object size in bytes (for object events).
	Size int64 `json:"size"`

	// ETag is the object's entity tag.
	ETag string `json:"etag"`

	// VersionID is the object version (if versioning enabled).
	VersionID string `json:"version_id,omitempty"`

	// OwnerID is the bucket/object owner's canonical user ID.
	OwnerID string `json:"owner_id"`

	// RequestID is the original S3 request ID.
	RequestID string `json:"request_id"`

	// SourceIP is the client IP that made the request.
	SourceIP string `json:"source_ip"`

	// Timestamp is the event time in Unix milliseconds.
	Timestamp int64 `json:"timestamp"`

	// Sequencer is used for ordering events on the same object.
	Sequencer string `json:"sequencer"`

	// UserAgent is the client's user agent string.
	UserAgent string `json:"user_agent,omitempty"`
}

// TaskStatus represents the current state of a task.
type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"     // Waiting to be picked up
	StatusRunning    TaskStatus = "running"     // Currently being processed
	StatusCompleted  TaskStatus = "completed"   // Successfully finished
	StatusFailed     TaskStatus = "failed"      // Failed, may retry
	StatusDeadLetter TaskStatus = "dead_letter" // Failed permanently
	StatusCancelled  TaskStatus = "cancelled"   // Cancelled by user/system
)

// TaskPriority allows urgent tasks to be processed first.
type TaskPriority int

const (
	PriorityLow    TaskPriority = 0
	PriorityNormal TaskPriority = 5
	PriorityHigh   TaskPriority = 10
	PriorityUrgent TaskPriority = 20
)

// Task represents a unit of work to be processed.
type Task struct {
	// Identification
	ID       string       `json:"id" db:"id"`
	Type     TaskType     `json:"type" db:"type"`
	Status   TaskStatus   `json:"status" db:"status"`
	Priority TaskPriority `json:"priority" db:"priority"`

	// Payload - JSON encoded task-specific data
	Payload json.RawMessage `json:"payload" db:"payload"`

	// Scheduling
	ScheduledAt time.Time  `json:"scheduled_at" db:"scheduled_at"`
	StartedAt   *time.Time `json:"started_at,omitempty" db:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty" db:"completed_at"`

	// Retry handling
	Attempts   int       `json:"attempts" db:"attempts"`
	MaxRetries int       `json:"max_retries" db:"max_retries"`
	RetryAfter time.Time `json:"retry_after,omitempty" db:"retry_after"`

	// Error tracking
	LastError string `json:"last_error,omitempty" db:"last_error"`

	// Metadata
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
	Region    string    `json:"region,omitempty" db:"region"` // Source region
	WorkerID  string    `json:"worker_id,omitempty" db:"worker_id"`
}

// TaskFilter for querying tasks.
type TaskFilter struct {
	Type   TaskType   `json:"type,omitempty"`
	Status TaskStatus `json:"status,omitempty"`
	Region string     `json:"region,omitempty"`
	Limit  int        `json:"limit,omitempty"`
	Offset int        `json:"offset,omitempty"`
}

// QueueStats provides queue metrics.
type QueueStats struct {
	Pending    int64 `json:"pending"`
	Running    int64 `json:"running"`
	Completed  int64 `json:"completed"`
	Failed     int64 `json:"failed"`
	DeadLetter int64 `json:"dead_letter"`

	// By type
	ByType map[TaskType]int64 `json:"by_type"`

	// Performance
	AvgProcessingTime time.Duration `json:"avg_processing_time_ms"`
	OldestPending     *time.Time    `json:"oldest_pending,omitempty"`
}

// MarshalPayload is a helper to marshal a payload struct to JSON.
func MarshalPayload(v any) (json.RawMessage, error) {
	return json.Marshal(v)
}

// UnmarshalPayload is a helper to unmarshal a JSON payload.
func UnmarshalPayload[T any](payload json.RawMessage) (T, error) {
	var v T
	err := json.Unmarshal(payload, &v)
	return v, err
}
