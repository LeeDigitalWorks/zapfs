// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package taskqueue provides a durable task queue for background processing.
//
// Supported backends:
// - Database (Vitess/MySQL) - default, uses existing infrastructure
// - In-memory - for testing only
//
// Community use cases:
// - Object/version cleanup
// - Lifecycle transitions
//
// Enterprise use cases (requires license):
// - Cross-region replication (CRR)
// - Audit log shipping
// - Webhook delivery
//
// Note: GC RefCount decrements are now handled by the centralized chunk_registry
// in the metadata database, eliminating the need for distributed task queues.
package taskqueue

import (
	"encoding/json"
	"time"
)

// Default configuration values
const (
	DefaultPollInterval      = time.Second
	DefaultConcurrency       = 5
	DefaultVisibilityTimeout = 5 * time.Minute
	DefaultMaxRetries        = 3
)

// TaskType identifies the type of task for routing to handlers.
type TaskType string

// Community task types
const (
	TaskTypeCleanup   TaskType = "cleanup"   // Object/version cleanup
	TaskTypeLifecycle TaskType = "lifecycle" // Lifecycle transitions
	TaskTypeEvent     TaskType = "event"     // S3 event notification
	TaskTypeRestore   TaskType = "restore"   // Object restore from archive

	// Federation task types (S3 passthrough/migration)
	TaskTypeFederationIngest    TaskType = "federation_ingest"    // Ingest object from external S3 to local
	TaskTypeFederationDiscovery TaskType = "federation_discovery" // Discover objects in external S3 bucket
)

// LifecyclePayload is the payload for TaskTypeLifecycle tasks.
// Contains object metadata and action to perform.
type LifecyclePayload struct {
	// Object identification
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id,omitempty"`

	// Action to take: "delete", "delete_version", "transition", "abort_mpu"
	Action       string `json:"action"`
	StorageClass string `json:"storage_class,omitempty"` // For transitions

	// Tracking
	RuleID      string `json:"rule_id"`
	EvaluatedAt int64  `json:"evaluated_at"` // Unix timestamp (nanos)

	// Verification (to ensure object hasn't changed since evaluation)
	ExpectedETag    string `json:"expected_etag,omitempty"`
	ExpectedModTime int64  `json:"expected_mod_time,omitempty"` // Unix timestamp (nanos)

	// For multipart uploads
	UploadID string `json:"upload_id,omitempty"`
}

// Lifecycle action constants
const (
	LifecycleActionDelete        = "delete"
	LifecycleActionDeleteVersion = "delete_version"
	LifecycleActionTransition    = "transition"
	LifecycleActionAbortMPU      = "abort_mpu"
	LifecycleActionPromote       = "promote" // Intelligent tiering auto-promotion on access
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

// RestorePayload is the payload for TaskTypeRestore tasks.
// Contains metadata for restoring an archived object.
type RestorePayload struct {
	// Object identification
	ObjectID  string `json:"object_id"`            // UUID of the object
	Bucket    string `json:"bucket"`               // Bucket name
	Key       string `json:"key"`                  // Object key
	VersionID string `json:"version_id,omitempty"` // Version ID if versioned

	// Restore configuration
	Days int    `json:"days"` // Number of days to keep restored copy
	Tier string `json:"tier"` // Retrieval tier: "Expedited", "Standard", "Bulk"

	// Source location
	StorageClass    string `json:"storage_class"`              // Current storage class (GLACIER, DEEP_ARCHIVE)
	TransitionedRef string `json:"transitioned_ref,omitempty"` // Reference to archived data

	// Tracking
	RequestedAt int64  `json:"requested_at"` // Unix nano when restore was initiated
	RequestID   string `json:"request_id"`   // Original S3 request ID
}

// Restore tier constants (match AWS retrieval tiers)
const (
	RestoreTierExpedited = "Expedited" // 1-5 minutes (only Glacier)
	RestoreTierStandard  = "Standard"  // 3-5 hours (Glacier), 12 hours (Deep Archive)
	RestoreTierBulk      = "Bulk"      // 5-12 hours (Glacier), 48 hours (Deep Archive)
)

// FederationIngestPayload is the payload for TaskTypeFederationIngest tasks.
// Contains metadata for ingesting an object from external S3 to local storage.
type FederationIngestPayload struct {
	// Local bucket name (where the object will be stored)
	Bucket string `json:"bucket"`

	// Object key
	Key string `json:"key"`

	// Version ID from external S3 (empty for non-versioned)
	VersionID string `json:"version_id,omitempty"`

	// Object metadata from external S3
	Size         int64  `json:"size"`
	ETag         string `json:"etag"`
	ContentType  string `json:"content_type,omitempty"`
	StorageClass string `json:"storage_class,omitempty"`
	LastModified int64  `json:"last_modified"` // Unix timestamp (nanos)

	// Tracking
	DiscoveredAt int64 `json:"discovered_at"` // Unix timestamp (nanos) when object was discovered
}

// FederationDiscoveryPayload is the payload for TaskTypeFederationDiscovery tasks.
// Contains metadata for discovering objects in an external S3 bucket.
type FederationDiscoveryPayload struct {
	// Local bucket name (federated bucket)
	Bucket string `json:"bucket"`

	// Pagination state for resuming discovery
	StartAfter string `json:"start_after,omitempty"` // Continue from this key
	Prefix     string `json:"prefix,omitempty"`      // Optional prefix filter

	// Batch configuration
	BatchSize int `json:"batch_size"` // Max objects per batch (default: 1000)

	// Versioning
	IncludeVersions bool `json:"include_versions"` // Discover all versions (for versioned buckets)

	// Tracking
	StartedAt int64 `json:"started_at"` // Unix timestamp (nanos) when discovery started
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
