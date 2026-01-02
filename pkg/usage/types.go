// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package usage provides usage tracking and reporting for billing.
// This is the core package containing types and interfaces used by both
// community and enterprise editions.
package usage

import (
	"time"
)

// EventType represents the type of usage event.
type EventType string

const (
	// EventTypeStorageDelta represents a change in storage (+/- bytes).
	EventTypeStorageDelta EventType = "storage_delta"

	// EventTypeObjectDelta represents a change in object count (+/- count).
	EventTypeObjectDelta EventType = "object_delta"

	// EventTypeRequest represents an API request.
	EventTypeRequest EventType = "request"

	// EventTypeBandwidth represents data transfer.
	EventTypeBandwidth EventType = "bandwidth"
)

// Direction represents the direction of data transfer.
type Direction string

const (
	// DirectionIngress represents incoming data (uploads).
	DirectionIngress Direction = "ingress"

	// DirectionEgress represents outgoing data (downloads).
	DirectionEgress Direction = "egress"
)

// JobStatus represents the status of a report generation job.
type JobStatus string

const (
	JobStatusPending    JobStatus = "pending"
	JobStatusProcessing JobStatus = "processing"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
)

// UsageEvent represents a single usage event to be recorded.
type UsageEvent struct {
	ID           int64
	EventTime    time.Time
	OwnerID      string
	Bucket       string
	EventType    EventType
	BytesDelta   int64
	CountDelta   int
	Operation    string
	Direction    Direction
	StorageClass string
}

// DailyUsage represents aggregated usage for a single day.
type DailyUsage struct {
	ID                    int64
	UsageDate             time.Time
	OwnerID               string
	Bucket                string
	StorageBytes          int64
	StorageBytesStandard  int64
	StorageBytesIA        int64
	StorageBytesGlacier   int64
	ObjectCount           int64
	RequestsGet           int
	RequestsPut           int
	RequestsDelete        int
	RequestsList          int
	RequestsHead          int
	RequestsCopy          int
	RequestsOther         int
	BandwidthIngressBytes int64
	BandwidthEgressBytes  int64
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// ReportJob represents an async report generation job.
type ReportJob struct {
	ID           string
	OwnerID      string
	PeriodStart  time.Time
	PeriodEnd    time.Time
	IncludeDaily bool
	Status       JobStatus
	ProgressPct  int
	ErrorMessage string
	ResultJSON   string
	CreatedAt    time.Time
	StartedAt    *time.Time
	CompletedAt  *time.Time
	ExpiresAt    time.Time
}

// UsageReport is the complete usage report for a billing period.
type UsageReport struct {
	ReportID    string
	OwnerID     string
	PeriodStart time.Time
	PeriodEnd   time.Time
	GeneratedAt time.Time
	Summary     UsageSummary
	Buckets     []BucketUsage
}

// UsageSummary contains aggregated totals across all buckets.
type UsageSummary struct {
	StorageBytesAvg  int64
	StorageBytesMax  int64
	ObjectCountAvg   int64
	StorageByClass   map[string]int64
	Requests         RequestCounts
	BandwidthIngress int64
	BandwidthEgress  int64
}

// BucketUsage contains usage for a single bucket.
type BucketUsage struct {
	Bucket           string
	StorageBytesAvg  int64
	StorageBytesMax  int64
	ObjectCountAvg   int64
	StorageByClass   map[string]int64
	Requests         RequestCounts
	BandwidthIngress int64
	BandwidthEgress  int64
	Daily            []DailySnapshot
}

// RequestCounts breaks down requests by operation type.
type RequestCounts struct {
	Get    int64
	Put    int64
	Delete int64
	List   int64
	Head   int64
	Copy   int64
	Other  int64
	Total  int64
}

// DailySnapshot contains usage for a single day (used in reports).
type DailySnapshot struct {
	Date             string // Format: "2025-01-15"
	StorageBytes     int64
	ObjectCount      int64
	Requests         RequestCounts
	BandwidthIngress int64
	BandwidthEgress  int64
}

// CurrentUsage contains real-time usage estimates.
type CurrentUsage struct {
	OwnerID            string
	AsOf               time.Time
	StorageBytes       int64
	ObjectCount        int64
	Buckets            []BucketSnapshot
	MTDRequests        int64
	MTDBandwidthEgress int64
}

// BucketSnapshot contains current usage for a single bucket.
type BucketSnapshot struct {
	Bucket       string
	StorageBytes int64
	ObjectCount  int64
}

// OwnerBucketPair identifies an owner/bucket combination for aggregation.
type OwnerBucketPair struct {
	OwnerID string
	Bucket  string
}

// AggregatedSummary contains aggregated metrics from events.
type AggregatedSummary struct {
	StorageBytes     int64
	ObjectCount      int64
	StorageByClass   map[string]int64
	Requests         map[string]int // operation -> count
	RequestsOther    int
	BandwidthIngress int64
	BandwidthEgress  int64
}
