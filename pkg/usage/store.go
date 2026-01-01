package usage

import (
	"context"
	"time"
)

// Store defines the interface for usage data persistence.
// This interface is implemented by the database layer and used by
// the collector, aggregator, and reporter.
type Store interface {
	// Event operations

	// InsertEvents batch inserts usage events.
	InsertEvents(ctx context.Context, events []UsageEvent) error

	// GetDistinctOwnerBuckets returns all owner/bucket pairs with events in the time range.
	GetDistinctOwnerBuckets(ctx context.Context, start, end time.Time) ([]OwnerBucketPair, error)

	// AggregateEvents calculates aggregated metrics for an owner/bucket in a time range.
	AggregateEvents(ctx context.Context, ownerID, bucket string, start, end time.Time) (*AggregatedSummary, error)

	// DeleteEventsOlderThan removes events older than the cutoff time.
	DeleteEventsOlderThan(ctx context.Context, cutoff time.Time) (int64, error)

	// Daily usage operations

	// GetDailyUsage retrieves daily usage records for an owner in a date range.
	GetDailyUsage(ctx context.Context, ownerID string, start, end time.Time) ([]DailyUsage, error)

	// GetDailyUsageByBucket retrieves daily usage for a specific bucket.
	GetDailyUsageByBucket(ctx context.Context, ownerID, bucket string, start, end time.Time) ([]DailyUsage, error)

	// UpsertDailyUsage inserts or updates a daily usage record.
	UpsertDailyUsage(ctx context.Context, usage *DailyUsage) error

	// GetLatestDailyUsage returns the most recent daily record for each bucket.
	// Used for calculating current storage from last known state.
	GetLatestDailyUsage(ctx context.Context, ownerID string) ([]DailyUsage, error)

	// Report job operations

	// CreateReportJob creates a new report generation job.
	CreateReportJob(ctx context.Context, job *ReportJob) error

	// GetReportJob retrieves a report job by ID.
	GetReportJob(ctx context.Context, jobID string) (*ReportJob, error)

	// UpdateReportJob updates a report job's status and result.
	UpdateReportJob(ctx context.Context, job *ReportJob) error

	// ClaimPendingJob atomically claims and returns a pending job for processing.
	// Returns nil if no pending jobs are available.
	ClaimPendingJob(ctx context.Context) (*ReportJob, error)

	// ListReportJobs lists recent report jobs for an owner.
	ListReportJobs(ctx context.Context, ownerID string, limit int) ([]ReportJob, error)

	// DeleteExpiredReportJobs removes report jobs that have expired.
	DeleteExpiredReportJobs(ctx context.Context) (int64, error)

	// Current usage operations

	// GetCurrentStorageByOwner calculates current storage for all buckets of an owner.
	// This uses the latest daily usage plus any pending events.
	GetCurrentStorageByOwner(ctx context.Context, ownerID string) ([]BucketSnapshot, error)

	// GetMTDRequestCount returns month-to-date request count for an owner.
	GetMTDRequestCount(ctx context.Context, ownerID string) (int64, error)

	// GetMTDBandwidthEgress returns month-to-date egress bandwidth for an owner.
	GetMTDBandwidthEgress(ctx context.Context, ownerID string) (int64, error)
}

// NopStore is a no-op implementation of Store for community edition.
type NopStore struct{}

// Verify NopStore implements Store.
var _ Store = (*NopStore)(nil)

func (s *NopStore) InsertEvents(ctx context.Context, events []UsageEvent) error {
	return nil
}

func (s *NopStore) GetDistinctOwnerBuckets(ctx context.Context, start, end time.Time) ([]OwnerBucketPair, error) {
	return nil, nil
}

func (s *NopStore) AggregateEvents(ctx context.Context, ownerID, bucket string, start, end time.Time) (*AggregatedSummary, error) {
	return nil, nil
}

func (s *NopStore) DeleteEventsOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	return 0, nil
}

func (s *NopStore) GetDailyUsage(ctx context.Context, ownerID string, start, end time.Time) ([]DailyUsage, error) {
	return nil, nil
}

func (s *NopStore) GetDailyUsageByBucket(ctx context.Context, ownerID, bucket string, start, end time.Time) ([]DailyUsage, error) {
	return nil, nil
}

func (s *NopStore) UpsertDailyUsage(ctx context.Context, usage *DailyUsage) error {
	return nil
}

func (s *NopStore) GetLatestDailyUsage(ctx context.Context, ownerID string) ([]DailyUsage, error) {
	return nil, nil
}

func (s *NopStore) CreateReportJob(ctx context.Context, job *ReportJob) error {
	return nil
}

func (s *NopStore) GetReportJob(ctx context.Context, jobID string) (*ReportJob, error) {
	return nil, nil
}

func (s *NopStore) UpdateReportJob(ctx context.Context, job *ReportJob) error {
	return nil
}

func (s *NopStore) ClaimPendingJob(ctx context.Context) (*ReportJob, error) {
	return nil, nil
}

func (s *NopStore) ListReportJobs(ctx context.Context, ownerID string, limit int) ([]ReportJob, error) {
	return nil, nil
}

func (s *NopStore) DeleteExpiredReportJobs(ctx context.Context) (int64, error) {
	return 0, nil
}

func (s *NopStore) GetCurrentStorageByOwner(ctx context.Context, ownerID string) ([]BucketSnapshot, error) {
	return nil, nil
}

func (s *NopStore) GetMTDRequestCount(ctx context.Context, ownerID string) (int64, error) {
	return 0, nil
}

func (s *NopStore) GetMTDBandwidthEgress(ctx context.Context, ownerID string) (int64, error) {
	return 0, nil
}
