package usage

import (
	"context"
	"testing"
	"time"
)

func TestNopStore(t *testing.T) {
	store := &NopStore{}
	ctx := context.Background()

	// All methods should return nil/empty without error
	t.Run("InsertEvents", func(t *testing.T) {
		err := store.InsertEvents(ctx, []UsageEvent{{OwnerID: "test"}})
		if err != nil {
			t.Errorf("InsertEvents() error = %v, want nil", err)
		}
	})

	t.Run("GetDistinctOwnerBuckets", func(t *testing.T) {
		pairs, err := store.GetDistinctOwnerBuckets(ctx, time.Now(), time.Now())
		if err != nil {
			t.Errorf("GetDistinctOwnerBuckets() error = %v, want nil", err)
		}
		if pairs != nil {
			t.Errorf("GetDistinctOwnerBuckets() = %v, want nil", pairs)
		}
	})

	t.Run("AggregateEvents", func(t *testing.T) {
		summary, err := store.AggregateEvents(ctx, "owner", "bucket", time.Now(), time.Now())
		if err != nil {
			t.Errorf("AggregateEvents() error = %v, want nil", err)
		}
		if summary != nil {
			t.Errorf("AggregateEvents() = %v, want nil", summary)
		}
	})

	t.Run("DeleteEventsOlderThan", func(t *testing.T) {
		count, err := store.DeleteEventsOlderThan(ctx, time.Now())
		if err != nil {
			t.Errorf("DeleteEventsOlderThan() error = %v, want nil", err)
		}
		if count != 0 {
			t.Errorf("DeleteEventsOlderThan() = %d, want 0", count)
		}
	})

	t.Run("GetDailyUsage", func(t *testing.T) {
		usage, err := store.GetDailyUsage(ctx, "owner", time.Now(), time.Now())
		if err != nil {
			t.Errorf("GetDailyUsage() error = %v, want nil", err)
		}
		if usage != nil {
			t.Errorf("GetDailyUsage() = %v, want nil", usage)
		}
	})

	t.Run("GetDailyUsageByBucket", func(t *testing.T) {
		usage, err := store.GetDailyUsageByBucket(ctx, "owner", "bucket", time.Now(), time.Now())
		if err != nil {
			t.Errorf("GetDailyUsageByBucket() error = %v, want nil", err)
		}
		if usage != nil {
			t.Errorf("GetDailyUsageByBucket() = %v, want nil", usage)
		}
	})

	t.Run("UpsertDailyUsage", func(t *testing.T) {
		err := store.UpsertDailyUsage(ctx, &DailyUsage{})
		if err != nil {
			t.Errorf("UpsertDailyUsage() error = %v, want nil", err)
		}
	})

	t.Run("GetLatestDailyUsage", func(t *testing.T) {
		usage, err := store.GetLatestDailyUsage(ctx, "owner")
		if err != nil {
			t.Errorf("GetLatestDailyUsage() error = %v, want nil", err)
		}
		if usage != nil {
			t.Errorf("GetLatestDailyUsage() = %v, want nil", usage)
		}
	})

	t.Run("CreateReportJob", func(t *testing.T) {
		err := store.CreateReportJob(ctx, &ReportJob{})
		if err != nil {
			t.Errorf("CreateReportJob() error = %v, want nil", err)
		}
	})

	t.Run("GetReportJob", func(t *testing.T) {
		job, err := store.GetReportJob(ctx, "job-id")
		if err != nil {
			t.Errorf("GetReportJob() error = %v, want nil", err)
		}
		if job != nil {
			t.Errorf("GetReportJob() = %v, want nil", job)
		}
	})

	t.Run("UpdateReportJob", func(t *testing.T) {
		err := store.UpdateReportJob(ctx, &ReportJob{})
		if err != nil {
			t.Errorf("UpdateReportJob() error = %v, want nil", err)
		}
	})

	t.Run("ClaimPendingJob", func(t *testing.T) {
		job, err := store.ClaimPendingJob(ctx)
		if err != nil {
			t.Errorf("ClaimPendingJob() error = %v, want nil", err)
		}
		if job != nil {
			t.Errorf("ClaimPendingJob() = %v, want nil", job)
		}
	})

	t.Run("ListReportJobs", func(t *testing.T) {
		jobs, err := store.ListReportJobs(ctx, "owner", 10)
		if err != nil {
			t.Errorf("ListReportJobs() error = %v, want nil", err)
		}
		if jobs != nil {
			t.Errorf("ListReportJobs() = %v, want nil", jobs)
		}
	})

	t.Run("DeleteExpiredReportJobs", func(t *testing.T) {
		count, err := store.DeleteExpiredReportJobs(ctx)
		if err != nil {
			t.Errorf("DeleteExpiredReportJobs() error = %v, want nil", err)
		}
		if count != 0 {
			t.Errorf("DeleteExpiredReportJobs() = %d, want 0", count)
		}
	})

	t.Run("GetCurrentStorageByOwner", func(t *testing.T) {
		buckets, err := store.GetCurrentStorageByOwner(ctx, "owner")
		if err != nil {
			t.Errorf("GetCurrentStorageByOwner() error = %v, want nil", err)
		}
		if buckets != nil {
			t.Errorf("GetCurrentStorageByOwner() = %v, want nil", buckets)
		}
	})

	t.Run("GetMTDRequestCount", func(t *testing.T) {
		count, err := store.GetMTDRequestCount(ctx, "owner")
		if err != nil {
			t.Errorf("GetMTDRequestCount() error = %v, want nil", err)
		}
		if count != 0 {
			t.Errorf("GetMTDRequestCount() = %d, want 0", count)
		}
	})

	t.Run("GetMTDBandwidthEgress", func(t *testing.T) {
		bytes, err := store.GetMTDBandwidthEgress(ctx, "owner")
		if err != nil {
			t.Errorf("GetMTDBandwidthEgress() error = %v, want nil", err)
		}
		if bytes != 0 {
			t.Errorf("GetMTDBandwidthEgress() = %d, want 0", bytes)
		}
	})
}

// MockStore is a mock implementation of Store for testing.
type MockStore struct {
	events          []UsageEvent
	dailyUsage      []DailyUsage
	reportJobs      map[string]*ReportJob
	insertErr       error
	aggregateResult *AggregatedSummary
}

func NewMockStore() *MockStore {
	return &MockStore{
		reportJobs: make(map[string]*ReportJob),
	}
}

func (m *MockStore) InsertEvents(ctx context.Context, events []UsageEvent) error {
	if m.insertErr != nil {
		return m.insertErr
	}
	m.events = append(m.events, events...)
	return nil
}

func (m *MockStore) GetDistinctOwnerBuckets(ctx context.Context, start, end time.Time) ([]OwnerBucketPair, error) {
	pairs := make(map[string]bool)
	var result []OwnerBucketPair
	for _, e := range m.events {
		if e.EventTime.After(start) && e.EventTime.Before(end) {
			key := e.OwnerID + ":" + e.Bucket
			if !pairs[key] {
				pairs[key] = true
				result = append(result, OwnerBucketPair{OwnerID: e.OwnerID, Bucket: e.Bucket})
			}
		}
	}
	return result, nil
}

func (m *MockStore) AggregateEvents(ctx context.Context, ownerID, bucket string, start, end time.Time) (*AggregatedSummary, error) {
	if m.aggregateResult != nil {
		return m.aggregateResult, nil
	}

	summary := &AggregatedSummary{
		StorageByClass: make(map[string]int64),
		Requests:       make(map[string]int),
	}

	for _, e := range m.events {
		if e.OwnerID == ownerID && e.Bucket == bucket &&
			e.EventTime.After(start) && e.EventTime.Before(end) {
			switch e.EventType {
			case EventTypeStorageDelta:
				summary.StorageBytes += e.BytesDelta
			case EventTypeObjectDelta:
				summary.ObjectCount += int64(e.CountDelta)
			case EventTypeRequest:
				summary.Requests[e.Operation]++
			case EventTypeBandwidth:
				if e.Direction == DirectionIngress {
					summary.BandwidthIngress += e.BytesDelta
				} else {
					summary.BandwidthEgress += e.BytesDelta
				}
			}
		}
	}

	return summary, nil
}

func (m *MockStore) DeleteEventsOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	var kept []UsageEvent
	var deleted int64
	for _, e := range m.events {
		if e.EventTime.Before(cutoff) {
			deleted++
		} else {
			kept = append(kept, e)
		}
	}
	m.events = kept
	return deleted, nil
}

func (m *MockStore) RunPartitionMaintenance(ctx context.Context, monthsAhead int) error {
	// No-op for mock - partitioning is a database-level concern
	return nil
}

func (m *MockStore) GetDailyUsage(ctx context.Context, ownerID string, start, end time.Time) ([]DailyUsage, error) {
	var result []DailyUsage
	for _, d := range m.dailyUsage {
		if d.OwnerID == ownerID && !d.UsageDate.Before(start) && !d.UsageDate.After(end) {
			result = append(result, d)
		}
	}
	return result, nil
}

func (m *MockStore) GetDailyUsageByBucket(ctx context.Context, ownerID, bucket string, start, end time.Time) ([]DailyUsage, error) {
	var result []DailyUsage
	for _, d := range m.dailyUsage {
		if d.OwnerID == ownerID && d.Bucket == bucket && !d.UsageDate.Before(start) && !d.UsageDate.After(end) {
			result = append(result, d)
		}
	}
	return result, nil
}

func (m *MockStore) UpsertDailyUsage(ctx context.Context, usage *DailyUsage) error {
	// Find and update, or append
	for i, d := range m.dailyUsage {
		if d.OwnerID == usage.OwnerID && d.Bucket == usage.Bucket &&
			d.UsageDate.Year() == usage.UsageDate.Year() &&
			d.UsageDate.YearDay() == usage.UsageDate.YearDay() {
			m.dailyUsage[i] = *usage
			return nil
		}
	}
	m.dailyUsage = append(m.dailyUsage, *usage)
	return nil
}

func (m *MockStore) GetLatestDailyUsage(ctx context.Context, ownerID string) ([]DailyUsage, error) {
	bucketLatest := make(map[string]*DailyUsage)
	for i := range m.dailyUsage {
		d := &m.dailyUsage[i]
		if d.OwnerID == ownerID {
			if existing, ok := bucketLatest[d.Bucket]; !ok || d.UsageDate.After(existing.UsageDate) {
				bucketLatest[d.Bucket] = d
			}
		}
	}
	var result []DailyUsage
	for _, d := range bucketLatest {
		result = append(result, *d)
	}
	return result, nil
}

func (m *MockStore) CreateReportJob(ctx context.Context, job *ReportJob) error {
	m.reportJobs[job.ID] = job
	return nil
}

func (m *MockStore) GetReportJob(ctx context.Context, jobID string) (*ReportJob, error) {
	return m.reportJobs[jobID], nil
}

func (m *MockStore) UpdateReportJob(ctx context.Context, job *ReportJob) error {
	m.reportJobs[job.ID] = job
	return nil
}

func (m *MockStore) ClaimPendingJob(ctx context.Context) (*ReportJob, error) {
	for _, job := range m.reportJobs {
		if job.Status == JobStatusPending {
			now := time.Now()
			job.Status = JobStatusProcessing
			job.StartedAt = &now
			return job, nil
		}
	}
	return nil, nil
}

func (m *MockStore) ListReportJobs(ctx context.Context, ownerID string, limit int) ([]ReportJob, error) {
	var result []ReportJob
	for _, job := range m.reportJobs {
		if job.OwnerID == ownerID {
			result = append(result, *job)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *MockStore) DeleteExpiredReportJobs(ctx context.Context) (int64, error) {
	now := time.Now()
	var deleted int64
	for id, job := range m.reportJobs {
		if job.ExpiresAt.Before(now) {
			delete(m.reportJobs, id)
			deleted++
		}
	}
	return deleted, nil
}

func (m *MockStore) GetCurrentStorageByOwner(ctx context.Context, ownerID string) ([]BucketSnapshot, error) {
	latest, err := m.GetLatestDailyUsage(ctx, ownerID)
	if err != nil {
		return nil, err
	}
	var result []BucketSnapshot
	for _, d := range latest {
		result = append(result, BucketSnapshot{
			Bucket:       d.Bucket,
			StorageBytes: d.StorageBytes,
			ObjectCount:  d.ObjectCount,
		})
	}
	return result, nil
}

func (m *MockStore) GetMTDRequestCount(ctx context.Context, ownerID string) (int64, error) {
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	var count int64
	for _, d := range m.dailyUsage {
		if d.OwnerID == ownerID && !d.UsageDate.Before(monthStart) {
			count += int64(d.RequestsGet + d.RequestsPut + d.RequestsDelete +
				d.RequestsList + d.RequestsHead + d.RequestsCopy + d.RequestsOther)
		}
	}
	return count, nil
}

func (m *MockStore) GetMTDBandwidthEgress(ctx context.Context, ownerID string) (int64, error) {
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	var bytes int64
	for _, d := range m.dailyUsage {
		if d.OwnerID == ownerID && !d.UsageDate.Before(monthStart) {
			bytes += d.BandwidthEgressBytes
		}
	}
	return bytes, nil
}

// Verify MockStore implements Store interface
var _ Store = (*MockStore)(nil)

func TestMockStore(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	t.Run("InsertAndAggregateEvents", func(t *testing.T) {
		now := time.Now()
		events := []UsageEvent{
			{
				EventTime:  now,
				OwnerID:    "owner1",
				Bucket:     "bucket1",
				EventType:  EventTypeStorageDelta,
				BytesDelta: 1024,
			},
			{
				EventTime: now,
				OwnerID:   "owner1",
				Bucket:    "bucket1",
				EventType: EventTypeRequest,
				Operation: "GetObject",
			},
		}

		err := store.InsertEvents(ctx, events)
		if err != nil {
			t.Fatalf("InsertEvents() error = %v", err)
		}

		summary, err := store.AggregateEvents(ctx, "owner1", "bucket1", now.Add(-time.Hour), now.Add(time.Hour))
		if err != nil {
			t.Fatalf("AggregateEvents() error = %v", err)
		}

		if summary.StorageBytes != 1024 {
			t.Errorf("StorageBytes = %d, want 1024", summary.StorageBytes)
		}
		if summary.Requests["GetObject"] != 1 {
			t.Errorf("Requests[GetObject] = %d, want 1", summary.Requests["GetObject"])
		}
	})

	t.Run("ReportJobLifecycle", func(t *testing.T) {
		job := &ReportJob{
			ID:          "job-1",
			OwnerID:     "owner1",
			PeriodStart: time.Now().AddDate(0, -1, 0),
			PeriodEnd:   time.Now(),
			Status:      JobStatusPending,
			CreatedAt:   time.Now(),
			ExpiresAt:   time.Now().Add(24 * time.Hour),
		}

		err := store.CreateReportJob(ctx, job)
		if err != nil {
			t.Fatalf("CreateReportJob() error = %v", err)
		}

		claimed, err := store.ClaimPendingJob(ctx)
		if err != nil {
			t.Fatalf("ClaimPendingJob() error = %v", err)
		}
		if claimed == nil {
			t.Fatal("ClaimPendingJob() returned nil, expected job")
		}
		if claimed.Status != JobStatusProcessing {
			t.Errorf("claimed.Status = %s, want processing", claimed.Status)
		}

		// Second claim should return nil
		claimed2, err := store.ClaimPendingJob(ctx)
		if err != nil {
			t.Fatalf("ClaimPendingJob() error = %v", err)
		}
		if claimed2 != nil {
			t.Error("ClaimPendingJob() should return nil when no pending jobs")
		}
	})
}
