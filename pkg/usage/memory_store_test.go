// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package usage

import (
	"context"
	"time"
)

// MemoryStore is an in-memory implementation of Store for testing.
// Unlike expectation-based mocks, this is a functional fake that
// stores real data in memory, enabling integration-style tests.
type MemoryStore struct {
	events     []UsageEvent
	dailyUsage []DailyUsage
	reportJobs map[string]*ReportJob
}

// Verify MemoryStore implements Store interface
var _ Store = (*MemoryStore)(nil)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		reportJobs: make(map[string]*ReportJob),
	}
}

func (m *MemoryStore) InsertEvents(ctx context.Context, events []UsageEvent) error {
	m.events = append(m.events, events...)
	return nil
}

func (m *MemoryStore) GetDistinctOwnerBuckets(ctx context.Context, start, end time.Time) ([]OwnerBucketPair, error) {
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

func (m *MemoryStore) AggregateEvents(ctx context.Context, ownerID, bucket string, start, end time.Time) (*AggregatedSummary, error) {
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

func (m *MemoryStore) DeleteEventsOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
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

func (m *MemoryStore) RunPartitionMaintenance(ctx context.Context, monthsAhead int) error {
	return nil
}

func (m *MemoryStore) GetDailyUsage(ctx context.Context, ownerID string, start, end time.Time) ([]DailyUsage, error) {
	var result []DailyUsage
	for _, d := range m.dailyUsage {
		if d.OwnerID == ownerID && !d.UsageDate.Before(start) && !d.UsageDate.After(end) {
			result = append(result, d)
		}
	}
	return result, nil
}

func (m *MemoryStore) GetDailyUsageByBucket(ctx context.Context, ownerID, bucket string, start, end time.Time) ([]DailyUsage, error) {
	var result []DailyUsage
	for _, d := range m.dailyUsage {
		if d.OwnerID == ownerID && d.Bucket == bucket && !d.UsageDate.Before(start) && !d.UsageDate.After(end) {
			result = append(result, d)
		}
	}
	return result, nil
}

func (m *MemoryStore) UpsertDailyUsage(ctx context.Context, usage *DailyUsage) error {
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

func (m *MemoryStore) GetLatestDailyUsage(ctx context.Context, ownerID string) ([]DailyUsage, error) {
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

func (m *MemoryStore) CreateReportJob(ctx context.Context, job *ReportJob) error {
	m.reportJobs[job.ID] = job
	return nil
}

func (m *MemoryStore) GetReportJob(ctx context.Context, jobID string) (*ReportJob, error) {
	return m.reportJobs[jobID], nil
}

func (m *MemoryStore) UpdateReportJob(ctx context.Context, job *ReportJob) error {
	m.reportJobs[job.ID] = job
	return nil
}

func (m *MemoryStore) ClaimPendingJob(ctx context.Context) (*ReportJob, error) {
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

func (m *MemoryStore) ListReportJobs(ctx context.Context, ownerID string, limit int) ([]ReportJob, error) {
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

func (m *MemoryStore) DeleteExpiredReportJobs(ctx context.Context) (int64, error) {
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

func (m *MemoryStore) GetCurrentStorageByOwner(ctx context.Context, ownerID string) ([]BucketSnapshot, error) {
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

func (m *MemoryStore) GetMTDRequestCount(ctx context.Context, ownerID string) (int64, error) {
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

func (m *MemoryStore) GetMTDBandwidthEgress(ctx context.Context, ownerID string) (int64, error) {
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

// Events returns the stored events for test assertions.
func (m *MemoryStore) Events() []UsageEvent {
	return m.events
}
