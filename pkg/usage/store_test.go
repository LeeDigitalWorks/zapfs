// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

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
