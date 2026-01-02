//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db/vitess"
	"github.com/LeeDigitalWorks/zapfs/pkg/usage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Usage Store Integration Tests
// =============================================================================

// getUsageStore returns a usage store for testing.
func getUsageStore(t *testing.T) (usage.Store, func()) {
	t.Helper()

	db, cleanup := getTestDB(t)
	v, ok := db.(*vitess.Vitess)
	require.True(t, ok, "expected Vitess database")

	return v.UsageStore(), cleanup
}

// =============================================================================
// Event Operations Tests
// =============================================================================

func TestUsageStore_InsertAndAggregateEvents(t *testing.T) {
	testutil.SkipIfShort(t)

	store, cleanup := getUsageStore(t)
	defer cleanup()

	ctx := context.Background()
	ownerID := testutil.UniqueID("owner")
	bucket := testutil.UniqueID("bucket")
	now := time.Now().UTC()

	// Insert various usage events
	events := []usage.UsageEvent{
		// Storage events
		{
			EventTime:    now,
			OwnerID:      ownerID,
			Bucket:       bucket,
			EventType:    usage.EventTypeStorageDelta,
			BytesDelta:   1024 * 1024, // 1MB added
			StorageClass: "STANDARD",
		},
		{
			EventTime:    now.Add(time.Minute),
			OwnerID:      ownerID,
			Bucket:       bucket,
			EventType:    usage.EventTypeStorageDelta,
			BytesDelta:   2 * 1024 * 1024, // 2MB added
			StorageClass: "STANDARD",
		},
		// Object count events
		{
			EventTime:  now,
			OwnerID:    ownerID,
			Bucket:     bucket,
			EventType:  usage.EventTypeObjectDelta,
			CountDelta: 5, // 5 objects added
		},
		{
			EventTime:  now.Add(time.Minute),
			OwnerID:    ownerID,
			Bucket:     bucket,
			EventType:  usage.EventTypeObjectDelta,
			CountDelta: 3, // 3 more objects
		},
		// Request events
		{
			EventTime: now,
			OwnerID:   ownerID,
			Bucket:    bucket,
			EventType: usage.EventTypeRequest,
			Operation: "GetObject",
		},
		{
			EventTime: now,
			OwnerID:   ownerID,
			Bucket:    bucket,
			EventType: usage.EventTypeRequest,
			Operation: "GetObject",
		},
		{
			EventTime: now,
			OwnerID:   ownerID,
			Bucket:    bucket,
			EventType: usage.EventTypeRequest,
			Operation: "PutObject",
		},
		// Bandwidth events
		{
			EventTime:  now,
			OwnerID:    ownerID,
			Bucket:     bucket,
			EventType:  usage.EventTypeBandwidth,
			BytesDelta: 500 * 1024, // 500KB egress
			Direction:  usage.DirectionEgress,
		},
		{
			EventTime:  now,
			OwnerID:    ownerID,
			Bucket:     bucket,
			EventType:  usage.EventTypeBandwidth,
			BytesDelta: 100 * 1024, // 100KB ingress
			Direction:  usage.DirectionIngress,
		},
	}

	err := store.InsertEvents(ctx, events)
	require.NoError(t, err, "should insert events")

	// Aggregate events
	start := now.Add(-time.Hour)
	end := now.Add(time.Hour)

	summary, err := store.AggregateEvents(ctx, ownerID, bucket, start, end)
	require.NoError(t, err, "should aggregate events")
	require.NotNil(t, summary)

	// Verify aggregation results
	assert.Equal(t, int64(3*1024*1024), summary.StorageBytes, "storage bytes should be 3MB")
	assert.Equal(t, int64(8), summary.ObjectCount, "object count should be 8")
	assert.Equal(t, 2, summary.Requests["GetObject"], "should have 2 GetObject requests")
	assert.Equal(t, 1, summary.Requests["PutObject"], "should have 1 PutObject request")
	assert.Equal(t, int64(500*1024), summary.BandwidthEgress, "egress should be 500KB")
	assert.Equal(t, int64(100*1024), summary.BandwidthIngress, "ingress should be 100KB")

	t.Logf("Aggregation verified: storage=%d, objects=%d, requests=%v",
		summary.StorageBytes, summary.ObjectCount, summary.Requests)
}

func TestUsageStore_GetDistinctOwnerBuckets(t *testing.T) {
	testutil.SkipIfShort(t)

	store, cleanup := getUsageStore(t)
	defer cleanup()

	ctx := context.Background()
	owner1 := testutil.UniqueID("owner1")
	owner2 := testutil.UniqueID("owner2")
	now := time.Now().UTC()

	// Insert events for multiple owner/bucket combinations
	events := []usage.UsageEvent{
		{EventTime: now, OwnerID: owner1, Bucket: "bucket-a", EventType: usage.EventTypeRequest, Operation: "GetObject"},
		{EventTime: now, OwnerID: owner1, Bucket: "bucket-b", EventType: usage.EventTypeRequest, Operation: "GetObject"},
		{EventTime: now, OwnerID: owner2, Bucket: "bucket-c", EventType: usage.EventTypeRequest, Operation: "GetObject"},
		{EventTime: now, OwnerID: owner1, Bucket: "bucket-a", EventType: usage.EventTypeRequest, Operation: "PutObject"}, // Duplicate pair
	}

	err := store.InsertEvents(ctx, events)
	require.NoError(t, err)

	// Get distinct pairs
	pairs, err := store.GetDistinctOwnerBuckets(ctx, now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)

	// Should have 3 distinct pairs (owner1/bucket-a, owner1/bucket-b, owner2/bucket-c)
	// Filter to our test owners
	var testPairs []usage.OwnerBucketPair
	for _, p := range pairs {
		if p.OwnerID == owner1 || p.OwnerID == owner2 {
			testPairs = append(testPairs, p)
		}
	}

	assert.Len(t, testPairs, 3, "should have 3 distinct owner/bucket pairs")
	t.Logf("Found %d distinct owner/bucket pairs for test owners", len(testPairs))
}

// =============================================================================
// Daily Usage Tests
// =============================================================================

func TestUsageStore_DailyUsageUpsert(t *testing.T) {
	testutil.SkipIfShort(t)

	store, cleanup := getUsageStore(t)
	defer cleanup()

	ctx := context.Background()
	ownerID := testutil.UniqueID("owner")
	bucket := testutil.UniqueID("bucket")
	today := time.Now().UTC().Truncate(24 * time.Hour)

	// Create initial daily usage
	daily := &usage.DailyUsage{
		UsageDate:             today,
		OwnerID:               ownerID,
		Bucket:                bucket,
		StorageBytes:          1000,
		ObjectCount:           10,
		RequestsGet:           100,
		RequestsPut:           50,
		BandwidthEgressBytes:  5000,
		BandwidthIngressBytes: 1000,
	}

	err := store.UpsertDailyUsage(ctx, daily)
	require.NoError(t, err, "should upsert daily usage")

	// Verify by retrieving
	results, err := store.GetDailyUsageByBucket(ctx, ownerID, bucket, today, today)
	require.NoError(t, err)
	require.Len(t, results, 1, "should have 1 daily record")

	assert.Equal(t, int64(1000), results[0].StorageBytes)
	assert.Equal(t, int64(10), results[0].ObjectCount)
	assert.Equal(t, 100, results[0].RequestsGet)
	assert.Equal(t, 50, results[0].RequestsPut)

	// Update (upsert) with new values
	daily.StorageBytes = 2000
	daily.ObjectCount = 20
	daily.RequestsGet = 200

	err = store.UpsertDailyUsage(ctx, daily)
	require.NoError(t, err, "should update daily usage")

	// Verify update
	results, err = store.GetDailyUsageByBucket(ctx, ownerID, bucket, today, today)
	require.NoError(t, err)
	require.Len(t, results, 1, "should still have 1 daily record")

	assert.Equal(t, int64(2000), results[0].StorageBytes, "storage should be updated")
	assert.Equal(t, int64(20), results[0].ObjectCount, "object count should be updated")
	assert.Equal(t, 200, results[0].RequestsGet, "requests should be updated")

	t.Logf("Daily usage upsert verified: storage=%d, objects=%d", results[0].StorageBytes, results[0].ObjectCount)
}

func TestUsageStore_GetLatestDailyUsage(t *testing.T) {
	testutil.SkipIfShort(t)

	store, cleanup := getUsageStore(t)
	defer cleanup()

	ctx := context.Background()
	ownerID := testutil.UniqueID("owner")
	bucket1 := testutil.UniqueID("bucket1")
	bucket2 := testutil.UniqueID("bucket2")

	today := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := today.AddDate(0, 0, -1)
	twoDaysAgo := today.AddDate(0, 0, -2)

	// Insert multiple days for multiple buckets
	dailyRecords := []*usage.DailyUsage{
		{UsageDate: twoDaysAgo, OwnerID: ownerID, Bucket: bucket1, StorageBytes: 100},
		{UsageDate: yesterday, OwnerID: ownerID, Bucket: bucket1, StorageBytes: 200},
		{UsageDate: today, OwnerID: ownerID, Bucket: bucket1, StorageBytes: 300}, // Latest for bucket1
		{UsageDate: yesterday, OwnerID: ownerID, Bucket: bucket2, StorageBytes: 500},
		{UsageDate: today, OwnerID: ownerID, Bucket: bucket2, StorageBytes: 600}, // Latest for bucket2
	}

	for _, d := range dailyRecords {
		err := store.UpsertDailyUsage(ctx, d)
		require.NoError(t, err)
	}

	// Get latest for each bucket
	latest, err := store.GetLatestDailyUsage(ctx, ownerID)
	require.NoError(t, err)

	// Should have 2 records (one per bucket)
	require.Len(t, latest, 2, "should have latest record for each bucket")

	// Verify we got the latest values
	latestByBucket := make(map[string]int64)
	for _, d := range latest {
		latestByBucket[d.Bucket] = d.StorageBytes
	}

	assert.Equal(t, int64(300), latestByBucket[bucket1], "bucket1 should have latest value 300")
	assert.Equal(t, int64(600), latestByBucket[bucket2], "bucket2 should have latest value 600")

	t.Logf("Latest daily usage: bucket1=%d, bucket2=%d", latestByBucket[bucket1], latestByBucket[bucket2])
}

// =============================================================================
// Aggregation Rollup Tests
// =============================================================================

func TestUsageStore_AggregationRollup(t *testing.T) {
	testutil.SkipIfShort(t)

	store, cleanup := getUsageStore(t)
	defer cleanup()

	ctx := context.Background()
	ownerID := testutil.UniqueID("owner")
	bucket := testutil.UniqueID("bucket")

	// Simulate a day's worth of events
	dayStart := time.Now().UTC().Truncate(24 * time.Hour)

	// Generate events throughout the day
	var events []usage.UsageEvent
	for i := 0; i < 100; i++ {
		eventTime := dayStart.Add(time.Duration(i) * time.Minute)

		// Storage delta (every 10th event)
		if i%10 == 0 {
			events = append(events, usage.UsageEvent{
				EventTime:    eventTime,
				OwnerID:      ownerID,
				Bucket:       bucket,
				EventType:    usage.EventTypeStorageDelta,
				BytesDelta:   int64(1024 * (i + 1)), // Variable sizes
				StorageClass: "STANDARD",
			})
		}

		// Request events
		events = append(events, usage.UsageEvent{
			EventTime: eventTime,
			OwnerID:   ownerID,
			Bucket:    bucket,
			EventType: usage.EventTypeRequest,
			Operation: "GetObject",
		})

		// Bandwidth (every 5th event)
		if i%5 == 0 {
			events = append(events, usage.UsageEvent{
				EventTime:  eventTime,
				OwnerID:    ownerID,
				Bucket:     bucket,
				EventType:  usage.EventTypeBandwidth,
				BytesDelta: int64(1024 * 10),
				Direction:  usage.DirectionEgress,
			})
		}
	}

	err := store.InsertEvents(ctx, events)
	require.NoError(t, err, "should insert events")

	// Aggregate the day
	dayEnd := dayStart.Add(24 * time.Hour)
	summary, err := store.AggregateEvents(ctx, ownerID, bucket, dayStart, dayEnd)
	require.NoError(t, err)

	// Calculate expected values
	var expectedStorage int64
	for i := 0; i < 100; i += 10 {
		expectedStorage += int64(1024 * (i + 1))
	}
	expectedRequests := 100                       // One GetObject per iteration
	expectedBandwidth := int64(20 * 1024 * 10)    // 20 egress events * 10KB each

	assert.Equal(t, expectedStorage, summary.StorageBytes, "storage should match expected")
	assert.Equal(t, expectedRequests, summary.Requests["GetObject"], "requests should match expected")
	assert.Equal(t, expectedBandwidth, summary.BandwidthEgress, "bandwidth should match expected")

	// Create daily rollup
	daily := &usage.DailyUsage{
		UsageDate:            dayStart,
		OwnerID:              ownerID,
		Bucket:               bucket,
		StorageBytes:         summary.StorageBytes,
		ObjectCount:          summary.ObjectCount,
		RequestsGet:          summary.Requests["GetObject"],
		BandwidthEgressBytes: summary.BandwidthEgress,
	}

	err = store.UpsertDailyUsage(ctx, daily)
	require.NoError(t, err)

	// Verify daily record
	results, err := store.GetDailyUsageByBucket(ctx, ownerID, bucket, dayStart, dayStart)
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Equal(t, summary.StorageBytes, results[0].StorageBytes, "daily record should match aggregation")
	assert.Equal(t, expectedRequests, results[0].RequestsGet, "daily requests should match")

	t.Logf("Rollup complete: %d events aggregated into daily record with storage=%d, requests=%d, bandwidth=%d",
		len(events), results[0].StorageBytes, results[0].RequestsGet, results[0].BandwidthEgressBytes)
}

// =============================================================================
// Partition Tests
// =============================================================================

func TestUsageStore_PartitionInfo(t *testing.T) {
	testutil.SkipIfShort(t)

	db, cleanup := getTestDB(t)
	defer cleanup()

	v, ok := db.(*vitess.Vitess)
	require.True(t, ok)

	ctx := context.Background()

	// Get raw DB connection to check partitions
	sqlDB := v.SqlDB()

	// Check if table is partitioned
	var count int
	err := sqlDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.PARTITIONS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'usage_events'
		  AND PARTITION_NAME IS NOT NULL
	`).Scan(&count)
	require.NoError(t, err)

	if count <= 1 {
		t.Skip("usage_events table is not partitioned")
	}

	t.Logf("Found %d partitions for usage_events", count)

	// List partitions
	rows, err := sqlDB.QueryContext(ctx, `
		SELECT PARTITION_NAME, PARTITION_DESCRIPTION, TABLE_ROWS
		FROM INFORMATION_SCHEMA.PARTITIONS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'usage_events'
		  AND PARTITION_NAME IS NOT NULL
		ORDER BY PARTITION_ORDINAL_POSITION
	`)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var name, desc string
		var tableRows int64
		err := rows.Scan(&name, &desc, &tableRows)
		require.NoError(t, err)
		t.Logf("  Partition %s: boundary=%s, rows=%d", name, desc, tableRows)
	}
}

func TestUsageStore_PartitionMaintenance(t *testing.T) {
	testutil.SkipIfShort(t)

	store, cleanup := getUsageStore(t)
	defer cleanup()

	ctx := context.Background()

	// Run partition maintenance (should add future partitions if partitioned)
	err := store.RunPartitionMaintenance(ctx, 3)
	require.NoError(t, err, "partition maintenance should succeed (or no-op if not partitioned)")

	t.Log("Partition maintenance completed successfully")
}

// =============================================================================
// Retention/Cleanup Tests
// =============================================================================

func TestUsageStore_DeleteOldEvents(t *testing.T) {
	testutil.SkipIfShort(t)

	store, cleanup := getUsageStore(t)
	defer cleanup()

	ctx := context.Background()
	ownerID := testutil.UniqueID("owner")
	bucket := testutil.UniqueID("bucket")

	now := time.Now().UTC()
	oldTime := now.AddDate(0, 0, -30) // 30 days ago
	recentTime := now.AddDate(0, 0, -1) // 1 day ago

	// Insert old and recent events
	events := []usage.UsageEvent{
		{EventTime: oldTime, OwnerID: ownerID, Bucket: bucket, EventType: usage.EventTypeRequest, Operation: "GetObject"},
		{EventTime: oldTime.Add(time.Hour), OwnerID: ownerID, Bucket: bucket, EventType: usage.EventTypeRequest, Operation: "GetObject"},
		{EventTime: recentTime, OwnerID: ownerID, Bucket: bucket, EventType: usage.EventTypeRequest, Operation: "PutObject"},
		{EventTime: now, OwnerID: ownerID, Bucket: bucket, EventType: usage.EventTypeRequest, Operation: "PutObject"},
	}

	err := store.InsertEvents(ctx, events)
	require.NoError(t, err)

	// Verify events exist
	summary, err := store.AggregateEvents(ctx, ownerID, bucket, oldTime.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)
	initialTotal := summary.Requests["GetObject"] + summary.Requests["PutObject"]
	require.GreaterOrEqual(t, initialTotal, 4, "should have at least 4 events")

	// Delete events older than 7 days
	cutoff := now.AddDate(0, 0, -7)
	deleted, err := store.DeleteEventsOlderThan(ctx, cutoff)
	require.NoError(t, err)
	t.Logf("Deleted %d old events (or partitions)", deleted)

	// Verify old events are gone (note: if partitioned, this deletes by partition)
	summary, err = store.AggregateEvents(ctx, ownerID, bucket, oldTime.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)

	// Recent events should still exist
	assert.GreaterOrEqual(t, summary.Requests["PutObject"], 2, "recent events should still exist")

	t.Logf("After cleanup: GetObject=%d, PutObject=%d",
		summary.Requests["GetObject"], summary.Requests["PutObject"])
}

// =============================================================================
// Report Job Tests
// =============================================================================

func TestUsageStore_ReportJobs(t *testing.T) {
	testutil.SkipIfShort(t)

	store, cleanup := getUsageStore(t)
	defer cleanup()

	ctx := context.Background()
	ownerID := testutil.UniqueID("owner")

	// Create a report job
	now := time.Now().UTC()
	job := &usage.ReportJob{
		ID:          testutil.UniqueID("job"),
		OwnerID:     ownerID,
		PeriodStart: now.AddDate(0, -1, 0),
		PeriodEnd:   now,
		Status:      usage.JobStatusPending,
		CreatedAt:   now,
		ExpiresAt:   now.Add(24 * time.Hour),
	}

	err := store.CreateReportJob(ctx, job)
	require.NoError(t, err, "should create report job")

	// Retrieve job
	retrieved, err := store.GetReportJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, job.ID, retrieved.ID)
	assert.Equal(t, usage.JobStatusPending, retrieved.Status)

	// Update job
	retrieved.Status = usage.JobStatusCompleted
	retrieved.ResultJSON = `{"total_storage": 1000}`
	err = store.UpdateReportJob(ctx, retrieved)
	require.NoError(t, err)

	// Verify update
	updated, err := store.GetReportJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, usage.JobStatusCompleted, updated.Status)
	assert.Equal(t, `{"total_storage": 1000}`, updated.ResultJSON)

	t.Logf("Report job lifecycle verified: created -> completed")
}

// =============================================================================
// MTD (Month-to-Date) Tests
// =============================================================================

func TestUsageStore_MTDQueries(t *testing.T) {
	testutil.SkipIfShort(t)

	store, cleanup := getUsageStore(t)
	defer cleanup()

	ctx := context.Background()
	ownerID := testutil.UniqueID("owner")
	bucket := testutil.UniqueID("bucket")

	// Create daily records for the current month
	today := time.Now().UTC().Truncate(24 * time.Hour)
	monthStart := time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, time.UTC)

	// Add records for a few days this month
	for i := 0; i < 5; i++ {
		day := monthStart.AddDate(0, 0, i)
		if day.After(today) {
			break
		}

		daily := &usage.DailyUsage{
			UsageDate:            day,
			OwnerID:              ownerID,
			Bucket:               bucket,
			StorageBytes:         int64(1000 * (i + 1)),
			RequestsGet:          10,
			RequestsPut:          5,
			RequestsDelete:       2,
			RequestsList:         3,
			RequestsHead:         1,
			RequestsCopy:         1,
			RequestsOther:        2,
			BandwidthEgressBytes: int64(10000 * (i + 1)),
		}
		err := store.UpsertDailyUsage(ctx, daily)
		require.NoError(t, err)
	}

	// Query MTD requests
	mtdRequests, err := store.GetMTDRequestCount(ctx, ownerID)
	require.NoError(t, err)
	t.Logf("MTD request count: %d", mtdRequests)
	assert.Greater(t, mtdRequests, int64(0), "should have some MTD requests")

	// Query MTD bandwidth
	mtdBandwidth, err := store.GetMTDBandwidthEgress(ctx, ownerID)
	require.NoError(t, err)
	t.Logf("MTD bandwidth egress: %d", mtdBandwidth)
	assert.Greater(t, mtdBandwidth, int64(0), "should have some MTD bandwidth")
}
