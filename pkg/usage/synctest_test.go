package usage

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestAggregator_RunNow_Synctest tests the real Aggregator.RunNow() method.
// Synctest controls time.Now() which affects date calculations.
func TestAggregator_RunNow_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		store := NewMockStore()

		// Synctest starts at 2000-01-01 00:00:00 UTC
		now := time.Now().UTC()
		assert.Equal(t, 2000, now.Year())
		assert.Equal(t, time.January, now.Month())
		assert.Equal(t, 1, now.Day())

		// Add events for "yesterday" (1999-12-31)
		yesterday := now.AddDate(0, 0, -1)
		dayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)

		events := []UsageEvent{
			{
				EventTime:    dayStart.Add(1 * time.Hour),
				OwnerID:      "owner1",
				Bucket:       "bucket1",
				EventType:    EventTypeStorageDelta,
				BytesDelta:   1024,
				StorageClass: "STANDARD",
			},
			{
				EventTime:  dayStart.Add(2 * time.Hour),
				OwnerID:    "owner1",
				Bucket:     "bucket1",
				EventType:  EventTypeRequest,
				Operation:  "GetObject",
			},
		}
		_ = store.InsertEvents(context.Background(), events)

		agg := NewAggregator(cfg, store)

		// Call the REAL RunNow method
		err := agg.RunNow(context.Background())
		assert.NoError(t, err)
	})
}

// TestAggregator_Retention_Synctest tests retention cleanup with controlled time.
func TestAggregator_Retention_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := Config{RetentionDays: 7}
		cfg.Validate()

		store := NewMockStore()

		// Synctest time starts at 2000-01-01 00:00:00 UTC
		now := time.Now().UTC()
		oldTime := now.AddDate(0, 0, -10) // 10 days ago - should be deleted
		newTime := now.AddDate(0, 0, -1)  // 1 day ago - should be kept

		events := []UsageEvent{
			{EventTime: oldTime, OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeRequest},
			{EventTime: oldTime, OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeRequest},
			{EventTime: newTime, OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeRequest},
		}
		_ = store.InsertEvents(context.Background(), events)

		assert.Equal(t, 3, len(store.events))

		agg := NewAggregator(cfg, store)

		// Call the REAL runRetention method
		deleted, err := agg.runRetention(context.Background())

		assert.NoError(t, err)
		assert.Equal(t, int64(2), deleted)
		assert.Equal(t, 1, len(store.events))
	})
}

// TestAggregator_AggregateDay_Synctest tests daily aggregation with controlled time.
func TestAggregator_AggregateDay_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		store := NewMockStore()

		// Synctest time starts at 2000-01-01 00:00:00 UTC
		now := time.Now().UTC()
		yesterday := now.AddDate(0, 0, -1)
		dayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)

		events := []UsageEvent{
			{
				EventTime:    dayStart.Add(1 * time.Hour),
				OwnerID:      "owner1",
				Bucket:       "bucket1",
				EventType:    EventTypeStorageDelta,
				BytesDelta:   1024 * 1024,
				StorageClass: "STANDARD",
			},
			{
				EventTime:  dayStart.Add(2 * time.Hour),
				OwnerID:    "owner1",
				Bucket:     "bucket1",
				EventType:  EventTypeObjectDelta,
				CountDelta: 5,
			},
			{
				EventTime:  dayStart.Add(3 * time.Hour),
				OwnerID:    "owner1",
				Bucket:     "bucket1",
				EventType:  EventTypeBandwidth,
				BytesDelta: 2048,
				Direction:  DirectionEgress,
			},
		}
		_ = store.InsertEvents(context.Background(), events)

		agg := NewAggregator(cfg, store)

		// Call the REAL aggregateDay method
		err := agg.aggregateDay(context.Background(), yesterday)
		assert.NoError(t, err)

		// Verify daily usage was created
		usage, err := store.GetDailyUsage(context.Background(), "owner1", dayStart, dayStart.AddDate(0, 0, 1))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(usage))

		if len(usage) > 0 {
			daily := usage[0]
			assert.Equal(t, int64(1024*1024), daily.StorageBytes)
			assert.Equal(t, int64(5), daily.ObjectCount)
			assert.Equal(t, int64(2048), daily.BandwidthEgressBytes)
		}
	})
}

// TestAggregator_CumulativeStorage_Synctest tests that storage accumulates across days.
func TestAggregator_CumulativeStorage_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		store := NewMockStore()

		now := time.Now().UTC()
		twoDaysAgo := now.AddDate(0, 0, -2)
		yesterday := now.AddDate(0, 0, -1)
		twoDaysStart := time.Date(twoDaysAgo.Year(), twoDaysAgo.Month(), twoDaysAgo.Day(), 0, 0, 0, 0, time.UTC)
		yesterdayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)

		// Set up previous day's aggregated data
		_ = store.UpsertDailyUsage(context.Background(), &DailyUsage{
			UsageDate:    twoDaysStart,
			OwnerID:      "owner1",
			Bucket:       "bucket1",
			StorageBytes: 1000,
			ObjectCount:  10,
		})

		// Add events for yesterday (delta on top of previous day)
		events := []UsageEvent{
			{EventTime: yesterdayStart.Add(1 * time.Hour), OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeStorageDelta, BytesDelta: 500},
			{EventTime: yesterdayStart.Add(2 * time.Hour), OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeObjectDelta, CountDelta: 5},
		}
		_ = store.InsertEvents(context.Background(), events)

		agg := NewAggregator(cfg, store)

		// Aggregate yesterday
		err := agg.aggregateDay(context.Background(), yesterday)
		assert.NoError(t, err)

		// Verify cumulative: 1000 (prev) + 500 (delta) = 1500
		usage, _ := store.GetDailyUsageByBucket(context.Background(), "owner1", "bucket1", yesterdayStart, yesterdayStart.AddDate(0, 0, 1))
		assert.Equal(t, 1, len(usage))

		if len(usage) > 0 {
			assert.Equal(t, int64(1500), usage[0].StorageBytes)
			assert.Equal(t, int64(15), usage[0].ObjectCount)
		}
	})
}

// TestConfig_RetentionDuration_Synctest tests config methods with controlled time.
func TestConfig_RetentionDuration_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := Config{RetentionDays: 7}
		cfg.Validate()

		// Verify retention calculation
		duration := cfg.RetentionDuration()
		assert.Equal(t, 7*24*time.Hour, duration)

		// Calculate cutoff using synctest time
		now := time.Now().UTC()
		cutoff := now.Add(-cfg.RetentionDuration())

		// Should be 7 days ago from 2000-01-01
		expected := now.AddDate(0, 0, -7)
		assert.Equal(t, expected.Year(), cutoff.Year())
		assert.Equal(t, expected.Month(), cutoff.Month())
		assert.Equal(t, expected.Day(), cutoff.Day())
	})
}
