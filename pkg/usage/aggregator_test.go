// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package usage

import (
	"context"
	"testing"
	"time"
)

func TestAggregator_New(t *testing.T) {
	cfg := DefaultConfig()
	store := &NopStore{}

	agg := NewAggregator(cfg, store)
	if agg == nil {
		t.Fatal("NewAggregator() returned nil")
	}

	if agg.cfg.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want 30", agg.cfg.RetentionDays)
	}
}

func TestAggregator_StartStop(t *testing.T) {
	cfg := DefaultConfig()
	store := &NopStore{}

	agg := NewAggregator(cfg, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should not panic
	agg.Start(ctx)

	// Give it a moment
	time.Sleep(10 * time.Millisecond)

	// Stop should not panic
	agg.Stop()
}

func TestAggregator_RunNow(t *testing.T) {
	cfg := DefaultConfig()
	store := NewMockStore()

	agg := NewAggregator(cfg, store)

	ctx := context.Background()

	// Should complete without error (even with empty store)
	err := agg.RunNow(ctx)
	if err != nil {
		t.Errorf("RunNow() error = %v, want nil", err)
	}
}

func TestAggregator_AggregateDay(t *testing.T) {
	cfg := DefaultConfig()
	store := NewMockStore()

	// Add some events
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
			EventTime: dayStart.Add(3 * time.Hour),
			OwnerID:   "owner1",
			Bucket:    "bucket1",
			EventType: EventTypeRequest,
			Operation: "GetObject",
		},
		{
			EventTime: dayStart.Add(4 * time.Hour),
			OwnerID:   "owner1",
			Bucket:    "bucket1",
			EventType: EventTypeRequest,
			Operation: "PutObject",
		},
		{
			EventTime:  dayStart.Add(5 * time.Hour),
			OwnerID:    "owner1",
			Bucket:     "bucket1",
			EventType:  EventTypeBandwidth,
			BytesDelta: 2048,
			Direction:  DirectionEgress,
		},
	}

	ctx := context.Background()
	if err := store.InsertEvents(ctx, events); err != nil {
		t.Fatalf("InsertEvents() error = %v", err)
	}

	agg := NewAggregator(cfg, store)

	// Run aggregation for yesterday
	err := agg.aggregateDay(ctx, yesterday)
	if err != nil {
		t.Errorf("aggregateDay() error = %v", err)
	}

	// Check daily usage was created
	usage, err := store.GetDailyUsage(ctx, "owner1", dayStart, dayStart.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("GetDailyUsage() error = %v", err)
	}

	if len(usage) != 1 {
		t.Fatalf("len(usage) = %d, want 1", len(usage))
	}

	daily := usage[0]
	if daily.StorageBytes != 1024*1024 {
		t.Errorf("StorageBytes = %d, want %d", daily.StorageBytes, 1024*1024)
	}
	if daily.ObjectCount != 5 {
		t.Errorf("ObjectCount = %d, want 5", daily.ObjectCount)
	}
	if daily.BandwidthEgressBytes != 2048 {
		t.Errorf("BandwidthEgressBytes = %d, want 2048", daily.BandwidthEgressBytes)
	}
}

func TestAggregator_Retention(t *testing.T) {
	cfg := Config{
		RetentionDays: 7,
	}
	cfg.Validate()

	store := NewMockStore()

	// Add old and new events
	now := time.Now().UTC()
	oldTime := now.AddDate(0, 0, -10) // 10 days ago
	newTime := now.AddDate(0, 0, -1)  // 1 day ago

	events := []UsageEvent{
		{EventTime: oldTime, OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeRequest},
		{EventTime: oldTime, OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeRequest},
		{EventTime: newTime, OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeRequest},
	}

	ctx := context.Background()
	if err := store.InsertEvents(ctx, events); err != nil {
		t.Fatalf("InsertEvents() error = %v", err)
	}

	if len(store.events) != 3 {
		t.Fatalf("events count = %d, want 3", len(store.events))
	}

	agg := NewAggregator(cfg, store)

	// Run retention
	deleted, err := agg.runRetention(ctx)
	if err != nil {
		t.Fatalf("runRetention() error = %v", err)
	}

	if deleted != 2 {
		t.Errorf("deleted = %d, want 2", deleted)
	}

	if len(store.events) != 1 {
		t.Errorf("remaining events = %d, want 1", len(store.events))
	}
}

func TestAggregator_MultipleBuckets(t *testing.T) {
	cfg := DefaultConfig()
	store := NewMockStore()

	now := time.Now().UTC()
	yesterday := now.AddDate(0, 0, -1)
	dayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)

	events := []UsageEvent{
		{EventTime: dayStart.Add(1 * time.Hour), OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeStorageDelta, BytesDelta: 1024},
		{EventTime: dayStart.Add(2 * time.Hour), OwnerID: "owner1", Bucket: "bucket2", EventType: EventTypeStorageDelta, BytesDelta: 2048},
		{EventTime: dayStart.Add(3 * time.Hour), OwnerID: "owner2", Bucket: "bucket3", EventType: EventTypeStorageDelta, BytesDelta: 4096},
	}

	ctx := context.Background()
	if err := store.InsertEvents(ctx, events); err != nil {
		t.Fatalf("InsertEvents() error = %v", err)
	}

	agg := NewAggregator(cfg, store)

	err := agg.aggregateDay(ctx, yesterday)
	if err != nil {
		t.Errorf("aggregateDay() error = %v", err)
	}

	// Check all three owner/bucket pairs have daily usage
	usage1, _ := store.GetDailyUsageByBucket(ctx, "owner1", "bucket1", dayStart, dayStart.AddDate(0, 0, 1))
	usage2, _ := store.GetDailyUsageByBucket(ctx, "owner1", "bucket2", dayStart, dayStart.AddDate(0, 0, 1))
	usage3, _ := store.GetDailyUsageByBucket(ctx, "owner2", "bucket3", dayStart, dayStart.AddDate(0, 0, 1))

	if len(usage1) != 1 || usage1[0].StorageBytes != 1024 {
		t.Error("bucket1 aggregation incorrect")
	}
	if len(usage2) != 1 || usage2[0].StorageBytes != 2048 {
		t.Error("bucket2 aggregation incorrect")
	}
	if len(usage3) != 1 || usage3[0].StorageBytes != 4096 {
		t.Error("bucket3 aggregation incorrect")
	}
}

func TestAggregator_CumulativeStorage(t *testing.T) {
	cfg := DefaultConfig()
	store := NewMockStore()

	now := time.Now().UTC()
	twoDaysAgo := now.AddDate(0, 0, -2)
	yesterday := now.AddDate(0, 0, -1)
	twoDaysStart := time.Date(twoDaysAgo.Year(), twoDaysAgo.Month(), twoDaysAgo.Day(), 0, 0, 0, 0, time.UTC)
	yesterdayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)

	ctx := context.Background()

	// First, add previous day's aggregation
	if err := store.UpsertDailyUsage(ctx, &DailyUsage{
		UsageDate:    twoDaysStart,
		OwnerID:      "owner1",
		Bucket:       "bucket1",
		StorageBytes: 1000,
		ObjectCount:  10,
	}); err != nil {
		t.Fatalf("UpsertDailyUsage() error = %v", err)
	}

	// Add events for yesterday
	events := []UsageEvent{
		{EventTime: yesterdayStart.Add(1 * time.Hour), OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeStorageDelta, BytesDelta: 500},
		{EventTime: yesterdayStart.Add(2 * time.Hour), OwnerID: "owner1", Bucket: "bucket1", EventType: EventTypeObjectDelta, CountDelta: 5},
	}
	if err := store.InsertEvents(ctx, events); err != nil {
		t.Fatalf("InsertEvents() error = %v", err)
	}

	agg := NewAggregator(cfg, store)

	err := agg.aggregateDay(ctx, yesterday)
	if err != nil {
		t.Errorf("aggregateDay() error = %v", err)
	}

	// Check cumulative storage: 1000 (prev) + 500 (delta) = 1500
	usage, _ := store.GetDailyUsageByBucket(ctx, "owner1", "bucket1", yesterdayStart, yesterdayStart.AddDate(0, 0, 1))
	if len(usage) != 1 {
		t.Fatalf("len(usage) = %d, want 1", len(usage))
	}

	if usage[0].StorageBytes != 1500 {
		t.Errorf("StorageBytes = %d, want 1500 (cumulative)", usage[0].StorageBytes)
	}
	if usage[0].ObjectCount != 15 {
		t.Errorf("ObjectCount = %d, want 15 (cumulative)", usage[0].ObjectCount)
	}
}
