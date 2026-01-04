// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package usage

import (
	"testing"
	"time"
)

func TestEventTypes(t *testing.T) {
	tests := []struct {
		eventType EventType
		expected  string
	}{
		{EventTypeStorageDelta, "storage_delta"},
		{EventTypeObjectDelta, "object_delta"},
		{EventTypeRequest, "request"},
		{EventTypeBandwidth, "bandwidth"},
	}

	for _, tt := range tests {
		if string(tt.eventType) != tt.expected {
			t.Errorf("EventType %v = %s, want %s", tt.eventType, tt.eventType, tt.expected)
		}
	}
}

func TestDirections(t *testing.T) {
	tests := []struct {
		direction Direction
		expected  string
	}{
		{DirectionIngress, "ingress"},
		{DirectionEgress, "egress"},
	}

	for _, tt := range tests {
		if string(tt.direction) != tt.expected {
			t.Errorf("Direction %v = %s, want %s", tt.direction, tt.direction, tt.expected)
		}
	}
}

func TestJobStatuses(t *testing.T) {
	tests := []struct {
		status   JobStatus
		expected string
	}{
		{JobStatusPending, "pending"},
		{JobStatusProcessing, "processing"},
		{JobStatusCompleted, "completed"},
		{JobStatusFailed, "failed"},
	}

	for _, tt := range tests {
		if string(tt.status) != tt.expected {
			t.Errorf("JobStatus %v = %s, want %s", tt.status, tt.status, tt.expected)
		}
	}
}

func TestUsageEvent(t *testing.T) {
	now := time.Now()
	event := UsageEvent{
		ID:           1,
		EventTime:    now,
		OwnerID:      "owner-123",
		Bucket:       "my-bucket",
		EventType:    EventTypeRequest,
		BytesDelta:   1024,
		CountDelta:   1,
		Operation:    "GetObject",
		Direction:    DirectionEgress,
		StorageClass: "STANDARD",
	}

	if event.OwnerID != "owner-123" {
		t.Errorf("OwnerID = %s, want owner-123", event.OwnerID)
	}
	if event.Bucket != "my-bucket" {
		t.Errorf("Bucket = %s, want my-bucket", event.Bucket)
	}
	if event.EventType != EventTypeRequest {
		t.Errorf("EventType = %s, want request", event.EventType)
	}
}

func TestDailyUsage(t *testing.T) {
	now := time.Now()
	daily := DailyUsage{
		ID:                   1,
		UsageDate:            now,
		OwnerID:              "owner-123",
		Bucket:               "my-bucket",
		StorageBytes:         1024 * 1024,
		StorageBytesStandard: 1024 * 1024,
		ObjectCount:          100,
		RequestsGet:          500,
		RequestsPut:          50,
		BandwidthEgressBytes: 1024 * 1024 * 10,
	}

	if daily.StorageBytes != 1024*1024 {
		t.Errorf("StorageBytes = %d, want %d", daily.StorageBytes, 1024*1024)
	}
	if daily.ObjectCount != 100 {
		t.Errorf("ObjectCount = %d, want 100", daily.ObjectCount)
	}
}

func TestReportJob(t *testing.T) {
	now := time.Now()
	job := ReportJob{
		ID:           "job-123",
		OwnerID:      "owner-123",
		PeriodStart:  now.AddDate(0, -1, 0),
		PeriodEnd:    now,
		IncludeDaily: true,
		Status:       JobStatusPending,
		ProgressPct:  0,
		CreatedAt:    now,
		ExpiresAt:    now.Add(24 * time.Hour),
	}

	if job.Status != JobStatusPending {
		t.Errorf("Status = %s, want pending", job.Status)
	}
	if !job.IncludeDaily {
		t.Error("IncludeDaily should be true")
	}
}

func TestRequestCounts(t *testing.T) {
	counts := RequestCounts{
		Get:    100,
		Put:    50,
		Delete: 10,
		List:   25,
		Head:   15,
		Copy:   5,
		Other:  3,
	}

	counts.Total = counts.Get + counts.Put + counts.Delete + counts.List + counts.Head + counts.Copy + counts.Other

	if counts.Total != 208 {
		t.Errorf("Total = %d, want 208", counts.Total)
	}
}
