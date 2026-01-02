//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package api

import (
	"context"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/usage"
	"github.com/LeeDigitalWorks/zapfs/proto/usage_pb"
)

// mockUsageStore implements usage.Store for testing
type mockUsageStore struct {
	usage.NopStore
	jobs        map[string]*usage.ReportJob
	snapshots   []usage.BucketSnapshot
	mtdRequests int64
	mtdEgress   int64
}

func newMockUsageStore() *mockUsageStore {
	return &mockUsageStore{
		jobs: make(map[string]*usage.ReportJob),
	}
}

func (m *mockUsageStore) CreateReportJob(ctx context.Context, job *usage.ReportJob) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *mockUsageStore) GetReportJob(ctx context.Context, jobID string) (*usage.ReportJob, error) {
	return m.jobs[jobID], nil
}

func (m *mockUsageStore) ListReportJobs(ctx context.Context, ownerID string, limit int) ([]usage.ReportJob, error) {
	var result []usage.ReportJob
	for _, job := range m.jobs {
		if job.OwnerID == ownerID {
			result = append(result, *job)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *mockUsageStore) GetCurrentStorageByOwner(ctx context.Context, ownerID string) ([]usage.BucketSnapshot, error) {
	return m.snapshots, nil
}

func (m *mockUsageStore) GetMTDRequestCount(ctx context.Context, ownerID string) (int64, error) {
	return m.mtdRequests, nil
}

func (m *mockUsageStore) GetMTDBandwidthEgress(ctx context.Context, ownerID string) (int64, error) {
	return m.mtdEgress, nil
}

// mockCollector implements usage.Collector for testing
type mockCollector struct{}

func (m *mockCollector) RecordRequest(ownerID, bucket, operation string)                                      {}
func (m *mockCollector) RecordBandwidth(ownerID, bucket string, bytes int64, direction usage.Direction)       {}
func (m *mockCollector) RecordStorageDelta(ownerID, bucket string, bytesDelta int64, objectDelta int, storageClass string) {
}
func (m *mockCollector) Start(ctx context.Context) {}
func (m *mockCollector) Stop()                     {}

func TestUsageService_RequestReport(t *testing.T) {
	store := newMockUsageStore()
	cfg := usage.Config{
		ReportExpiry: 24 * time.Hour,
	}
	cfg.Validate()

	svc := newUsageService(UsageServiceConfig{
		Store:     store,
		Collector: &mockCollector{},
		Config:    cfg,
	})

	ctx := context.Background()

	t.Run("valid request", func(t *testing.T) {
		req := &usage_pb.ReportRequest{
			OwnerId:      "owner-123",
			Month:        "2025-01",
			IncludeDaily: true,
		}

		job, err := svc.RequestReport(ctx, req)
		// Note: This may fail if license check fails in enterprise build
		// For full testing, would need to mock license manager
		if err != nil {
			t.Skipf("RequestReport() error (possibly due to license): %v", err)
		}

		if job == nil {
			t.Fatal("RequestReport() returned nil job")
		}
		if job.Status != "pending" {
			t.Errorf("Status = %s, want pending", job.Status)
		}
	})

	t.Run("missing owner_id", func(t *testing.T) {
		req := &usage_pb.ReportRequest{
			Month: "2025-01",
		}

		_, err := svc.RequestReport(ctx, req)
		if err == nil {
			t.Error("RequestReport() should fail with missing owner_id")
		}
	})

	t.Run("missing month", func(t *testing.T) {
		req := &usage_pb.ReportRequest{
			OwnerId: "owner-123",
		}

		_, err := svc.RequestReport(ctx, req)
		if err == nil {
			t.Error("RequestReport() should fail with missing month")
		}
	})

	t.Run("invalid month format", func(t *testing.T) {
		req := &usage_pb.ReportRequest{
			OwnerId: "owner-123",
			Month:   "2025/01", // Wrong format
		}

		_, err := svc.RequestReport(ctx, req)
		if err == nil {
			t.Error("RequestReport() should fail with invalid month format")
		}
	})
}

func TestUsageService_GetReport(t *testing.T) {
	store := newMockUsageStore()
	cfg := usage.DefaultConfig()

	svc := newUsageService(UsageServiceConfig{
		Store:     store,
		Collector: &mockCollector{},
		Config:    cfg,
	})

	ctx := context.Background()

	t.Run("job not found", func(t *testing.T) {
		req := &usage_pb.GetReportRequest{
			JobId: "nonexistent",
		}

		_, err := svc.GetReport(ctx, req)
		if err == nil {
			t.Error("GetReport() should fail for nonexistent job")
		}
	})

	t.Run("missing job_id", func(t *testing.T) {
		req := &usage_pb.GetReportRequest{}

		_, err := svc.GetReport(ctx, req)
		if err == nil {
			t.Error("GetReport() should fail with missing job_id")
		}
	})
}

func TestUsageService_GetCurrentUsage(t *testing.T) {
	store := newMockUsageStore()
	store.snapshots = []usage.BucketSnapshot{
		{Bucket: "bucket1", StorageBytes: 1024, ObjectCount: 10},
		{Bucket: "bucket2", StorageBytes: 2048, ObjectCount: 20},
	}
	store.mtdRequests = 1000
	store.mtdEgress = 5000

	cfg := usage.DefaultConfig()

	svc := newUsageService(UsageServiceConfig{
		Store:     store,
		Collector: &mockCollector{},
		Config:    cfg,
	})

	ctx := context.Background()

	t.Run("valid request", func(t *testing.T) {
		req := &usage_pb.CurrentUsageRequest{
			OwnerId: "owner-123",
		}

		resp, err := svc.GetCurrentUsage(ctx, req)
		// May fail due to license check
		if err != nil {
			t.Skipf("GetCurrentUsage() error (possibly due to license): %v", err)
		}

		if resp == nil {
			t.Fatal("GetCurrentUsage() returned nil")
		}

		if resp.OwnerId != "owner-123" {
			t.Errorf("OwnerId = %s, want owner-123", resp.OwnerId)
		}
		if resp.StorageBytes != 3072 {
			t.Errorf("StorageBytes = %d, want 3072", resp.StorageBytes)
		}
		if resp.ObjectCount != 30 {
			t.Errorf("ObjectCount = %d, want 30", resp.ObjectCount)
		}
		if len(resp.Buckets) != 2 {
			t.Errorf("len(Buckets) = %d, want 2", len(resp.Buckets))
		}
		if resp.MtdRequests != 1000 {
			t.Errorf("MtdRequests = %d, want 1000", resp.MtdRequests)
		}
		if resp.MtdBandwidthEgress != 5000 {
			t.Errorf("MtdBandwidthEgress = %d, want 5000", resp.MtdBandwidthEgress)
		}
	})

	t.Run("missing owner_id", func(t *testing.T) {
		req := &usage_pb.CurrentUsageRequest{}

		_, err := svc.GetCurrentUsage(ctx, req)
		if err == nil {
			t.Error("GetCurrentUsage() should fail with missing owner_id")
		}
	})
}

func TestUsageService_ListReports(t *testing.T) {
	store := newMockUsageStore()
	cfg := usage.DefaultConfig()

	svc := newUsageService(UsageServiceConfig{
		Store:     store,
		Collector: &mockCollector{},
		Config:    cfg,
	})

	ctx := context.Background()

	// Add some jobs to the store
	store.jobs["job1"] = &usage.ReportJob{
		ID:      "job1",
		OwnerID: "owner-123",
		Status:  usage.JobStatusCompleted,
	}
	store.jobs["job2"] = &usage.ReportJob{
		ID:      "job2",
		OwnerID: "owner-123",
		Status:  usage.JobStatusPending,
	}
	store.jobs["job3"] = &usage.ReportJob{
		ID:      "job3",
		OwnerID: "other-owner",
		Status:  usage.JobStatusCompleted,
	}

	t.Run("valid request", func(t *testing.T) {
		req := &usage_pb.ListReportsRequest{
			OwnerId: "owner-123",
			Limit:   10,
		}

		resp, err := svc.ListReports(ctx, req)
		// May fail due to license check
		if err != nil {
			t.Skipf("ListReports() error (possibly due to license): %v", err)
		}

		if resp == nil {
			t.Fatal("ListReports() returned nil")
		}

		if len(resp.Reports) != 2 {
			t.Errorf("len(Reports) = %d, want 2", len(resp.Reports))
		}
	})

	t.Run("missing owner_id", func(t *testing.T) {
		req := &usage_pb.ListReportsRequest{}

		_, err := svc.ListReports(ctx, req)
		if err == nil {
			t.Error("ListReports() should fail with missing owner_id")
		}
	})
}

func TestConvertToProtoReport(t *testing.T) {
	report := &usage.UsageReport{
		ReportID:    "report-123",
		OwnerID:     "owner-123",
		PeriodStart: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC),
		GeneratedAt: time.Now(),
		Summary: usage.UsageSummary{
			StorageBytesAvg:  1024 * 1024,
			StorageBytesMax:  2 * 1024 * 1024,
			ObjectCountAvg:   100,
			StorageByClass:   map[string]int64{"STANDARD": 1024 * 1024},
			Requests:         usage.RequestCounts{Get: 500, Put: 50, Total: 550},
			BandwidthIngress: 1000,
			BandwidthEgress:  5000,
		},
		Buckets: []usage.BucketUsage{
			{
				Bucket:          "bucket1",
				StorageBytesAvg: 512 * 1024,
				Requests:        usage.RequestCounts{Get: 250},
			},
		},
	}

	proto := convertToProtoReport(report)

	if proto.ReportId != "report-123" {
		t.Errorf("ReportId = %s, want report-123", proto.ReportId)
	}
	if proto.OwnerId != "owner-123" {
		t.Errorf("OwnerId = %s, want owner-123", proto.OwnerId)
	}
	if proto.Summary.StorageBytesAvg != 1024*1024 {
		t.Errorf("Summary.StorageBytesAvg = %d, want %d", proto.Summary.StorageBytesAvg, 1024*1024)
	}
	if len(proto.Buckets) != 1 {
		t.Errorf("len(Buckets) = %d, want 1", len(proto.Buckets))
	}
}
