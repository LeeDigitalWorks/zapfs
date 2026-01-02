//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/LeeDigitalWorks/zapfs/pkg/usage"
	"github.com/LeeDigitalWorks/zapfs/proto/usage_pb"
)

// usageServiceEnterprise implements the UsageReportingService for enterprise edition.
type usageServiceEnterprise struct {
	usage_pb.UnimplementedUsageReportingServiceServer

	store     usage.Store
	collector usage.Collector
	config    usage.Config
}

func newUsageService(cfg UsageServiceConfig) usage_pb.UsageReportingServiceServer {
	return &usageServiceEnterprise{
		store:     cfg.Store,
		collector: cfg.Collector,
		config:    cfg.Config,
	}
}

// RequestReport queues an async usage report generation job.
func (s *usageServiceEnterprise) RequestReport(ctx context.Context, req *usage_pb.ReportRequest) (*usage_pb.ReportJob, error) {
	if !usage.IsUsageReportingEnabled() {
		return nil, status.Error(codes.FailedPrecondition, "usage reporting requires enterprise license with AdvancedMetrics feature")
	}

	if req.OwnerId == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_id is required")
	}

	if req.Month == "" {
		return nil, status.Error(codes.InvalidArgument, "month is required (format: 2025-01)")
	}

	// Parse month format "2025-01"
	periodStart, err := time.Parse("2006-01", req.Month)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid month format, expected YYYY-MM")
	}
	periodEnd := periodStart.AddDate(0, 1, 0).Add(-time.Nanosecond)

	// Create job
	job := &usage.ReportJob{
		ID:           uuid.New().String(),
		OwnerID:      req.OwnerId,
		PeriodStart:  periodStart,
		PeriodEnd:    periodEnd,
		IncludeDaily: req.IncludeDaily,
		Status:       usage.JobStatusPending,
		ProgressPct:  0,
		CreatedAt:    time.Now().UTC(),
		ExpiresAt:    time.Now().UTC().Add(s.config.ReportExpiry),
	}

	if err := s.store.CreateReportJob(ctx, job); err != nil {
		log.Error().Err(err).Str("owner", req.OwnerId).Msg("failed to create report job")
		return nil, status.Error(codes.Internal, "failed to create report job")
	}

	log.Info().
		Str("job_id", job.ID).
		Str("owner", req.OwnerId).
		Str("month", req.Month).
		Bool("include_daily", req.IncludeDaily).
		Msg("report job created")

	return &usage_pb.ReportJob{
		JobId:            job.ID,
		Status:           string(job.Status),
		ProgressPct:      int32(job.ProgressPct),
		EstimatedSeconds: 30, // Rough estimate
	}, nil
}

// GetReport retrieves the status or result of a report job.
func (s *usageServiceEnterprise) GetReport(ctx context.Context, req *usage_pb.GetReportRequest) (*usage_pb.GetReportResponse, error) {
	if !usage.IsUsageReportingEnabled() {
		return nil, status.Error(codes.FailedPrecondition, "usage reporting requires enterprise license")
	}

	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	job, err := s.store.GetReportJob(ctx, req.JobId)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to get report job")
	}
	if job == nil {
		return nil, status.Error(codes.NotFound, "report job not found")
	}

	resp := &usage_pb.GetReportResponse{
		JobId:        job.ID,
		Status:       string(job.Status),
		ProgressPct:  int32(job.ProgressPct),
		ErrorMessage: job.ErrorMessage,
	}

	// If completed, parse and include the report
	if job.Status == usage.JobStatusCompleted && job.ResultJSON != "" {
		var report usage.UsageReport
		if err := json.Unmarshal([]byte(job.ResultJSON), &report); err != nil {
			log.Error().Err(err).Str("job_id", job.ID).Msg("failed to unmarshal report")
		} else {
			resp.Report = convertToProtoReport(&report)
		}
	}

	return resp, nil
}

// GetCurrentUsage returns real-time usage estimate.
func (s *usageServiceEnterprise) GetCurrentUsage(ctx context.Context, req *usage_pb.CurrentUsageRequest) (*usage_pb.CurrentUsage, error) {
	if !usage.IsUsageReportingEnabled() {
		return nil, status.Error(codes.FailedPrecondition, "usage reporting requires enterprise license")
	}

	if req.OwnerId == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_id is required")
	}

	// Get current storage by bucket
	buckets, err := s.store.GetCurrentStorageByOwner(ctx, req.OwnerId)
	if err != nil {
		log.Error().Err(err).Str("owner", req.OwnerId).Msg("failed to get current storage")
		return nil, status.Error(codes.Internal, "failed to get current storage")
	}

	// Get MTD request count
	mtdRequests, err := s.store.GetMTDRequestCount(ctx, req.OwnerId)
	if err != nil {
		log.Error().Err(err).Str("owner", req.OwnerId).Msg("failed to get MTD requests")
		// Continue with 0
	}

	// Get MTD egress bandwidth
	mtdEgress, err := s.store.GetMTDBandwidthEgress(ctx, req.OwnerId)
	if err != nil {
		log.Error().Err(err).Str("owner", req.OwnerId).Msg("failed to get MTD egress")
		// Continue with 0
	}

	// Calculate totals
	var totalStorage, totalObjects int64
	protoBuckets := make([]*usage_pb.BucketSnapshot, len(buckets))
	for i, b := range buckets {
		totalStorage += b.StorageBytes
		totalObjects += b.ObjectCount
		protoBuckets[i] = &usage_pb.BucketSnapshot{
			Bucket:       b.Bucket,
			StorageBytes: b.StorageBytes,
			ObjectCount:  b.ObjectCount,
		}
	}

	return &usage_pb.CurrentUsage{
		OwnerId:            req.OwnerId,
		AsOf:               timestamppb.Now(),
		StorageBytes:       totalStorage,
		ObjectCount:        totalObjects,
		Buckets:            protoBuckets,
		MtdRequests:        mtdRequests,
		MtdBandwidthEgress: mtdEgress,
	}, nil
}

// StreamUsageUpdates streams real-time usage updates.
func (s *usageServiceEnterprise) StreamUsageUpdates(req *usage_pb.StreamUsageRequest, stream usage_pb.UsageReportingService_StreamUsageUpdatesServer) error {
	if !usage.IsUsageReportingEnabled() {
		return status.Error(codes.FailedPrecondition, "usage reporting requires enterprise license")
	}

	if req.OwnerId == "" {
		return status.Error(codes.InvalidArgument, "owner_id is required")
	}

	// Create a channel to receive events
	eventCh := make(chan usage.UsageEvent, 100)

	// Subscribe to events for this owner
	// Note: The collector needs to support subscription (added in collector_enterprise.go)
	if subscriber, ok := s.collector.(subscribableCollector); ok {
		subscriber.Subscribe(req.OwnerId, eventCh)
		defer subscriber.Unsubscribe(req.OwnerId, eventCh)
	} else {
		// Fallback: just send periodic updates
		return s.streamPeriodicUpdates(req, stream)
	}

	// Build bucket filter
	bucketFilter := make(map[string]bool)
	for _, b := range req.Buckets {
		bucketFilter[b] = true
	}

	// Stream events
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case event := <-eventCh:
			// Apply bucket filter
			if len(bucketFilter) > 0 && !bucketFilter[event.Bucket] {
				continue
			}

			update := &usage_pb.UsageUpdate{
				Timestamp: timestamppb.New(event.EventTime),
				Bucket:    event.Bucket,
				EventType: string(event.EventType),
				Delta:     event.BytesDelta,
				Operation: event.Operation,
			}

			if err := stream.Send(update); err != nil {
				return err
			}
		}
	}
}

// streamPeriodicUpdates sends periodic current usage updates when streaming isn't supported.
func (s *usageServiceEnterprise) streamPeriodicUpdates(req *usage_pb.StreamUsageRequest, stream usage_pb.UsageReportingService_StreamUsageUpdatesServer) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			// Send a heartbeat update
			update := &usage_pb.UsageUpdate{
				Timestamp: timestamppb.Now(),
				EventType: "heartbeat",
			}
			if err := stream.Send(update); err != nil {
				return err
			}
		}
	}
}

// ListReports lists recent report jobs for an owner.
func (s *usageServiceEnterprise) ListReports(ctx context.Context, req *usage_pb.ListReportsRequest) (*usage_pb.ListReportsResponse, error) {
	if !usage.IsUsageReportingEnabled() {
		return nil, status.Error(codes.FailedPrecondition, "usage reporting requires enterprise license")
	}

	if req.OwnerId == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_id is required")
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 10
	}

	jobs, err := s.store.ListReportJobs(ctx, req.OwnerId, limit)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list report jobs")
	}

	protoJobs := make([]*usage_pb.ReportJob, len(jobs))
	for i, j := range jobs {
		protoJobs[i] = &usage_pb.ReportJob{
			JobId:       j.ID,
			Status:      string(j.Status),
			ProgressPct: int32(j.ProgressPct),
		}
	}

	return &usage_pb.ListReportsResponse{
		Reports: protoJobs,
	}, nil
}

// subscribableCollector defines the subscription interface for streaming.
// The enterprise collector implements this interface.
type subscribableCollector interface {
	Subscribe(ownerID string, ch chan usage.UsageEvent)
	Unsubscribe(ownerID string, ch chan usage.UsageEvent)
}

// convertToProtoReport converts internal UsageReport to proto format.
func convertToProtoReport(r *usage.UsageReport) *usage_pb.UsageReport {
	proto := &usage_pb.UsageReport{
		ReportId:    r.ReportID,
		OwnerId:     r.OwnerID,
		PeriodStart: timestamppb.New(r.PeriodStart),
		PeriodEnd:   timestamppb.New(r.PeriodEnd),
		GeneratedAt: timestamppb.New(r.GeneratedAt),
		Summary:     convertToProtoSummary(&r.Summary),
		Buckets:     make([]*usage_pb.BucketUsage, len(r.Buckets)),
	}

	for i, b := range r.Buckets {
		proto.Buckets[i] = convertToProtoBucketUsage(&b)
	}

	return proto
}

func convertToProtoSummary(s *usage.UsageSummary) *usage_pb.UsageSummary {
	return &usage_pb.UsageSummary{
		StorageBytesAvg:  s.StorageBytesAvg,
		StorageBytesMax:  s.StorageBytesMax,
		ObjectCountAvg:   s.ObjectCountAvg,
		StorageByClass:   s.StorageByClass,
		Requests:         convertToProtoRequestCounts(&s.Requests),
		BandwidthIngress: s.BandwidthIngress,
		BandwidthEgress:  s.BandwidthEgress,
	}
}

func convertToProtoBucketUsage(b *usage.BucketUsage) *usage_pb.BucketUsage {
	proto := &usage_pb.BucketUsage{
		Bucket:           b.Bucket,
		StorageBytesAvg:  b.StorageBytesAvg,
		StorageBytesMax:  b.StorageBytesMax,
		ObjectCountAvg:   b.ObjectCountAvg,
		StorageByClass:   b.StorageByClass,
		Requests:         convertToProtoRequestCounts(&b.Requests),
		BandwidthIngress: b.BandwidthIngress,
		BandwidthEgress:  b.BandwidthEgress,
		Daily:            make([]*usage_pb.DailyUsage, len(b.Daily)),
	}

	for i, d := range b.Daily {
		proto.Daily[i] = &usage_pb.DailyUsage{
			Date:             d.Date,
			StorageBytes:     d.StorageBytes,
			ObjectCount:      d.ObjectCount,
			Requests:         convertToProtoRequestCounts(&d.Requests),
			BandwidthIngress: d.BandwidthIngress,
			BandwidthEgress:  d.BandwidthEgress,
		}
	}

	return proto
}

func convertToProtoRequestCounts(r *usage.RequestCounts) *usage_pb.RequestCounts {
	return &usage_pb.RequestCounts{
		Get:    r.Get,
		Put:    r.Put,
		Delete: r.Delete,
		List:   r.List,
		Head:   r.Head,
		Copy:   r.Copy,
		Other:  r.Other,
		Total:  r.Total,
	}
}
