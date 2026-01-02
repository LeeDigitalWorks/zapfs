//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/LeeDigitalWorks/zapfs/proto/usage_pb"
)

// usageServiceStub implements the UsageReportingService for community edition.
// All methods return Unimplemented errors.
type usageServiceStub struct {
	usage_pb.UnimplementedUsageReportingServiceServer
}

func newUsageService(cfg UsageServiceConfig) usage_pb.UsageReportingServiceServer {
	return &usageServiceStub{}
}

var errEnterpriseRequired = status.Error(codes.Unimplemented, "usage reporting requires enterprise license")

func (s *usageServiceStub) RequestReport(ctx context.Context, req *usage_pb.ReportRequest) (*usage_pb.ReportJob, error) {
	return nil, errEnterpriseRequired
}

func (s *usageServiceStub) GetReport(ctx context.Context, req *usage_pb.GetReportRequest) (*usage_pb.GetReportResponse, error) {
	return nil, errEnterpriseRequired
}

func (s *usageServiceStub) GetCurrentUsage(ctx context.Context, req *usage_pb.CurrentUsageRequest) (*usage_pb.CurrentUsage, error) {
	return nil, errEnterpriseRequired
}

func (s *usageServiceStub) StreamUsageUpdates(req *usage_pb.StreamUsageRequest, stream usage_pb.UsageReportingService_StreamUsageUpdatesServer) error {
	return errEnterpriseRequired
}

func (s *usageServiceStub) ListReports(ctx context.Context, req *usage_pb.ListReportsRequest) (*usage_pb.ListReportsResponse, error) {
	return nil, errEnterpriseRequired
}
