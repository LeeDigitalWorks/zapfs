// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"github.com/LeeDigitalWorks/zapfs/pkg/usage"
	"github.com/LeeDigitalWorks/zapfs/proto/usage_pb"
	"google.golang.org/grpc"
)

// UsageServiceConfig holds dependencies for the usage reporting gRPC service.
type UsageServiceConfig struct {
	Store     usage.Store
	Collector usage.Collector
	Config    usage.Config
}

// RegisterUsageService registers the usage reporting gRPC service.
// The implementation differs between enterprise (full functionality)
// and community (returns Unimplemented errors) editions.
func RegisterUsageService(server *grpc.Server, cfg UsageServiceConfig) {
	svc := newUsageService(cfg)
	usage_pb.RegisterUsageReportingServiceServer(server, svc)
}

// newUsageService creates the appropriate service implementation.
// This function is implemented in usage_grpc_enterprise.go and usage_grpc_stub.go
// with build tags to select the appropriate implementation at compile time.
