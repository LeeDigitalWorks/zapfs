// Package client defines interfaces for external service clients used by the metadata server.
// These interfaces are separate from the implementations to avoid import cycles with mocks.
package client

import (
	"context"

	"zapfs/proto/manager_pb"
)

// Manager defines the manager service operations needed by MetadataServer.
// This interface allows for easy mocking in tests while the production code
// uses ManagerClientPool which provides leader-aware routing.
type Manager interface {
	// Collection operations (buckets)
	CreateCollection(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error)
	DeleteCollection(ctx context.Context, req *manager_pb.DeleteCollectionRequest) (*manager_pb.DeleteCollectionResponse, error)

	// Replication
	GetReplicationTargets(ctx context.Context, req *manager_pb.GetReplicationTargetsRequest) (*manager_pb.GetReplicationTargetsResponse, error)

	// Topology (PUSH-based streaming)
	WatchTopology(ctx context.Context, req *manager_pb.WatchTopologyRequest) (manager_pb.ManagerService_WatchTopologyClient, error)

	// Lifecycle
	Close() error
}
