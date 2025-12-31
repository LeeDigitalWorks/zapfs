//go:build integration

package testutil

import (
	"context"
	"io"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/stretchr/testify/require"
)

// ManagerClient wraps manager_pb.ManagerServiceClient with test helpers
type ManagerClient struct {
	manager_pb.ManagerServiceClient
	t    *testing.T
	addr string
}

// NewManagerClient creates a manager service client with test helpers
func NewManagerClient(t *testing.T, addr string) *ManagerClient {
	t.Helper()
	conn := NewGRPCConn(t, addr)
	return &ManagerClient{
		ManagerServiceClient: manager_pb.NewManagerServiceClient(conn),
		t:                    t,
		addr:                 addr,
	}
}

// Ping pings the manager server
func (mc *ManagerClient) Ping() *manager_pb.PingResponse {
	mc.t.Helper()

	ctx, cancel := WithShortTimeout(context.Background())
	defer cancel()

	resp, err := mc.ManagerServiceClient.Ping(ctx, &manager_pb.PingRequest{})
	require.NoError(mc.t, err, "failed to ping manager server at %s", mc.addr)
	return resp
}

// CreateCollection creates a new collection
func (mc *ManagerClient) CreateCollection(name string) *manager_pb.CreateCollectionResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := mc.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name: name,
	})
	require.NoError(mc.t, err, "failed to create collection %s", name)
	return resp
}

// GetCollection gets a collection
func (mc *ManagerClient) GetCollection(name string) *manager_pb.GetCollectionResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := mc.ManagerServiceClient.GetCollection(ctx, &manager_pb.GetCollectionRequest{
		Name: name,
	})
	require.NoError(mc.t, err, "failed to get collection %s", name)
	return resp
}

// DeleteCollection deletes a collection
func (mc *ManagerClient) DeleteCollection(name string) *manager_pb.DeleteCollectionResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := mc.ManagerServiceClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{
		Name: name,
	})
	require.NoError(mc.t, err, "failed to delete collection %s", name)
	return resp
}

// ListCollections lists all collections
func (mc *ManagerClient) ListCollections() []*manager_pb.Collection {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	stream, err := mc.ManagerServiceClient.ListCollections(ctx, &manager_pb.ListCollectionsRequest{})
	require.NoError(mc.t, err, "failed to start listing collections")

	var collections []*manager_pb.Collection
	for {
		collection, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(mc.t, err, "failed to receive collection")
		collections = append(collections, collection)
	}

	return collections
}

// WatchTopology starts watching topology and returns the first event (FULL_SYNC).
// This is useful for tests that just need to get the current topology state.
func (mc *ManagerClient) WatchTopology() *manager_pb.TopologyEvent {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	stream, err := mc.ManagerServiceClient.WatchTopology(ctx, &manager_pb.WatchTopologyRequest{})
	require.NoError(mc.t, err, "failed to start topology watch")

	// Receive the first event (should be FULL_SYNC)
	event, err := stream.Recv()
	require.NoError(mc.t, err, "failed to receive topology event")
	return event
}

// ListClusterServers lists Raft cluster servers
func (mc *ManagerClient) ListClusterServers() *manager_pb.RaftListClusterServersResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := mc.ManagerServiceClient.RaftListClusterServers(ctx, &manager_pb.RaftListClusterServersRequest{})
	require.NoError(mc.t, err, "failed to list cluster servers")
	return resp
}

// GetReplicationTargets gets replication targets for a given file size
func (mc *ManagerClient) GetReplicationTargets(fileSize uint64, numReplicas uint32) *manager_pb.GetReplicationTargetsResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := mc.ManagerServiceClient.GetReplicationTargets(ctx, &manager_pb.GetReplicationTargetsRequest{
		FileSize:    fileSize,
		NumReplicas: numReplicas,
	})
	require.NoError(mc.t, err, "failed to get replication targets")
	return resp
}
