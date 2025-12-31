//go:build integration

package manager

import (
	"context"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Ping Tests
// =============================================================================

func TestPing(t *testing.T) {
	t.Parallel()

	client := newManagerClient(t, managerServer1Addr)
	resp := client.Ping()
	assert.NotNil(t, resp)
}

func TestPing_AllNodes(t *testing.T) {
	t.Parallel()

	addrs := []string{managerServer1Addr, managerServer2Addr, managerServer3Addr}

	for _, addr := range addrs {
		t.Run(addr, func(t *testing.T) {
			client := newManagerClient(t, addr)
			resp := client.Ping()
			assert.NotNil(t, resp, "ping to %s should succeed", addr)
		})
	}
}

// =============================================================================
// Cluster Status Tests
// =============================================================================

func TestListClusterServers(t *testing.T) {
	t.Parallel()

	client := newManagerClient(t, managerServer1Addr)
	resp := client.ListClusterServers()

	assert.NotNil(t, resp)
	assert.NotEmpty(t, resp.Servers, "should have at least one cluster server")

	t.Logf("Cluster has %d servers", len(resp.Servers))
	for _, server := range resp.Servers {
		t.Logf("  - %s (leader: %v)", server.Address, server.IsLeader)
	}
}

func TestWatchTopology(t *testing.T) {
	t.Parallel()

	client := newManagerClient(t, managerServer1Addr)
	event := client.WatchTopology()

	assert.NotNil(t, event)
	assert.Equal(t, manager_pb.TopologyEvent_FULL_SYNC, event.Type)
	t.Logf("Topology event: type=%s version=%d services=%d", event.Type, event.Version, len(event.Services))
}

// =============================================================================
// Collection Tests (via Manager)
// =============================================================================

func TestCreateCollection(t *testing.T) {
	t.Parallel()

	client := newManagerClient(t, managerServer1Addr)
	name := testutil.UniqueID("test-mgr-collection")

	resp := client.CreateCollection(name)
	assert.NotNil(t, resp)

	// Verify we can get the collection
	getResp := client.GetCollection(name)
	assert.NotNil(t, getResp)
	assert.Equal(t, name, getResp.Collection.Name)

	// Cleanup
	client.DeleteCollection(name)
}

func TestListCollections(t *testing.T) {
	t.Parallel()

	client := newManagerClient(t, managerServer1Addr)

	// Create some collections
	names := make([]string, 3)
	for i := 0; i < 3; i++ {
		names[i] = testutil.UniqueID("test-mgr-list")
		client.CreateCollection(names[i])
	}

	// List
	collections := client.ListCollections()
	assert.GreaterOrEqual(t, len(collections), 3, "should have at least 3 collections")

	// Cleanup
	for _, name := range names {
		client.DeleteCollection(name)
	}
}

// =============================================================================
// Replication Target Tests
// =============================================================================

func TestGetReplicationTargets(t *testing.T) {
	t.Parallel()

	client := newManagerClient(t, managerServer1Addr)

	resp := client.GetReplicationTargets(1024*1024, 2) // 1MB, 2 replicas
	assert.NotNil(t, resp)
	t.Logf("Replication targets: %+v", resp)
}

// =============================================================================
// Leader Failover Tests (Long Running)
// =============================================================================

func TestLeaderDiscovery(t *testing.T) {
	t.Parallel()

	// Connect to any node and verify we can find the leader
	client := newManagerClient(t, managerServer1Addr)
	resp := client.ListClusterServers()

	leaderFound := false
	for _, server := range resp.Servers {
		if server.IsLeader {
			leaderFound = true
			t.Logf("Leader: %s", server.Address)
			break
		}
	}

	assert.True(t, leaderFound, "should find a leader in the cluster")
}

func TestWriteToAnyNode(t *testing.T) {
	// Test that writes can be routed through any node
	t.Parallel()

	addrs := []string{managerServer1Addr, managerServer2Addr, managerServer3Addr}

	for i, addr := range addrs {
		t.Run(addr, func(t *testing.T) {
			client := newManagerClient(t, addr)
			name := testutil.UniqueID("test-write-any")

			// This may forward to leader internally
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, err := client.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
				Name: name,
			})

			if err != nil {
				t.Logf("Node %d (%s) returned error: %v (may need forwarding)", i, addr, err)
			} else {
				t.Logf("Node %d (%s) handled write successfully", i, addr)
				// Cleanup
				client.DeleteCollection(name)
			}
		})
	}
}
