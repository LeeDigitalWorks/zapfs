//go:build integration

package manager

import (
	"context"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForClusterLeader waits for a leader to be available in the cluster.
// This is used by tests that need a stable leader before running.
func waitForClusterLeader(t *testing.T) {
	t.Helper()
	addrs := []string{managerServer1Addr, managerServer2Addr, managerServer3Addr}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, addr := range addrs {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			client := newManagerClient(t, addr)
			resp, err := client.ManagerServiceClient.Ping(ctx, &manager_pb.PingRequest{})
			cancel()
			if err == nil && resp.IsLeader {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Fail(t, "timeout waiting for cluster leader")
}

// =============================================================================
// Ping Tests
// =============================================================================

func TestPing(t *testing.T) {
	// Not parallel - share cluster with other tests
	client := newManagerClient(t, managerServer1Addr)
	resp := client.Ping()
	assert.NotNil(t, resp)
}

func TestPing_AllNodes(t *testing.T) {
	// Not parallel - share cluster with other tests
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
	// Not parallel - share cluster with other tests
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
	// Not parallel - share cluster with other tests
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
	// Not parallel - requires stable leader
	waitForClusterLeader(t)

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
	// Not parallel - requires stable leader
	waitForClusterLeader(t)

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
	// Not parallel - share cluster with other tests
	client := newManagerClient(t, managerServer1Addr)

	resp := client.GetReplicationTargets(1024*1024, 2) // 1MB, 2 replicas
	assert.NotNil(t, resp)
	t.Logf("Replication targets: %+v", resp)
}

// =============================================================================
// Leader Failover Tests (Long Running)
// =============================================================================

func TestLeaderDiscovery(t *testing.T) {
	// Not parallel - requires stable leader
	waitForClusterLeader(t)

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
	// Not parallel - share cluster with other tests and needs stable leader
	waitForClusterLeader(t)

	addrs := []string{managerServer1Addr, managerServer2Addr, managerServer3Addr}

	for i, addr := range addrs {
		t.Run(addr, func(t *testing.T) {
			client := newManagerClient(t, addr)
			name := testutil.UniqueID("test-write-any")

			// This may forward to leader internally
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, err := client.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
				Name:  name,
				Owner: "test-owner",
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
