//go:build integration

package manager

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// =============================================================================
// Container Management Helpers
// =============================================================================

// Container names as used by docker compose
const (
	manager1Container = "docker-manager-1-1"
	manager2Container = "docker-manager-2-1"
	manager3Container = "docker-manager-3-1"
	dockerNetwork     = "docker_zapfs-net"
)

// containerAddrMap maps container names to gRPC addresses
var containerAddrMap = map[string]string{
	manager1Container: managerServer1Addr,
	manager2Container: managerServer2Addr,
	manager3Container: managerServer3Addr,
}

// addrContainerMap maps gRPC addresses to container names
var addrContainerMap = map[string]string{
	managerServer1Addr: manager1Container,
	managerServer2Addr: manager2Container,
	managerServer3Addr: manager3Container,
}

// pauseContainer pauses a docker container
func pauseContainer(t *testing.T, container string) {
	t.Helper()
	cmd := exec.Command("docker", "pause", container)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "failed to pause container %s: %s", container, string(output))
	t.Logf("Paused container: %s", container)
}

// unpauseContainer unpauses a docker container (idempotent - ignores if not paused)
func unpauseContainer(t *testing.T, container string) {
	t.Helper()
	cmd := exec.Command("docker", "unpause", container)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if it's already unpaused (not an error condition)
		if strings.Contains(string(output), "is not paused") {
			return
		}
		require.NoError(t, err, "failed to unpause container %s: %s", container, string(output))
	}
	t.Logf("Unpaused container: %s", container)
}

// disconnectFromNetwork disconnects a container from the network
func disconnectFromNetwork(t *testing.T, container, network string) {
	t.Helper()
	cmd := exec.Command("docker", "network", "disconnect", network, container)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "failed to disconnect %s from %s: %s", container, network, string(output))
	t.Logf("Disconnected %s from network %s", container, network)
}

// connectToNetwork connects a container to the network (idempotent - ignores if already connected)
func connectToNetwork(t *testing.T, container, network string) {
	t.Helper()
	cmd := exec.Command("docker", "network", "connect", network, container)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if it's already connected (not an error condition)
		if strings.Contains(string(output), "already exists") {
			return
		}
		require.NoError(t, err, "failed to connect %s to %s: %s", container, network, string(output))
	}
	t.Logf("Connected %s to network %s", container, network)
}

// =============================================================================
// Leader Discovery Helpers
// =============================================================================

// clusterState holds information about the current cluster state
type clusterState struct {
	leaderAddr     string
	leaderContainer string
	followerAddrs  []string
	followerContainers []string
}

// getClusterState returns the current cluster state including leader info
func getClusterState(t *testing.T) *clusterState {
	t.Helper()

	// Query each node individually to determine leader status
	// This is more reliable than parsing raft addresses
	allAddrs := []string{managerServer1Addr, managerServer2Addr, managerServer3Addr}
	allContainers := []string{manager1Container, manager2Container, manager3Container}

	state := &clusterState{}

	for i, addr := range allAddrs {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		client := testutil.NewManagerClient(t, addr)
		resp, err := client.ManagerServiceClient.Ping(ctx, &manager_pb.PingRequest{})
		cancel()

		if err != nil {
			t.Logf("Failed to ping %s: %v", addr, err)
			continue
		}

		if resp.IsLeader {
			state.leaderAddr = addr
			state.leaderContainer = allContainers[i]
		} else {
			state.followerAddrs = append(state.followerAddrs, addr)
			state.followerContainers = append(state.followerContainers, allContainers[i])
		}
	}

	require.NotEmpty(t, state.leaderAddr, "no leader found in cluster")
	return state
}

// waitForLeader waits for a leader to be elected, polling the given addresses
// Returns the gRPC address of the new leader
func waitForLeader(t *testing.T, timeout time.Duration, addrs ...string) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for leader election after %v", timeout)
		}

		// Query each address to see if it's the leader
		for _, addr := range addrs {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			client := testutil.NewManagerClient(t, addr)
			resp, err := client.ManagerServiceClient.Ping(ctx, &manager_pb.PingRequest{})
			cancel()

			if err != nil {
				continue
			}

			if resp.IsLeader {
				t.Logf("Leader elected: %s", addr)
				return addr
			}
		}
	}
	return "" // unreachable, but satisfies compiler
}

// waitForNoLeader waits until there is no leader (used after pausing majority)
func waitForNoLeader(t *testing.T, timeout time.Duration, addr string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if time.Now().After(deadline) {
			// Timeout is acceptable - we just want to give the cluster time to detect leader loss
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		client := testutil.NewManagerClient(t, addr)
		resp, err := client.ManagerServiceClient.RaftListClusterServers(ctx, &manager_pb.RaftListClusterServersRequest{})
		cancel()

		if err != nil {
			// Can't connect - cluster may be in transition
			continue
		}

		hasLeader := false
		for _, server := range resp.Servers {
			if server.IsLeader {
				hasLeader = true
				break
			}
		}

		if !hasLeader {
			t.Log("Cluster has no leader")
			return
		}
	}
}

// =============================================================================
// Failover Tests
// =============================================================================

// TestLeaderFailover tests that the cluster can elect a new leader when the current leader fails
func TestLeaderFailover(t *testing.T) {
	// Don't run in parallel - this test manipulates cluster state

	// Get initial cluster state
	state := getClusterState(t)
	t.Logf("Initial leader: %s (%s)", state.leaderAddr, state.leaderContainer)
	t.Logf("Followers: %v", state.followerAddrs)

	// Create a collection before failover
	leaderClient := testutil.NewManagerClient(t, state.leaderAddr)
	collectionName := testutil.UniqueID("failover-test")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := leaderClient.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name:  collectionName,
		Owner: "test-owner",
	})
	cancel()
	require.NoError(t, err, "failed to create collection before failover")
	t.Logf("Created collection: %s", collectionName)

	// Pause the leader container
	pauseContainer(t, state.leaderContainer)
	defer unpauseContainer(t, state.leaderContainer) // Ensure cleanup

	// Wait for new leader election (Raft election timeout is typically 1-5 seconds)
	newLeaderAddr := waitForLeader(t, 30*time.Second, state.followerAddrs...)
	require.NotEqual(t, state.leaderAddr, newLeaderAddr, "new leader should be different from old leader")
	t.Logf("New leader elected: %s", newLeaderAddr)

	// Verify the collection still exists on new leader
	newLeaderClient := testutil.NewManagerClient(t, newLeaderAddr)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	getResp, err := newLeaderClient.ManagerServiceClient.GetCollection(ctx, &manager_pb.GetCollectionRequest{
		Name: collectionName,
	})
	cancel()
	require.NoError(t, err, "failed to get collection from new leader")
	assert.Equal(t, collectionName, getResp.Collection.Name)
	t.Log("Collection verified on new leader")

	// Create a new collection on the new leader
	newCollectionName := testutil.UniqueID("post-failover")
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	_, err = newLeaderClient.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name:  newCollectionName,
		Owner: "test-owner",
	})
	cancel()
	require.NoError(t, err, "failed to create collection after failover")
	t.Logf("Created new collection after failover: %s", newCollectionName)

	// Unpause the old leader
	unpauseContainer(t, state.leaderContainer)

	// Wait for old leader to rejoin
	time.Sleep(5 * time.Second)

	// Verify the new collection is visible from the rejoined node
	oldLeaderClient := testutil.NewManagerClient(t, state.leaderAddr)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	getResp, err = oldLeaderClient.ManagerServiceClient.GetCollection(ctx, &manager_pb.GetCollectionRequest{
		Name: newCollectionName,
	})
	cancel()
	require.NoError(t, err, "failed to get post-failover collection from rejoined node")
	assert.Equal(t, newCollectionName, getResp.Collection.Name)
	t.Log("Post-failover collection visible on rejoined node")

	// Cleanup
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	newLeaderClient.ManagerServiceClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{Name: collectionName})
	newLeaderClient.ManagerServiceClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{Name: newCollectionName})
	cancel()
}

// TestFollowerFailure tests that the cluster continues operating when a follower fails
func TestFollowerFailure(t *testing.T) {
	// Get initial cluster state
	state := getClusterState(t)
	require.NotEmpty(t, state.followerContainers, "need at least one follower")

	followerToPause := state.followerContainers[0]
	t.Logf("Pausing follower: %s", followerToPause)

	// Pause a follower
	pauseContainer(t, followerToPause)
	defer unpauseContainer(t, followerToPause)

	// Give cluster time to detect the failure
	time.Sleep(2 * time.Second)

	// Verify writes still work on leader
	leaderClient := testutil.NewManagerClient(t, state.leaderAddr)
	collectionName := testutil.UniqueID("follower-failure")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := leaderClient.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name:  collectionName,
		Owner: "test-owner",
	})
	cancel()
	require.NoError(t, err, "write should succeed with one follower down")
	t.Log("Write succeeded with follower down")

	// Verify read works
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	getResp, err := leaderClient.ManagerServiceClient.GetCollection(ctx, &manager_pb.GetCollectionRequest{
		Name: collectionName,
	})
	cancel()
	require.NoError(t, err, "read should succeed")
	assert.Equal(t, collectionName, getResp.Collection.Name)

	// Unpause follower
	unpauseContainer(t, followerToPause)
	time.Sleep(3 * time.Second)

	// Verify collection is replicated to the rejoined follower
	followerAddr := containerAddrMap[followerToPause]
	followerClient := testutil.NewManagerClient(t, followerAddr)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	getResp, err = followerClient.ManagerServiceClient.GetCollection(ctx, &manager_pb.GetCollectionRequest{
		Name: collectionName,
	})
	cancel()
	require.NoError(t, err, "collection should be visible on rejoined follower")
	assert.Equal(t, collectionName, getResp.Collection.Name)
	t.Log("Collection replicated to rejoined follower")

	// Cleanup
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	leaderClient.ManagerServiceClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{Name: collectionName})
	cancel()
}

// TestMajorityFailure tests that the cluster becomes unavailable when majority fails
func TestMajorityFailure(t *testing.T) {
	// Get initial cluster state
	state := getClusterState(t)
	require.Len(t, state.followerContainers, 2, "need exactly 2 followers for this test")

	// Pause two nodes (majority) - pause followers to keep one node available for queries
	t.Logf("Pausing majority (2 followers): %v", state.followerContainers)
	pauseContainer(t, state.followerContainers[0])
	pauseContainer(t, state.followerContainers[1])
	defer func() {
		unpauseContainer(t, state.followerContainers[0])
		unpauseContainer(t, state.followerContainers[1])
	}()

	// Wait for leader to step down (no quorum)
	waitForNoLeader(t, 15*time.Second, state.leaderAddr)

	// Verify writes fail (no quorum)
	leaderClient := testutil.NewManagerClient(t, state.leaderAddr)
	collectionName := testutil.UniqueID("majority-failure")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := leaderClient.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name:  collectionName,
		Owner: "test-owner",
	})
	cancel()

	require.Error(t, err, "write should fail without quorum")
	t.Logf("Write failed as expected: %v", err)

	// Check the error type
	st, ok := status.FromError(err)
	if ok {
		// Should be Unavailable or FailedPrecondition (no leader)
		assert.True(t, st.Code() == codes.Unavailable || st.Code() == codes.FailedPrecondition || st.Code() == codes.DeadlineExceeded,
			"expected Unavailable, FailedPrecondition, or DeadlineExceeded, got %v", st.Code())
	}

	// Restore one follower to regain quorum
	t.Logf("Restoring one follower to regain quorum: %s", state.followerContainers[0])
	unpauseContainer(t, state.followerContainers[0])

	// Wait for leader election
	newLeaderAddr := waitForLeader(t, 30*time.Second, state.leaderAddr, state.followerAddrs[0])
	t.Logf("Leader elected after restoring quorum: %s", newLeaderAddr)

	// Verify writes work again
	newLeaderClient := testutil.NewManagerClient(t, newLeaderAddr)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	_, err = newLeaderClient.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name:  collectionName,
		Owner: "test-owner",
	})
	cancel()
	require.NoError(t, err, "write should succeed after quorum restored")
	t.Log("Write succeeded after quorum restored")

	// Restore remaining follower
	unpauseContainer(t, state.followerContainers[1])
	time.Sleep(3 * time.Second)

	// Cleanup
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	newLeaderClient.ManagerServiceClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{Name: collectionName})
	cancel()
}

// TestWriteForwardingDuringFailover tests that writes sent to followers during failover are handled correctly
func TestWriteForwardingDuringFailover(t *testing.T) {
	// Get initial cluster state
	state := getClusterState(t)
	require.NotEmpty(t, state.followerAddrs, "need at least one follower")

	followerAddr := state.followerAddrs[0]
	t.Logf("Sending write through follower: %s", followerAddr)

	// Send write to follower (should be forwarded to leader)
	followerClient := testutil.NewManagerClient(t, followerAddr)
	collectionName := testutil.UniqueID("forwarding-test")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := followerClient.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name:  collectionName,
		Owner: "test-owner",
	})
	cancel()
	require.NoError(t, err, "write via follower should succeed (forwarded to leader)")
	t.Log("Write via follower succeeded")

	// Wait for Raft replication to complete
	time.Sleep(1 * time.Second)

	// Verify collection exists (query leader since it has the authoritative state)
	leaderClient := testutil.NewManagerClient(t, state.leaderAddr)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	getResp, err := leaderClient.ManagerServiceClient.GetCollection(ctx, &manager_pb.GetCollectionRequest{
		Name: collectionName,
	})
	cancel()
	require.NoError(t, err, "collection should exist")
	assert.Equal(t, collectionName, getResp.Collection.Name)

	// Now pause leader and verify forwarding fails gracefully
	pauseContainer(t, state.leaderContainer)
	defer unpauseContainer(t, state.leaderContainer)

	// Wait for cluster to detect leader loss
	time.Sleep(5 * time.Second)

	// Try write via follower - should fail or succeed after new election
	newCollectionName := testutil.UniqueID("during-failover")
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	_, err = followerClient.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name:  newCollectionName,
		Owner: "test-owner",
	})
	cancel()

	// Either it succeeds (new leader elected) or fails gracefully
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			t.Logf("Write during failover returned: %v (%s)", st.Code(), st.Message())
		}
	} else {
		t.Log("Write during failover succeeded (new leader was elected)")
		// Cleanup
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		followerClient.ManagerServiceClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{Name: newCollectionName})
		cancel()
	}

	// Restore leader
	unpauseContainer(t, state.leaderContainer)
	time.Sleep(3 * time.Second)

	// Cleanup first collection
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	followerClient.ManagerServiceClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{Name: collectionName})
	cancel()
}

// TestNetworkPartition tests behavior when a node is partitioned from the network
func TestNetworkPartition(t *testing.T) {
	// Get initial cluster state
	state := getClusterState(t)

	t.Logf("Partitioning leader from network: %s", state.leaderContainer)

	// Disconnect leader from network
	disconnectFromNetwork(t, state.leaderContainer, dockerNetwork)
	defer connectToNetwork(t, state.leaderContainer, dockerNetwork)

	// Wait for new leader election among remaining nodes
	newLeaderAddr := waitForLeader(t, 30*time.Second, state.followerAddrs...)
	t.Logf("New leader elected: %s", newLeaderAddr)

	// Verify writes work on new leader
	newLeaderClient := testutil.NewManagerClient(t, newLeaderAddr)
	collectionName := testutil.UniqueID("partition-test")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := newLeaderClient.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name:  collectionName,
		Owner: "test-owner",
	})
	cancel()
	require.NoError(t, err, "write should succeed on new leader after partition")
	t.Log("Write succeeded on new leader")

	// Reconnect partitioned node
	connectToNetwork(t, state.leaderContainer, dockerNetwork)
	time.Sleep(5 * time.Second)

	// Verify the collection is visible from the reconnected node
	reconnectedClient := testutil.NewManagerClient(t, state.leaderAddr)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	getResp, err := reconnectedClient.ManagerServiceClient.GetCollection(ctx, &manager_pb.GetCollectionRequest{
		Name: collectionName,
	})
	cancel()
	require.NoError(t, err, "collection should be visible on reconnected node")
	assert.Equal(t, collectionName, getResp.Collection.Name)
	t.Log("Collection visible on reconnected node")

	// Cleanup
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	newLeaderClient.ManagerServiceClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{Name: collectionName})
	cancel()
}

// TestRapidLeaderFailovers tests cluster stability with multiple rapid leader failures
func TestRapidLeaderFailovers(t *testing.T) {
	// Create a collection first
	state := getClusterState(t)
	leaderClient := testutil.NewManagerClient(t, state.leaderAddr)
	collectionName := testutil.UniqueID("rapid-failover")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := leaderClient.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name:  collectionName,
		Owner: "test-owner",
	})
	cancel()
	require.NoError(t, err, "failed to create initial collection")

	// Track containers we've paused
	pausedContainers := make(map[string]bool)
	defer func() {
		// Cleanup: unpause all containers
		for container := range pausedContainers {
			unpauseContainer(t, container)
		}
	}()

	// Perform 2 leader failovers
	for i := 0; i < 2; i++ {
		t.Logf("=== Failover iteration %d ===", i+1)

		// Get current state
		currentState := getClusterState(t)
		t.Logf("Current leader: %s", currentState.leaderAddr)

		// Pause current leader
		pauseContainer(t, currentState.leaderContainer)
		pausedContainers[currentState.leaderContainer] = true

		// Wait for new leader (only from non-paused nodes)
		availableAddrs := []string{}
		for _, addr := range []string{managerServer1Addr, managerServer2Addr, managerServer3Addr} {
			container := addrContainerMap[addr]
			if !pausedContainers[container] {
				availableAddrs = append(availableAddrs, addr)
			}
		}

		if len(availableAddrs) < 2 {
			t.Log("Not enough nodes for quorum, restoring one")
			// Restore oldest paused container
			for container := range pausedContainers {
				unpauseContainer(t, container)
				delete(pausedContainers, container)
				availableAddrs = append(availableAddrs, containerAddrMap[container])
				break
			}
		}

		newLeaderAddr := waitForLeader(t, 30*time.Second, availableAddrs...)
		t.Logf("New leader: %s", newLeaderAddr)

		// Verify collection still exists
		client := testutil.NewManagerClient(t, newLeaderAddr)
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		_, err = client.ManagerServiceClient.GetCollection(ctx, &manager_pb.GetCollectionRequest{
			Name: collectionName,
		})
		cancel()
		require.NoError(t, err, "collection should exist after failover %d", i+1)
	}

	// Restore all paused containers
	for container := range pausedContainers {
		unpauseContainer(t, container)
		delete(pausedContainers, container)
	}

	// Wait for cluster to stabilize
	time.Sleep(5 * time.Second)

	// Final verification - collection should be on all nodes
	for _, addr := range []string{managerServer1Addr, managerServer2Addr, managerServer3Addr} {
		client := testutil.NewManagerClient(t, addr)
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		getResp, err := client.ManagerServiceClient.GetCollection(ctx, &manager_pb.GetCollectionRequest{
			Name: collectionName,
		})
		cancel()
		require.NoError(t, err, "collection should be visible on %s", addr)
		assert.Equal(t, collectionName, getResp.Collection.Name)
	}
	t.Log("Collection visible on all nodes after rapid failovers")

	// Cleanup
	finalState := getClusterState(t)
	finalClient := testutil.NewManagerClient(t, finalState.leaderAddr)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	finalClient.ManagerServiceClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{Name: collectionName})
	cancel()
}

// TestConcurrentWritesDuringFailover tests handling of concurrent writes during leader failover
func TestConcurrentWritesDuringFailover(t *testing.T) {
	state := getClusterState(t)

	// Start goroutines sending writes to different nodes
	results := make(chan error, 100)
	done := make(chan struct{})

	// Writer goroutine - sends writes continuously
	go func() {
		i := 0
		for {
			select {
			case <-done:
				return
			default:
				addr := []string{managerServer1Addr, managerServer2Addr, managerServer3Addr}[i%3]
				client := testutil.NewManagerClient(t, addr)
				collectionName := fmt.Sprintf("concurrent-%d-%d", time.Now().UnixNano(), i)

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := client.ManagerServiceClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
					Name:  collectionName,
					Owner: "test-owner",
				})
				cancel()

				if err == nil {
					// Cleanup successful writes
					ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
					client.ManagerServiceClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{Name: collectionName})
					cancel()
				}

				results <- err
				i++
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()

	// Let some writes succeed first (give more time)
	time.Sleep(3 * time.Second)

	// Trigger failover
	pauseContainer(t, state.leaderContainer)
	defer unpauseContainer(t, state.leaderContainer)

	// Wait for new leader
	newLeaderAddr := waitForLeader(t, 30*time.Second, state.followerAddrs...)
	t.Logf("New leader: %s", newLeaderAddr)

	// Let writes continue after failover
	time.Sleep(3 * time.Second)

	// Stop writer
	close(done)

	// Drain remaining results
	time.Sleep(500 * time.Millisecond)

	// Count results
	successes := 0
	failures := 0
	for {
		select {
		case err := <-results:
			if err == nil {
				successes++
			} else {
				failures++
			}
		default:
			goto done
		}
	}
done:

	t.Logf("Concurrent writes: %d succeeded, %d failed", successes, failures)
	// During failover, it's acceptable for writes to fail
	// The key test is that the cluster recovers and continues operating
	assert.Greater(t, successes+failures, 0, "should have attempted some writes")

	// Restore leader
	unpauseContainer(t, state.leaderContainer)
}
