package manager

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewLeaderForwarder(t *testing.T) {
	t.Parallel()

	lf := NewLeaderForwarder()
	require.NotNil(t, lf)

	assert.Equal(t, 5*time.Second, lf.dialTimeout)
	assert.Nil(t, lf.conn)
	assert.Nil(t, lf.client)
	assert.Empty(t, lf.leaderAddr)
}

func TestLeaderForwarder_Close(t *testing.T) {
	t.Parallel()

	lf := NewLeaderForwarder()
	require.NotNil(t, lf)

	// Close on empty forwarder should not panic
	lf.Close()

	assert.Nil(t, lf.conn)
	assert.Nil(t, lf.client)
	assert.Empty(t, lf.leaderAddr)
}

func TestLeaderForwarder_ConnectionFailure(t *testing.T) {
	t.Parallel()

	lf := NewLeaderForwarder()
	lf.dialTimeout = 100 * time.Millisecond // Short timeout for faster test
	defer lf.Close()

	ctx := context.Background()

	// Try to forward to an invalid address
	req := &manager_pb.CreateCollectionRequest{
		Name: "test-collection",
	}

	resp, err := lf.ForwardCreateCollection(ctx, "localhost:99999", req)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "failed to connect to leader")
}

func TestLeaderForwarder_Timeout(t *testing.T) {
	t.Parallel()

	lf := NewLeaderForwarder()
	lf.dialTimeout = 50 * time.Millisecond // Very short timeout
	defer lf.Close()

	ctx := context.Background()

	// Try to connect to a non-responding address (use a valid IP but invalid port)
	req := &manager_pb.RegisterServiceRequest{
		ServiceType: manager_pb.ServiceType_FILE_SERVICE,
	}

	_, err := lf.ForwardRegisterService(ctx, "10.255.255.1:9999", req)
	assert.Error(t, err)
}

func TestLeaderForwarder_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	lf := NewLeaderForwarder()
	lf.dialTimeout = 100 * time.Millisecond
	defer lf.Close()

	ctx := context.Background()
	var wg sync.WaitGroup

	// Concurrent access should not cause race conditions
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := &manager_pb.CreateCollectionRequest{
				Name: "test",
			}
			// This will fail but shouldn't panic or race
			lf.ForwardCreateCollection(ctx, "localhost:99999", req)
		}()
	}

	wg.Wait()
}

// testManagerServer implements a minimal ManagerServiceServer for testing
type testManagerServer struct {
	manager_pb.UnimplementedManagerServiceServer

	createCollectionFn  func(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error)
	deleteCollectionFn  func(ctx context.Context, req *manager_pb.DeleteCollectionRequest) (*manager_pb.DeleteCollectionResponse, error)
	registerServiceFn   func(ctx context.Context, req *manager_pb.RegisterServiceRequest) (*manager_pb.RegisterServiceResponse, error)
	unregisterServiceFn func(ctx context.Context, req *manager_pb.UnregisterServiceRequest) (*manager_pb.UnregisterServiceResponse, error)
	raftAddServerFn     func(ctx context.Context, req *manager_pb.RaftAddServerRequest) (*manager_pb.RaftAddServerResponse, error)
	raftRemoveServerFn  func(ctx context.Context, req *manager_pb.RaftRemoveServerRequest) (*manager_pb.RaftRemoveServerResponse, error)
}

func (s *testManagerServer) CreateCollection(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
	if s.createCollectionFn != nil {
		return s.createCollectionFn(ctx, req)
	}
	return &manager_pb.CreateCollectionResponse{Success: true}, nil
}

func (s *testManagerServer) DeleteCollection(ctx context.Context, req *manager_pb.DeleteCollectionRequest) (*manager_pb.DeleteCollectionResponse, error) {
	if s.deleteCollectionFn != nil {
		return s.deleteCollectionFn(ctx, req)
	}
	return &manager_pb.DeleteCollectionResponse{Success: true}, nil
}

func (s *testManagerServer) RegisterService(ctx context.Context, req *manager_pb.RegisterServiceRequest) (*manager_pb.RegisterServiceResponse, error) {
	if s.registerServiceFn != nil {
		return s.registerServiceFn(ctx, req)
	}
	return &manager_pb.RegisterServiceResponse{Success: true}, nil
}

func (s *testManagerServer) UnregisterService(ctx context.Context, req *manager_pb.UnregisterServiceRequest) (*manager_pb.UnregisterServiceResponse, error) {
	if s.unregisterServiceFn != nil {
		return s.unregisterServiceFn(ctx, req)
	}
	return &manager_pb.UnregisterServiceResponse{Success: true}, nil
}

func (s *testManagerServer) RaftAddServer(ctx context.Context, req *manager_pb.RaftAddServerRequest) (*manager_pb.RaftAddServerResponse, error) {
	if s.raftAddServerFn != nil {
		return s.raftAddServerFn(ctx, req)
	}
	return &manager_pb.RaftAddServerResponse{Success: true}, nil
}

func (s *testManagerServer) RaftRemoveServer(ctx context.Context, req *manager_pb.RaftRemoveServerRequest) (*manager_pb.RaftRemoveServerResponse, error) {
	if s.raftRemoveServerFn != nil {
		return s.raftRemoveServerFn(ctx, req)
	}
	return &manager_pb.RaftRemoveServerResponse{Success: true}, nil
}

// startTestGRPCServer starts a gRPC server for testing and returns the address
func startTestGRPCServer(t *testing.T, server *testManagerServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	s := grpc.NewServer()
	manager_pb.RegisterManagerServiceServer(s, server)

	go func() {
		if err := s.Serve(lis); err != nil {
			// Server stopped, this is expected during cleanup
		}
	}()

	t.Cleanup(func() {
		s.Stop()
	})

	return lis.Addr().String()
}

func TestLeaderForwarder_ForwardCreateCollection_Success(t *testing.T) {
	t.Parallel()

	server := &testManagerServer{
		createCollectionFn: func(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
			return &manager_pb.CreateCollectionResponse{
				Success: true,
			}, nil
		},
	}

	addr := startTestGRPCServer(t, server)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx := context.Background()
	req := &manager_pb.CreateCollectionRequest{
		Name: "test-collection",
	}

	resp, err := lf.ForwardCreateCollection(ctx, addr, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)
}

func TestLeaderForwarder_ForwardDeleteCollection_Success(t *testing.T) {
	t.Parallel()

	server := &testManagerServer{
		deleteCollectionFn: func(ctx context.Context, req *manager_pb.DeleteCollectionRequest) (*manager_pb.DeleteCollectionResponse, error) {
			return &manager_pb.DeleteCollectionResponse{
				Success: true,
			}, nil
		},
	}

	addr := startTestGRPCServer(t, server)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx := context.Background()
	req := &manager_pb.DeleteCollectionRequest{
		Name: "test-collection",
	}

	resp, err := lf.ForwardDeleteCollection(ctx, addr, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)
}

func TestLeaderForwarder_ForwardRegisterService_Success(t *testing.T) {
	t.Parallel()

	server := &testManagerServer{
		registerServiceFn: func(ctx context.Context, req *manager_pb.RegisterServiceRequest) (*manager_pb.RegisterServiceResponse, error) {
			return &manager_pb.RegisterServiceResponse{
				Success: true,
			}, nil
		},
	}

	addr := startTestGRPCServer(t, server)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx := context.Background()
	req := &manager_pb.RegisterServiceRequest{
		ServiceType: manager_pb.ServiceType_FILE_SERVICE,
	}

	resp, err := lf.ForwardRegisterService(ctx, addr, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)
}

func TestLeaderForwarder_ForwardUnregisterService_Success(t *testing.T) {
	t.Parallel()

	server := &testManagerServer{}

	addr := startTestGRPCServer(t, server)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx := context.Background()
	req := &manager_pb.UnregisterServiceRequest{
		ServiceType: manager_pb.ServiceType_FILE_SERVICE,
	}

	resp, err := lf.ForwardUnregisterService(ctx, addr, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)
}

func TestLeaderForwarder_ForwardRaftAddServer_Success(t *testing.T) {
	t.Parallel()

	server := &testManagerServer{}

	addr := startTestGRPCServer(t, server)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx := context.Background()
	req := &manager_pb.RaftAddServerRequest{
		Id:      "node-2",
		Address: "localhost:9002",
	}

	resp, err := lf.ForwardRaftAddServer(ctx, addr, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)
}

func TestLeaderForwarder_ForwardRaftRemoveServer_Success(t *testing.T) {
	t.Parallel()

	server := &testManagerServer{}

	addr := startTestGRPCServer(t, server)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx := context.Background()
	req := &manager_pb.RaftRemoveServerRequest{
		Id: "node-2",
	}

	resp, err := lf.ForwardRaftRemoveServer(ctx, addr, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)
}

func TestLeaderForwarder_CacheConnection(t *testing.T) {
	t.Parallel()

	callCount := 0
	server := &testManagerServer{
		createCollectionFn: func(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
			callCount++
			return &manager_pb.CreateCollectionResponse{Success: true}, nil
		},
	}

	addr := startTestGRPCServer(t, server)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx := context.Background()
	req := &manager_pb.CreateCollectionRequest{
		Name: "test",
	}

	// Multiple calls to the same address should reuse the connection
	for i := 0; i < 5; i++ {
		resp, err := lf.ForwardCreateCollection(ctx, addr, req)
		require.NoError(t, err)
		assert.True(t, resp.Success)
	}

	// All calls should have succeeded
	assert.Equal(t, 5, callCount)

	// Connection should be cached
	lf.mu.RLock()
	assert.Equal(t, addr, lf.leaderAddr)
	assert.NotNil(t, lf.conn)
	assert.NotNil(t, lf.client)
	lf.mu.RUnlock()
}

func TestLeaderForwarder_LeaderChange_RefreshConnection(t *testing.T) {
	t.Parallel()

	// Start two test servers
	server1CallCount := 0
	server1 := &testManagerServer{
		createCollectionFn: func(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
			server1CallCount++
			return &manager_pb.CreateCollectionResponse{Success: true}, nil
		},
	}
	addr1 := startTestGRPCServer(t, server1)

	server2CallCount := 0
	server2 := &testManagerServer{
		createCollectionFn: func(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
			server2CallCount++
			return &manager_pb.CreateCollectionResponse{Success: true}, nil
		},
	}
	addr2 := startTestGRPCServer(t, server2)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx := context.Background()
	req := &manager_pb.CreateCollectionRequest{Name: "test"}

	// Forward to first leader
	_, err := lf.ForwardCreateCollection(ctx, addr1, req)
	require.NoError(t, err)
	assert.Equal(t, 1, server1CallCount)
	assert.Equal(t, 0, server2CallCount)

	// Leader changes - forward to second leader
	_, err = lf.ForwardCreateCollection(ctx, addr2, req)
	require.NoError(t, err)
	assert.Equal(t, 1, server1CallCount)
	assert.Equal(t, 1, server2CallCount)

	// Verify connection is now to second leader
	lf.mu.RLock()
	assert.Equal(t, addr2, lf.leaderAddr)
	lf.mu.RUnlock()
}

func TestLeaderForwarder_RPCError(t *testing.T) {
	t.Parallel()

	server := &testManagerServer{
		createCollectionFn: func(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
			return nil, status.Error(codes.InvalidArgument, "invalid collection ID")
		},
	}

	addr := startTestGRPCServer(t, server)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx := context.Background()
	req := &manager_pb.CreateCollectionRequest{
		Name: "",
	}

	resp, err := lf.ForwardCreateCollection(ctx, addr, req)
	assert.Error(t, err)
	assert.Nil(t, resp)

	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "invalid collection ID")
}

func TestLeaderForwarder_ContextCancellation(t *testing.T) {
	t.Parallel()

	// Create a server that delays response
	server := &testManagerServer{
		createCollectionFn: func(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
				return &manager_pb.CreateCollectionResponse{Success: true}, nil
			}
		},
	}

	addr := startTestGRPCServer(t, server)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	req := &manager_pb.CreateCollectionRequest{
		Name: "test",
	}

	_, err := lf.ForwardCreateCollection(ctx, addr, req)
	assert.Error(t, err)
}

func TestLeaderForwarder_DoubleCheckedLocking_Race(t *testing.T) {
	t.Parallel()

	server := &testManagerServer{}
	addr := startTestGRPCServer(t, server)

	lf := NewLeaderForwarder()
	defer lf.Close()

	ctx := context.Background()
	req := &manager_pb.CreateCollectionRequest{Name: "test"}

	// Simulate concurrent first-time connections
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = lf.ForwardCreateCollection(ctx, addr, req)
		}()
	}

	wg.Wait()

	// Should have created exactly one connection
	lf.mu.RLock()
	assert.NotNil(t, lf.conn)
	assert.Equal(t, addr, lf.leaderAddr)
	lf.mu.RUnlock()
}
