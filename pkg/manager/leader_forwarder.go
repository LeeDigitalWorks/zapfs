package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	// forwardedRequestsTotal counts requests forwarded to the leader
	forwardedRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "manager_forwarded_requests_total",
			Help: "Number of requests forwarded to leader",
		},
		[]string{"method"},
	)

	// forwardedRequestsDuration tracks latency of forwarded requests
	forwardedRequestsDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "manager_forwarded_requests_duration_seconds",
			Help:    "Duration of forwarded requests",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"method"},
	)

	// forwardedRequestsErrors counts forwarding errors
	forwardedRequestsErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "manager_forwarded_requests_errors_total",
			Help: "Number of forwarding errors",
		},
		[]string{"method", "error_type"},
	)
)

// LeaderForwarder manages gRPC connections to the leader for request forwarding.
// It caches the leader connection and refreshes it when the leader changes.
type LeaderForwarder struct {
	mu         sync.RWMutex
	leaderAddr string
	conn       *grpc.ClientConn
	client     manager_pb.ManagerServiceClient

	dialTimeout time.Duration
}

// NewLeaderForwarder creates a new leader forwarder
func NewLeaderForwarder() *LeaderForwarder {
	return &LeaderForwarder{
		dialTimeout: 5 * time.Second,
	}
}

// getClient returns a client connected to the given leader address.
// If the leader address has changed, it creates a new connection.
func (lf *LeaderForwarder) getClient(leaderAddr string) (manager_pb.ManagerServiceClient, error) {
	lf.mu.RLock()
	if lf.leaderAddr == leaderAddr && lf.client != nil {
		client := lf.client
		lf.mu.RUnlock()
		return client, nil
	}
	lf.mu.RUnlock()

	// Need to create/update connection
	lf.mu.Lock()
	defer lf.mu.Unlock()

	// Double-check after acquiring write lock
	if lf.leaderAddr == leaderAddr && lf.client != nil {
		return lf.client, nil
	}

	// Close old connection if exists
	if lf.conn != nil {
		lf.conn.Close()
		lf.conn = nil
		lf.client = nil
	}

	// Create new connection to leader
	ctx, cancel := context.WithTimeout(context.Background(), lf.dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader %s: %w", leaderAddr, err)
	}

	lf.conn = conn
	lf.client = manager_pb.NewManagerServiceClient(conn)
	lf.leaderAddr = leaderAddr

	logger.Info().
		Str("leader_addr", leaderAddr).
		Msg("Connected to leader for forwarding")

	return lf.client, nil
}

// Close closes any open connections
func (lf *LeaderForwarder) Close() {
	lf.mu.Lock()
	defer lf.mu.Unlock()

	if lf.conn != nil {
		lf.conn.Close()
		lf.conn = nil
		lf.client = nil
		lf.leaderAddr = ""
	}
}

// ForwardCreateCollection forwards a CreateCollection request to the leader
func (lf *LeaderForwarder) ForwardCreateCollection(ctx context.Context, leaderAddr string, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("CreateCollection").Inc()

	client, err := lf.getClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("CreateCollection", "connection").Inc()
		return nil, err
	}

	resp, err := client.CreateCollection(ctx, req)

	forwardedRequestsDuration.WithLabelValues("CreateCollection").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("CreateCollection", "rpc").Inc()
	}

	return resp, err
}

// ForwardDeleteCollection forwards a DeleteCollection request to the leader
func (lf *LeaderForwarder) ForwardDeleteCollection(ctx context.Context, leaderAddr string, req *manager_pb.DeleteCollectionRequest) (*manager_pb.DeleteCollectionResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("DeleteCollection").Inc()

	client, err := lf.getClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("DeleteCollection", "connection").Inc()
		return nil, err
	}

	resp, err := client.DeleteCollection(ctx, req)

	forwardedRequestsDuration.WithLabelValues("DeleteCollection").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("DeleteCollection", "rpc").Inc()
	}

	return resp, err
}

// ForwardRegisterService forwards a RegisterService request to the leader
func (lf *LeaderForwarder) ForwardRegisterService(ctx context.Context, leaderAddr string, req *manager_pb.RegisterServiceRequest) (*manager_pb.RegisterServiceResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("RegisterService").Inc()

	client, err := lf.getClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("RegisterService", "connection").Inc()
		return nil, err
	}

	resp, err := client.RegisterService(ctx, req)

	forwardedRequestsDuration.WithLabelValues("RegisterService").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("RegisterService", "rpc").Inc()
	}

	return resp, err
}

// ForwardUnregisterService forwards an UnregisterService request to the leader
func (lf *LeaderForwarder) ForwardUnregisterService(ctx context.Context, leaderAddr string, req *manager_pb.UnregisterServiceRequest) (*manager_pb.UnregisterServiceResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("UnregisterService").Inc()

	client, err := lf.getClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("UnregisterService", "connection").Inc()
		return nil, err
	}

	resp, err := client.UnregisterService(ctx, req)

	forwardedRequestsDuration.WithLabelValues("UnregisterService").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("UnregisterService", "rpc").Inc()
	}

	return resp, err
}

// ForwardRaftAddServer forwards a RaftAddServer request to the leader
func (lf *LeaderForwarder) ForwardRaftAddServer(ctx context.Context, leaderAddr string, req *manager_pb.RaftAddServerRequest) (*manager_pb.RaftAddServerResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("RaftAddServer").Inc()

	client, err := lf.getClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("RaftAddServer", "connection").Inc()
		return nil, err
	}

	resp, err := client.RaftAddServer(ctx, req)

	forwardedRequestsDuration.WithLabelValues("RaftAddServer").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("RaftAddServer", "rpc").Inc()
	}

	return resp, err
}

// ForwardRaftRemoveServer forwards a RaftRemoveServer request to the leader
func (lf *LeaderForwarder) ForwardRaftRemoveServer(ctx context.Context, leaderAddr string, req *manager_pb.RaftRemoveServerRequest) (*manager_pb.RaftRemoveServerResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("RaftRemoveServer").Inc()

	client, err := lf.getClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("RaftRemoveServer", "connection").Inc()
		return nil, err
	}

	resp, err := client.RaftRemoveServer(ctx, req)

	forwardedRequestsDuration.WithLabelValues("RaftRemoveServer").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("RaftRemoveServer", "rpc").Inc()
	}

	return resp, err
}

// getLeaderGrpcAddr gets the leader's gRPC address from the Raft node.
// Returns empty string if no leader is elected.
func getLeaderGrpcAddr(raftNode *RaftNode) string {
	leaderAddr := raftNode.Leader()
	if leaderAddr == "" {
		return ""
	}
	return raftAddrToGrpcAddr(leaderAddr)
}

// forwardOrError returns an error if there's no leader, otherwise returns the leader address for forwarding
func forwardOrError(raftNode *RaftNode) (string, error) {
	if raftNode.IsLeader() {
		return "", nil // Empty string means handle locally
	}

	leaderAddr := getLeaderGrpcAddr(raftNode)
	if leaderAddr == "" {
		return "", status.Error(codes.Unavailable, "no leader elected")
	}

	return leaderAddr, nil
}
