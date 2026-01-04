// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

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
	"google.golang.org/grpc/connectivity"
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
	mu               sync.RWMutex
	leaderAddr       string
	conn             *grpc.ClientConn
	client           manager_pb.ManagerServiceClient
	federationClient manager_pb.FederationServiceClient

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
		lf.federationClient = nil
	}

	// Create new connection to leader
	conn, err := grpc.NewClient(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for leader %s: %w", leaderAddr, err)
	}

	// Trigger connection and wait for it to be ready
	conn.Connect()
	ctx, cancel := context.WithTimeout(context.Background(), lf.dialTimeout)
	defer cancel()

	if !waitForConnReady(ctx, conn) {
		conn.Close()
		return nil, fmt.Errorf("failed to connect to leader %s: timeout", leaderAddr)
	}

	lf.conn = conn
	lf.client = manager_pb.NewManagerServiceClient(conn)
	lf.federationClient = manager_pb.NewFederationServiceClient(conn)
	lf.leaderAddr = leaderAddr

	logger.Info().
		Str("leader_addr", leaderAddr).
		Msg("Connected to leader for forwarding")

	return lf.client, nil
}

// getFederationClient returns a FederationServiceClient connected to the given leader address.
func (lf *LeaderForwarder) getFederationClient(leaderAddr string) (manager_pb.FederationServiceClient, error) {
	// First ensure we have a connection (reuse getClient logic)
	_, err := lf.getClient(leaderAddr)
	if err != nil {
		return nil, err
	}

	lf.mu.RLock()
	client := lf.federationClient
	lf.mu.RUnlock()

	return client, nil
}

// waitForConnReady waits for the connection to become ready or the context to be done
func waitForConnReady(ctx context.Context, conn *grpc.ClientConn) bool {
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return true
		}
		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			return false
		}
		if !conn.WaitForStateChange(ctx, state) {
			return false // context done
		}
	}
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

// ForwardTriggerReconciliation forwards a TriggerReconciliation request to the leader
func (lf *LeaderForwarder) ForwardTriggerReconciliation(ctx context.Context, leaderAddr string, req *manager_pb.TriggerReconciliationRequest) (*manager_pb.TriggerReconciliationResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("TriggerReconciliation").Inc()

	client, err := lf.getClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("TriggerReconciliation", "connection").Inc()
		return nil, err
	}

	resp, err := client.TriggerReconciliation(ctx, req)

	forwardedRequestsDuration.WithLabelValues("TriggerReconciliation").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("TriggerReconciliation", "rpc").Inc()
	}

	return resp, err
}

// ===== FEDERATION SERVICE FORWARDERS =====

// ForwardFederationRegisterBucket forwards a RegisterBucket request to the leader
func (lf *LeaderForwarder) ForwardFederationRegisterBucket(ctx context.Context, leaderAddr string, req *manager_pb.FederationRegisterBucketRequest) (*manager_pb.FederationRegisterBucketResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("FederationRegisterBucket").Inc()

	client, err := lf.getFederationClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationRegisterBucket", "connection").Inc()
		return nil, err
	}

	resp, err := client.RegisterBucket(ctx, req)
	forwardedRequestsDuration.WithLabelValues("FederationRegisterBucket").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationRegisterBucket", "rpc").Inc()
	}

	return resp, err
}

// ForwardFederationUnregisterBucket forwards an UnregisterBucket request to the leader
func (lf *LeaderForwarder) ForwardFederationUnregisterBucket(ctx context.Context, leaderAddr string, req *manager_pb.FederationUnregisterBucketRequest) (*manager_pb.FederationUnregisterBucketResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("FederationUnregisterBucket").Inc()

	client, err := lf.getFederationClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationUnregisterBucket", "connection").Inc()
		return nil, err
	}

	resp, err := client.UnregisterBucket(ctx, req)
	forwardedRequestsDuration.WithLabelValues("FederationUnregisterBucket").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationUnregisterBucket", "rpc").Inc()
	}

	return resp, err
}

// ForwardFederationSetBucketMode forwards a SetBucketMode request to the leader
func (lf *LeaderForwarder) ForwardFederationSetBucketMode(ctx context.Context, leaderAddr string, req *manager_pb.FederationSetBucketModeRequest) (*manager_pb.FederationSetBucketModeResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("FederationSetBucketMode").Inc()

	client, err := lf.getFederationClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationSetBucketMode", "connection").Inc()
		return nil, err
	}

	resp, err := client.SetBucketMode(ctx, req)
	forwardedRequestsDuration.WithLabelValues("FederationSetBucketMode").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationSetBucketMode", "rpc").Inc()
	}

	return resp, err
}

// ForwardFederationPauseMigration forwards a PauseMigration request to the leader
func (lf *LeaderForwarder) ForwardFederationPauseMigration(ctx context.Context, leaderAddr string, req *manager_pb.FederationPauseMigrationRequest) (*manager_pb.FederationPauseMigrationResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("FederationPauseMigration").Inc()

	client, err := lf.getFederationClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationPauseMigration", "connection").Inc()
		return nil, err
	}

	resp, err := client.PauseMigration(ctx, req)
	forwardedRequestsDuration.WithLabelValues("FederationPauseMigration").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationPauseMigration", "rpc").Inc()
	}

	return resp, err
}

// ForwardFederationResumeMigration forwards a ResumeMigration request to the leader
func (lf *LeaderForwarder) ForwardFederationResumeMigration(ctx context.Context, leaderAddr string, req *manager_pb.FederationResumeMigrationRequest) (*manager_pb.FederationResumeMigrationResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("FederationResumeMigration").Inc()

	client, err := lf.getFederationClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationResumeMigration", "connection").Inc()
		return nil, err
	}

	resp, err := client.ResumeMigration(ctx, req)
	forwardedRequestsDuration.WithLabelValues("FederationResumeMigration").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationResumeMigration", "rpc").Inc()
	}

	return resp, err
}

// ForwardFederationSetDualWrite forwards a SetDualWrite request to the leader
func (lf *LeaderForwarder) ForwardFederationSetDualWrite(ctx context.Context, leaderAddr string, req *manager_pb.FederationSetDualWriteRequest) (*manager_pb.FederationSetDualWriteResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("FederationSetDualWrite").Inc()

	client, err := lf.getFederationClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationSetDualWrite", "connection").Inc()
		return nil, err
	}

	resp, err := client.SetDualWrite(ctx, req)
	forwardedRequestsDuration.WithLabelValues("FederationSetDualWrite").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationSetDualWrite", "rpc").Inc()
	}

	return resp, err
}

// ForwardFederationUpdateCredentials forwards an UpdateCredentials request to the leader
func (lf *LeaderForwarder) ForwardFederationUpdateCredentials(ctx context.Context, leaderAddr string, req *manager_pb.FederationUpdateCredentialsRequest) (*manager_pb.FederationUpdateCredentialsResponse, error) {
	start := time.Now()
	forwardedRequestsTotal.WithLabelValues("FederationUpdateCredentials").Inc()

	client, err := lf.getFederationClient(leaderAddr)
	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationUpdateCredentials", "connection").Inc()
		return nil, err
	}

	resp, err := client.UpdateCredentials(ctx, req)
	forwardedRequestsDuration.WithLabelValues("FederationUpdateCredentials").Observe(time.Since(start).Seconds())

	if err != nil {
		forwardedRequestsErrors.WithLabelValues("FederationUpdateCredentials", "rpc").Inc()
	}

	return resp, err
}
