package manager

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// HasExistingRaftState checks if there's existing Raft state in the data directory.
// This is used to determine if a node is restarting (has state) vs joining fresh (no state).
func HasExistingRaftState(dataDir string) bool {
	// Check for raft.db (the BoltDB store)
	raftDBPath := filepath.Join(dataDir, "raft.db")
	if _, err := os.Stat(raftDBPath); err == nil {
		return true
	}
	return false
}

type RaftNode struct {
	raft      *raft.Raft
	fsm       raft.FSM
	config    *Config
	transport *raft.NetworkTransport
	logStore  *raftboltdb.BoltStore

	shutdownCh chan struct{}
}

type Config struct {
	// Node identity
	NodeID   string
	BindAddr string
	DataDir  string

	// Raft tuning
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration
	MaxAppendEntries int

	// Cluster
	Bootstrap       bool
	BootstrapExpect int
}

func NewRaftNode(fsm raft.FSM, config *Config) (*RaftNode, error) {
	// Validate config
	if config.NodeID == "" {
		return nil, fmt.Errorf("NodeID is required")
	}
	if config.BindAddr == "" {
		return nil, fmt.Errorf("BindAddr is required")
	}
	if config.DataDir == "" {
		return nil, fmt.Errorf("DataDir is required")
	}

	rn := &RaftNode{
		fsm:        fsm,
		config:     config,
		shutdownCh: make(chan struct{}),
	}

	if err := rn.setupRaft(); err != nil {
		return nil, err
	}

	return rn, nil
}

func (rn *RaftNode) setupRaft() error {
	// Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.Logger = logger.ZerologRaftAdapter{}
	raftConfig.LocalID = raft.ServerID(rn.config.NodeID)

	// Apply custom timeouts if set
	if rn.config.HeartbeatTimeout > 0 {
		raftConfig.HeartbeatTimeout = rn.config.HeartbeatTimeout
	}
	if rn.config.ElectionTimeout > 0 {
		raftConfig.ElectionTimeout = rn.config.ElectionTimeout
	}
	if rn.config.CommitTimeout > 0 {
		raftConfig.CommitTimeout = rn.config.CommitTimeout
	}
	if rn.config.MaxAppendEntries > 0 {
		raftConfig.MaxAppendEntries = rn.config.MaxAppendEntries
	}

	// Snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(rn.config.DataDir, 2, nil)
	if err != nil {
		return fmt.Errorf("snapshot store: %w", err)
	}

	// Log store and stable store (BoltDB)
	logStore, err := raftboltdb.NewBoltStore(rn.config.DataDir + "/raft.db")
	if err != nil {
		return fmt.Errorf("bolt store: %w", err)
	}
	rn.logStore = logStore

	// Transport
	addr, err := net.ResolveTCPAddr("tcp", rn.config.BindAddr)
	if err != nil {
		return fmt.Errorf("resolve tcp addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(rn.config.BindAddr, addr, 3, 10*time.Second, nil)
	if err != nil {
		return fmt.Errorf("tcp transport: %w", err)
	}
	rn.transport = transport

	// Create Raft
	ra, err := raft.NewRaft(raftConfig, rn.fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %w", err)
	}
	rn.raft = ra

	// Bootstrap if needed
	if rn.config.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		future := ra.BootstrapCluster(configuration)
		if err := future.Error(); err != nil {
			logger.Warn().Err(err).Msg("Bootstrap failed (may already be bootstrapped)")
		} else {
			logger.Info().Msg("Bootstrapped Raft cluster")
		}
	}

	return nil
}

// IsLeader returns true if this node is the leader
func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

// Leader returns the current leader address
func (rn *RaftNode) Leader() string {
	return string(rn.raft.Leader())
}

// HasLeader returns true if the cluster has an elected leader
func (rn *RaftNode) HasLeader() bool {
	return rn.raft.Leader() != ""
}

// State returns the current Raft state (Follower, Candidate, Leader, Shutdown)
func (rn *RaftNode) State() string {
	return rn.raft.State().String()
}

// Apply applies a log entry
func (rn *RaftNode) Apply(data []byte, timeout time.Duration) error {
	future := rn.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return err
	}

	// Check response for errors
	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}

	return nil
}

// GetConfiguration returns the cluster configuration
func (rn *RaftNode) GetConfiguration() (raft.Configuration, error) {
	future := rn.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return raft.Configuration{}, err
	}
	return future.Configuration(), nil
}

// AddVoter adds a voting member to the cluster
func (rn *RaftNode) AddVoter(id, address string, timeout time.Duration) error {
	future := rn.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, timeout)
	return future.Error()
}

// AddNonvoter adds a non-voting member to the cluster
func (rn *RaftNode) AddNonvoter(id, address string, timeout time.Duration) error {
	future := rn.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(address), 0, timeout)
	return future.Error()
}

// RemoveServer removes a server from the cluster
func (rn *RaftNode) RemoveServer(id string, timeout time.Duration) error {
	future := rn.raft.RemoveServer(raft.ServerID(id), 0, timeout)
	return future.Error()
}

// Stats returns Raft stats
func (rn *RaftNode) Stats() map[string]string {
	return rn.raft.Stats()
}

// Shutdown gracefully shuts down the Raft node
func (rn *RaftNode) Shutdown() error {
	close(rn.shutdownCh)

	if rn.raft != nil {
		future := rn.raft.Shutdown()
		if err := future.Error(); err != nil {
			logger.Error().Err(err).Msg("Error shutting down raft")
			return err
		}
	}

	if rn.logStore != nil {
		return rn.logStore.Close()
	}

	return nil
}

// WaitForLeader blocks until a leader is elected or timeout
func (rn *RaftNode) WaitForLeader(timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			if rn.Leader() != "" {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout waiting for leader")
		}
	}
}
