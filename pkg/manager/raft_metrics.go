// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"strconv"
	"sync"

	"github.com/LeeDigitalWorks/zapfs/pkg/debug"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// RaftState tracks the current Raft state (0=Follower, 1=Candidate, 2=Leader, 3=Shutdown)
	RaftState = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "raft",
		Name:      "state",
		Help:      "Current Raft state (0=Follower, 1=Candidate, 2=Leader, 3=Shutdown)",
	})

	// RaftIsLeader is 1 if this node is the leader, 0 otherwise
	RaftIsLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "raft",
		Name:      "is_leader",
		Help:      "1 if this node is the Raft leader, 0 otherwise",
	})

	// RaftTerm tracks the current Raft term
	RaftTerm = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "raft",
		Name:      "term",
		Help:      "Current Raft term",
	})

	// RaftCommitIndex tracks the commit index
	RaftCommitIndex = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "raft",
		Name:      "commit_index",
		Help:      "Current Raft commit index",
	})

	// RaftAppliedIndex tracks the last applied index
	RaftAppliedIndex = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "raft",
		Name:      "applied_index",
		Help:      "Last applied Raft index",
	})

	// RaftLastLogIndex tracks the last log index
	RaftLastLogIndex = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "raft",
		Name:      "last_log_index",
		Help:      "Last Raft log index",
	})

	// RaftLeaderChanges counts leader election changes
	RaftLeaderChanges = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "raft",
		Name:      "leader_changes_total",
		Help:      "Total number of leader changes observed",
	})

	// RaftPeers tracks the number of peers in the cluster
	RaftPeers = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "raft",
		Name:      "peers",
		Help:      "Number of peers in the Raft cluster",
	})

	// RaftFSMApplyLatency tracks FSM apply latency
	RaftFSMApplyLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "zapfs",
		Subsystem: "raft",
		Name:      "fsm_apply_duration_seconds",
		Help:      "Duration of FSM apply operations",
		Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})

	// RaftSnapshotLatency tracks snapshot creation latency
	RaftSnapshotLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "zapfs",
		Subsystem: "raft",
		Name:      "snapshot_duration_seconds",
		Help:      "Duration of Raft snapshot operations",
		Buckets:   []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
	})
)

func init() {
	debug.Registry().MustRegister(
		RaftState,
		RaftIsLeader,
		RaftTerm,
		RaftCommitIndex,
		RaftAppliedIndex,
		RaftLastLogIndex,
		RaftLeaderChanges,
		RaftPeers,
		RaftFSMApplyLatency,
		RaftSnapshotLatency,
	)
}

// UpdateRaftMetrics updates Raft metrics from the Raft stats map.
// This should be called periodically (e.g., every second) by the manager server.
func UpdateRaftMetrics(stats map[string]string, isLeader bool, numPeers int) {
	// Update state
	stateMap := map[string]float64{
		"Follower":  0,
		"Candidate": 1,
		"Leader":    2,
		"Shutdown":  3,
	}
	if state, ok := stats["state"]; ok {
		if val, exists := stateMap[state]; exists {
			RaftState.Set(val)
		}
	}

	// Update leader status
	if isLeader {
		RaftIsLeader.Set(1)
	} else {
		RaftIsLeader.Set(0)
	}

	// Update term
	if term, ok := stats["term"]; ok {
		if val, err := strconv.ParseFloat(term, 64); err == nil {
			RaftTerm.Set(val)
		}
	}

	// Update commit index
	if commitIndex, ok := stats["commit_index"]; ok {
		if val, err := strconv.ParseFloat(commitIndex, 64); err == nil {
			RaftCommitIndex.Set(val)
		}
	}

	// Update applied index
	if appliedIndex, ok := stats["applied_index"]; ok {
		if val, err := strconv.ParseFloat(appliedIndex, 64); err == nil {
			RaftAppliedIndex.Set(val)
		}
	}

	// Update last log index
	if lastLogIndex, ok := stats["last_log_index"]; ok {
		if val, err := strconv.ParseFloat(lastLogIndex, 64); err == nil {
			RaftLastLogIndex.Set(val)
		}
	}

	// Update peer count
	RaftPeers.Set(float64(numPeers))
}

// leaderTracker tracks leader changes
type leaderTracker struct {
	mu         sync.Mutex
	lastLeader string
}

var tracker = &leaderTracker{}

// TrackLeaderChange should be called when leader changes are observed
func TrackLeaderChange(newLeader string) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	if tracker.lastLeader != "" && tracker.lastLeader != newLeader {
		RaftLeaderChanges.Inc()
	}
	tracker.lastLeader = newLeader
}
