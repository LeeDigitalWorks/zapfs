package pool

import (
	"slices"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ClusterPool manages connections to a cluster of servers with automatic
// failover and retry logic. It maintains a list of known nodes and tries
// them in order on failures.
//
// This is a simplified design that relies on server-side forwarding (e.g.,
// LeaderForwarder) to route writes to the correct node. The client doesn't
// need to track which node is the leader.
type ClusterPool[T any] struct {
	pool    *Pool[T]
	opts    ClusterOptions
	factory ClientFactory[T]

	// Known nodes in the cluster
	mu       sync.RWMutex
	nodeAddrs []string

	closed atomic.Bool
}

// NewClusterPool creates a new cluster-aware connection pool.
func NewClusterPool[T any](
	factory ClientFactory[T],
	opts ClusterOptions,
) *ClusterPool[T] {
	// Create underlying pool with base options
	pool := NewPool(factory,
		WithDialTimeout(opts.DialTimeout),
		WithRequestTimeout(opts.RequestTimeout),
		WithMaxRetries(opts.MaxRetries),
		WithConnsPerHost(opts.ConnsPerHost),
		WithDialOpts(opts.DialOpts...),
	)

	return &ClusterPool[T]{
		pool:      pool,
		opts:      opts,
		factory:   factory,
		nodeAddrs: opts.SeedAddrs,
	}
}

// GetAny returns a client connected to any available node.
// Tries nodes in order until one succeeds.
func (cp *ClusterPool[T]) GetAny(ctx context.Context) (T, string, error) {
	var zero T
	if cp.closed.Load() {
		return zero, "", fmt.Errorf("cluster pool is closed")
	}

	cp.mu.RLock()
	addrs := cp.nodeAddrs
	cp.mu.RUnlock()

	var lastErr error
	for _, addr := range addrs {
		client, err := cp.pool.Get(ctx, addr)
		if err != nil {
			logger.Debug().Str("addr", addr).Err(err).Msg("failed to connect to node")
			lastErr = err
			continue
		}
		return client, addr, nil
	}

	if lastErr != nil {
		return zero, "", fmt.Errorf("failed to connect to any node: %w", lastErr)
	}
	return zero, "", fmt.Errorf("no nodes available")
}

// ExecuteOnAny executes an operation on any available node with retry and backoff.
// This is the primary method for both read and write operations.
// Server-side forwarding (LeaderForwarder) handles routing writes to the leader.
func (cp *ClusterPool[T]) ExecuteOnAny(ctx context.Context, op func(client T) error) error {
	var lastErr error
	backoff := cp.opts.InitialBackoff

	for attempt := 0; attempt <= cp.opts.MaxRetries; attempt++ {
		// Apply backoff before retry (not on first attempt)
		if attempt > 0 {
			sleepDuration := cp.calculateBackoff(backoff)
			select {
			case <-time.After(sleepDuration):
			case <-ctx.Done():
				return ctx.Err()
			}
			// Exponential backoff for next attempt
			backoff = min(backoff*2, cp.opts.MaxBackoff)
		}

		client, addr, err := cp.GetAny(ctx)
		if err != nil {
			lastErr = err
			logger.Debug().
				Int("attempt", attempt+1).
				Err(err).
				Msg("failed to get client, retrying")
			continue
		}

		err = op(client)
		if err == nil {
			return nil
		}

		// Check if error is retryable
		if !cp.isRetryableError(err) {
			return err // Non-retryable error, fail immediately
		}

		// Remove failed node from pool and try again
		cp.pool.Remove(addr)
		lastErr = err

		logger.Debug().
			Int("attempt", attempt+1).
			Str("addr", addr).
			Err(err).
			Msg("operation failed with retryable error, trying next node")
	}

	return fmt.Errorf("failed after %d attempts: %w", cp.opts.MaxRetries+1, lastErr)
}

// calculateBackoff returns the backoff duration with jitter applied
func (cp *ClusterPool[T]) calculateBackoff(base time.Duration) time.Duration {
	if cp.opts.BackoffJitter <= 0 {
		return base
	}
	// Add jitter: base ± (base * jitter)
	jitter := time.Duration(rand.Float64() * float64(base) * cp.opts.BackoffJitter)
	if rand.Float64() < 0.5 {
		return base - jitter
	}
	return base + jitter
}

// isRetryableError checks if the error should trigger a retry
func (cp *ClusterPool[T]) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	s, ok := status.FromError(err)
	if !ok {
		// Not a gRPC error - retry on connection errors
		return true
	}

	code := s.Code()
	return slices.Contains(cp.opts.RetryableCodes, code)
}

// UpdateNodes updates the list of known cluster nodes.
// This can be called when receiving topology updates from the cluster.
func (cp *ClusterPool[T]) UpdateNodes(addrs []string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.nodeAddrs = addrs
	logger.Debug().Strs("nodes", addrs).Msg("cluster nodes updated")
}

// AddNode adds a node to the known cluster nodes if not already present.
func (cp *ClusterPool[T]) AddNode(addr string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if slices.Contains(cp.nodeAddrs, addr) {
		return
	}
	cp.nodeAddrs = append(cp.nodeAddrs, addr)
}

// RemoveNode removes a node from the known cluster nodes.
func (cp *ClusterPool[T]) RemoveNode(addr string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.nodeAddrs = slices.DeleteFunc(cp.nodeAddrs, func(s string) bool {
		return s == addr
	})
}

// Close closes all connections in the pool
func (cp *ClusterPool[T]) Close() error {
	if !cp.closed.CompareAndSwap(false, true) {
		return nil
	}
	return cp.pool.Close()
}

// IsRetryableCode returns true if the given gRPC code is retryable
// This is a utility function for checking specific codes
func IsRetryableCode(code codes.Code) bool {
	return slices.Contains(DefaultRetryableCodes, code)
}
