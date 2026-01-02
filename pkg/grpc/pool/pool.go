// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// ClientFactory creates a gRPC client from a connection
type ClientFactory[T any] func(cc grpc.ClientConnInterface) T

// Pool manages gRPC connections to multiple hosts.
// It provides lazy connection creation, connection reuse, and automatic cleanup.
type Pool[T any] struct {
	mu      sync.RWMutex
	hosts   map[string]*hostPool[T] // address -> pool
	opts    Options
	factory ClientFactory[T]
	closed  atomic.Bool
}

// hostPool manages connections to a single host
type hostPool[T any] struct {
	mu      sync.Mutex
	address string
	conns   []*grpc.ClientConn
	clients []T
	opts    Options
	factory ClientFactory[T]
}

// NewPool creates a new connection pool with the given client factory
func NewPool[T any](factory ClientFactory[T], opts ...Option) *Pool[T] {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	return &Pool[T]{
		hosts:   make(map[string]*hostPool[T]),
		opts:    options,
		factory: factory,
	}
}

// Get returns a client for the given address.
// Creates connections lazily if they don't exist.
func (p *Pool[T]) Get(ctx context.Context, address string) (T, error) {
	var zero T
	if p.closed.Load() {
		return zero, fmt.Errorf("pool is closed")
	}

	hp := p.getOrCreateHostPool(address)
	return hp.get(ctx)
}

// getOrCreateHostPool gets or creates a host pool for the address
func (p *Pool[T]) getOrCreateHostPool(address string) *hostPool[T] {
	// Fast path: check if exists
	p.mu.RLock()
	hp, exists := p.hosts[address]
	p.mu.RUnlock()
	if exists {
		return hp
	}

	// Slow path: create new
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check
	if hp, exists := p.hosts[address]; exists {
		return hp
	}

	hp = &hostPool[T]{
		address: address,
		conns:   make([]*grpc.ClientConn, 0, p.opts.ConnsPerHost),
		clients: make([]T, 0, p.opts.ConnsPerHost),
		opts:    p.opts,
		factory: p.factory,
	}
	p.hosts[address] = hp

	logger.Debug().Str("address", address).Msg("created new host pool")
	return hp
}

// Remove removes all connections for an address
func (p *Pool[T]) Remove(address string) {
	p.mu.Lock()
	hp, exists := p.hosts[address]
	if exists {
		delete(p.hosts, address)
	}
	p.mu.Unlock()

	if exists {
		hp.close()
		logger.Debug().Str("address", address).Msg("removed host from pool")
	}
}

// Close closes all connections in the pool
func (p *Pool[T]) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	p.mu.Lock()
	hosts := p.hosts
	p.hosts = make(map[string]*hostPool[T])
	p.mu.Unlock()

	var errs []error
	for _, hp := range hosts {
		if err := hp.close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing pool: %v", errs)
	}
	return nil
}

// Addresses returns all addresses in the pool
func (p *Pool[T]) Addresses() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	addrs := make([]string, 0, len(p.hosts))
	for addr := range p.hosts {
		addrs = append(addrs, addr)
	}
	return addrs
}

// get returns a client, creating a connection if needed
func (hp *hostPool[T]) get(ctx context.Context) (T, error) {
	var zero T

	hp.mu.Lock()
	defer hp.mu.Unlock()

	// Try to find a healthy existing connection
	for i, conn := range hp.conns {
		state := conn.GetState()
		if state == connectivity.Ready || state == connectivity.Idle {
			// Round-robin across healthy connections
			return hp.clients[i], nil
		}
	}

	// Need to create a new connection (or replace unhealthy ones)
	if len(hp.conns) < hp.opts.ConnsPerHost {
		client, err := hp.createConnection(ctx)
		if err != nil {
			return zero, err
		}
		return client, nil
	}

	// All connections exist but unhealthy, try to reconnect first one
	conn := hp.conns[0]
	conn.Connect()
	if conn.GetState() == connectivity.Ready {
		return hp.clients[0], nil
	}

	// Create replacement connection
	client, err := hp.createConnection(ctx)
	if err != nil {
		return zero, err
	}
	return client, nil
}

// createConnection creates a new connection to the host
func (hp *hostPool[T]) createConnection(ctx context.Context) (T, error) {
	var zero T

	// grpc.NewClient is the recommended way to create connections (grpc.DialContext is deprecated)
	// NewClient creates a "virtual" connection that connects lazily on first RPC
	conn, err := grpc.NewClient(hp.address, hp.opts.DialOpts...)
	if err != nil {
		return zero, fmt.Errorf("failed to create client for %s: %w", hp.address, err)
	}

	client := hp.factory(conn)
	hp.conns = append(hp.conns, conn)
	hp.clients = append(hp.clients, client)

	logger.Debug().
		Str("address", hp.address).
		Int("total_conns", len(hp.conns)).
		Msg("created new connection")

	return client, nil
}

// close closes all connections in the host pool
func (hp *hostPool[T]) close() error {
	hp.mu.Lock()
	defer hp.mu.Unlock()

	var errs []error
	for _, conn := range hp.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	hp.conns = nil
	hp.clients = nil

	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}
	return nil
}
