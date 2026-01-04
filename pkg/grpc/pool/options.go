// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package pool provides generic gRPC connection pooling with optional
// cluster-aware routing for Raft-based services.
package pool

import (
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	// Default configuration values
	DefaultDialTimeout       = 5 * time.Second
	DefaultRequestTimeout    = 10 * time.Second
	DefaultHealthCheckPeriod = 30 * time.Second
	DefaultMaxRetries        = 3
	DefaultConnsPerHost      = 4

	// Retry backoff configuration
	DefaultInitialBackoff = 100 * time.Millisecond
	DefaultMaxBackoff     = 5 * time.Second
	DefaultBackoffJitter  = 0.2 // 20% jitter

	// Transport-level retry configuration (go-grpc-middleware)
	DefaultTransportRetries = 3
	DefaultPerRetryTimeout  = 5 * time.Second
	DefaultTransportBackoff = 100 * time.Millisecond

	// KeepAlive settings matching proto/grpc.go
	KeepAliveTime    = 60 * time.Second
	KeepAliveTimeout = 20 * time.Second

	// Max message size (1 GiB)
	MaxMessageSize = 1 << 30
)

// DefaultRetryableCodes are gRPC status codes that should trigger a retry
var DefaultRetryableCodes = []codes.Code{
	codes.Unavailable,       // Server temporarily unavailable
	codes.ResourceExhausted, // Rate limiting, resource limits
	codes.DeadlineExceeded,  // Timeout (may succeed on retry)
	codes.Aborted,           // Operation aborted (often retryable)
}

// Options configures a connection pool
type Options struct {
	// DialTimeout is the timeout for establishing new connections
	DialTimeout time.Duration

	// RequestTimeout is the default timeout for requests
	RequestTimeout time.Duration

	// HealthCheckPeriod is how often to check connection health
	HealthCheckPeriod time.Duration

	// MaxRetries is the number of times to retry failed requests
	MaxRetries int

	// ConnsPerHost is the number of connections to maintain per host
	ConnsPerHost int

	// DialOpts are additional gRPC dial options
	DialOpts []grpc.DialOption
}

// DefaultOptions returns Options with sensible defaults
func DefaultOptions() Options {
	return Options{
		DialTimeout:       DefaultDialTimeout,
		RequestTimeout:    DefaultRequestTimeout,
		HealthCheckPeriod: DefaultHealthCheckPeriod,
		MaxRetries:        DefaultMaxRetries,
		ConnsPerHost:      DefaultConnsPerHost,
		DialOpts:          DefaultDialOpts(),
	}
}

// DefaultDialOpts returns the default gRPC dial options with retry interceptor
func DefaultDialOpts() []grpc.DialOption {
	// Transport-level retry for transient connection issues
	retryOpts := []retry.CallOption{
		retry.WithMax(DefaultTransportRetries),
		retry.WithBackoff(retry.BackoffExponentialWithJitter(DefaultTransportBackoff, DefaultBackoffJitter)),
		retry.WithCodes(DefaultRetryableCodes...),
		retry.WithPerRetryTimeout(DefaultPerRetryTimeout),
	}

	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    KeepAliveTime,
			Timeout: KeepAliveTimeout,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageSize),
			grpc.MaxCallSendMsgSize(MaxMessageSize),
		),
		grpc.WithChainUnaryInterceptor(retry.UnaryClientInterceptor(retryOpts...)),
	}
}

// ClusterOptions extends Options with cluster-aware settings
type ClusterOptions struct {
	Options

	// SeedAddrs are initial addresses to connect to
	SeedAddrs []string

	// Cluster-level retry backoff configuration
	// (separate from transport-level retry in dial options)
	InitialBackoff time.Duration // Initial backoff between cluster-level retries
	MaxBackoff     time.Duration // Maximum backoff duration
	BackoffJitter  float64       // Jitter fraction (0.0 to 1.0)

	// RetryableCodes defines which gRPC codes trigger cluster-level retry
	RetryableCodes []codes.Code
}

// DefaultClusterOptions returns ClusterOptions with sensible defaults
func DefaultClusterOptions(seedAddrs []string) ClusterOptions {
	return ClusterOptions{
		Options:        DefaultOptions(),
		SeedAddrs:      seedAddrs,
		InitialBackoff: DefaultInitialBackoff,
		MaxBackoff:     DefaultMaxBackoff,
		BackoffJitter:  DefaultBackoffJitter,
		RetryableCodes: DefaultRetryableCodes,
	}
}

// Option is a functional option for configuring pools
type Option func(*Options)

// WithDialTimeout sets the dial timeout
func WithDialTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.DialTimeout = d
	}
}

// WithRequestTimeout sets the request timeout
func WithRequestTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.RequestTimeout = d
	}
}

// WithMaxRetries sets the max retries
func WithMaxRetries(n int) Option {
	return func(o *Options) {
		o.MaxRetries = n
	}
}

// WithConnsPerHost sets connections per host
func WithConnsPerHost(n int) Option {
	return func(o *Options) {
		o.ConnsPerHost = n
	}
}

// WithDialOpts sets additional dial options
func WithDialOpts(opts ...grpc.DialOption) Option {
	return func(o *Options) {
		o.DialOpts = append(o.DialOpts, opts...)
	}
}

// ClusterOption is a functional option for configuring cluster pools
type ClusterOption func(*ClusterOptions)

// WithInitialBackoff sets the initial backoff duration for cluster-level retries
func WithInitialBackoff(d time.Duration) ClusterOption {
	return func(o *ClusterOptions) {
		o.InitialBackoff = d
	}
}

// WithMaxBackoff sets the maximum backoff duration for cluster-level retries
func WithMaxBackoff(d time.Duration) ClusterOption {
	return func(o *ClusterOptions) {
		o.MaxBackoff = d
	}
}

// WithBackoffJitter sets the jitter fraction (0.0 to 1.0) for backoff
func WithBackoffJitter(jitter float64) ClusterOption {
	return func(o *ClusterOptions) {
		o.BackoffJitter = jitter
	}
}

// WithRetryableCodes sets which gRPC codes trigger cluster-level retry
func WithRetryableCodes(codes ...codes.Code) ClusterOption {
	return func(o *ClusterOptions) {
		o.RetryableCodes = codes
	}
}

// WithBaseOptions applies base pool options to cluster options
func WithBaseOptions(opts ...Option) ClusterOption {
	return func(o *ClusterOptions) {
		for _, opt := range opts {
			opt(&o.Options)
		}
	}
}
