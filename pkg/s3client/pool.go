// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package s3client provides a connection pool for external S3 clients.
// This is used by both the Manager (for federation) and Metadata (for proxying).
package s3client

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config holds configuration for connecting to an external S3 service.
type Config struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	PathStyle       bool
}

// Pool manages a pool of S3 clients for different external endpoints.
// Clients are cached by endpoint+region+accessKey for connection reuse.
type Pool struct {
	mu      sync.RWMutex
	clients map[string]*s3.Client
	timeout time.Duration
	maxIdle int

	// Shared HTTP client for connection reuse
	httpClient *http.Client
}

// NewPool creates a new client pool with the given timeout and max idle connections.
func NewPool(timeout time.Duration, maxIdleConns int) *Pool {
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	if maxIdleConns == 0 {
		maxIdleConns = 100
	}

	return &Pool{
		clients: make(map[string]*s3.Client),
		timeout: timeout,
		maxIdle: maxIdleConns,
		httpClient: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        maxIdleConns,
				MaxIdleConnsPerHost: maxIdleConns / 10, // 10% per host
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// GetClient returns an S3 client configured for the given config.
// Clients are cached by endpoint+region+accessKey for connection reuse.
func (p *Pool) GetClient(ctx context.Context, cfg *Config) (*s3.Client, error) {
	cacheKey := fmt.Sprintf("%s|%s|%s", cfg.Endpoint, cfg.Region, cfg.AccessKeyID)

	// Check cache first
	p.mu.RLock()
	client, exists := p.clients[cacheKey]
	p.mu.RUnlock()
	if exists {
		return client, nil
	}

	// Create new client
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if client, exists := p.clients[cacheKey]; exists {
		return client, nil
	}

	client, err := p.createClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	p.clients[cacheKey] = client

	logger.Debug().
		Str("endpoint", cfg.Endpoint).
		Str("region", cfg.Region).
		Msg("Created new external S3 client")

	return client, nil
}

// createClient creates a new S3 client for the given config.
func (p *Pool) createClient(ctx context.Context, cfg *Config) (*s3.Client, error) {
	// Create static credentials provider
	staticCreds := credentials.NewStaticCredentialsProvider(
		cfg.AccessKeyID,
		cfg.SecretAccessKey,
		"", // session token (empty for permanent credentials)
	)

	// Load AWS config with our credentials
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(staticCreds),
		config.WithHTTPClient(p.httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	// Create S3 client with custom endpoint if provided
	opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = cfg.PathStyle
		},
	}

	if cfg.Endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	return s3.NewFromConfig(awsCfg, opts...), nil
}

// Close closes the client pool and releases resources.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.clients = make(map[string]*s3.Client)
	p.httpClient.CloseIdleConnections()

	return nil
}
