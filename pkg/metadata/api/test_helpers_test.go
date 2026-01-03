// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"io"
	"sync"
	"testing"

	cachemocks "github.com/LeeDigitalWorks/zapfs/mocks/cache"
	clientmocks "github.com/LeeDigitalWorks/zapfs/mocks/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/cache"
	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db/memory"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/filter"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// TestServerOption configures a test server
type TestServerOption func(*testServerConfig)

type testServerConfig struct {
	managerClient client.Manager
	fileClient    client.File
	iamService    *iam.Service
	db            db.DB
}

// WithManagerClient injects a mock manager client for testing handlers
// that interact with the manager (CreateBucket, DeleteBucket, etc.)
func WithManagerClient(mc client.Manager) TestServerOption {
	return func(cfg *testServerConfig) {
		cfg.managerClient = mc
	}
}

// WithFileClient injects a mock file client for testing handlers
// that interact with file servers (PutObject, GetObject, etc.)
func WithFileClient(fc client.File) TestServerOption {
	return func(cfg *testServerConfig) {
		cfg.fileClient = fc
	}
}

// WithIAMService injects an IAM service for testing handlers
// that require IAM/KMS functionality (SSE-KMS, etc.)
func WithIAMService(iamSvc *iam.Service) TestServerOption {
	return func(cfg *testServerConfig) {
		cfg.iamService = iamSvc
	}
}

// WithDB injects a database for testing handlers.
// If not provided, an in-memory test DB is created automatically.
func WithDB(mockDB db.DB) TestServerOption {
	return func(cfg *testServerConfig) {
		cfg.db = mockDB
	}
}

// newTestServer creates a MetadataServer for unit testing.
// Uses in-memory DB and mock clients - no real gRPC connections.
//
// Usage:
//
//	// Simple - uses in-memory DB and auto-created mock clients
//	srv := newTestServer(t)
//
//	// With mock manager client for operations that need it
//	mockMgr := newMockManagerClient(t)
//	mockMgr.EXPECT().GetReplicationTargets(mock.Anything, mock.Anything).Return(...)
//	srv := newTestServer(t, WithManagerClient(mockMgr))
//
//	// With both mocks for PutObject/GetObject operations
//	mockMgr := newMockManagerClient(t)
//	mockFile := newMockFileClient(t)
//	srv := newTestServer(t, WithManagerClient(mockMgr), WithFileClient(mockFile))
func newTestServer(t *testing.T, opts ...TestServerOption) *MetadataServer {
	t.Helper()

	cfg := &testServerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	ctx := context.Background()

	// Create bucket caches
	bucketsCache := cache.New(ctx, cache.WithMaxSize[string, s3types.Bucket](1000))
	bucketStore := cache.NewBucketStore(bucketsCache)

	// Create global bucket cache with mock
	mockBucketCacheClient := cachemocks.NewMockBucketCacheClient(t)
	globalBucketCache := cache.NewGlobalBucketCache(ctx, mockBucketCacheClient)

	// Create a pass-through filter chain
	chain := filter.NewChain()

	// Create test pools and profiles
	pools := types.NewPoolSet()
	profiles := types.NewProfileSet()

	// Use provided DB or create in-memory test DB
	testDatabase := cfg.db
	if testDatabase == nil {
		testDatabase = memory.New()
	}

	// Use provided manager client or create a mock
	// The mock is created to satisfy the service layer validation
	// Tests that need specific manager behavior should provide their own mock
	managerClient := cfg.managerClient
	if managerClient == nil {
		mockMgr := clientmocks.NewMockManager(t)
		// Set up default no-op behavior for cleanup
		mockMgr.EXPECT().Close().Return(nil).Maybe()
		managerClient = mockMgr
	}

	// Use provided file client or create a mock
	fileClient := cfg.fileClient
	if fileClient == nil {
		mockFile := clientmocks.NewMockFile(t)
		// Set up default no-op behavior for cleanup
		mockFile.EXPECT().Close().Return(nil).Maybe()
		fileClient = mockFile
	}

	srv := NewMetadataServer(ctx, ServerConfig{
		Chain:             chain,
		BucketStore:       bucketStore,
		GlobalBucketCache: globalBucketCache,
		Pools:             pools,
		Profiles:          profiles,
		DefaultProfile:    "STANDARD",
		ManagerClient:     managerClient,
		FileClientPool:    fileClient,
		IAMService:        cfg.iamService,
		DB:                testDatabase,
	})

	// Expose the DB and globalBucketCache for test setup
	srv.db = testDatabase
	srv.globalBucketCache = globalBucketCache

	// Register cleanup to stop background goroutines (usage aggregator, reporter, etc.)
	// Note: We don't call srv.Shutdown() because tests may provide their own mocks
	// that don't have Close() expectations. We just need to stop the usage components.
	t.Cleanup(func() {
		if srv.usageCollector != nil {
			srv.usageCollector.Stop()
		}
		if srv.usageAggregator != nil {
			srv.usageAggregator.Stop()
		}
		if srv.usageReporter != nil {
			srv.usageReporter.Stop()
		}
	})

	return srv
}

// threadSafeFileClient is a thread-safe wrapper for file client mocks.
// It's designed to avoid data races when testify mocks are called concurrently
// (e.g., in storage coordinator's writeToAllTargets which uses goroutines).
type threadSafeFileClient struct {
	mu              sync.Mutex
	putObjectFn     func(ctx context.Context, address, objectID string, data io.Reader, totalSize uint64) (*client.PutObjectResult, error)
	getObjectFn     func(ctx context.Context, address, objectID string, callback client.ObjectWriter) (string, error)
	getChunkFn      func(ctx context.Context, address, chunkID string, callback client.ObjectWriter) error
	getChunkRangeFn func(ctx context.Context, address, chunkID string, offset, length uint64, callback client.ObjectWriter) error
	closeFn         func() error
}

// newThreadSafeFileClient creates a thread-safe file client for concurrent PutObject tests.
func newThreadSafeFileClient() *threadSafeFileClient {
	return &threadSafeFileClient{
		closeFn: func() error { return nil },
	}
}

// OnPutObject sets the handler for PutObject calls.
func (m *threadSafeFileClient) OnPutObject(fn func(ctx context.Context, address, objectID string, data io.Reader, totalSize uint64) (*client.PutObjectResult, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putObjectFn = fn
}

func (m *threadSafeFileClient) PutObject(ctx context.Context, address, objectID string, data io.Reader, totalSize uint64) (*client.PutObjectResult, error) {
	m.mu.Lock()
	fn := m.putObjectFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, address, objectID, data, totalSize)
	}
	// Drain the reader to avoid blocking
	io.Copy(io.Discard, data)
	return &client.PutObjectResult{ObjectID: objectID, Size: totalSize}, nil
}

func (m *threadSafeFileClient) GetObject(ctx context.Context, address, objectID string, callback client.ObjectWriter) (string, error) {
	m.mu.Lock()
	fn := m.getObjectFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, address, objectID, callback)
	}
	return "", nil
}

func (m *threadSafeFileClient) GetObjectRange(ctx context.Context, address, objectID string, offset, length uint64, callback client.ObjectWriter) (string, error) {
	return "", nil
}

func (m *threadSafeFileClient) GetChunk(ctx context.Context, address, chunkID string, callback client.ObjectWriter) error {
	m.mu.Lock()
	fn := m.getChunkFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, address, chunkID, callback)
	}
	return nil
}

func (m *threadSafeFileClient) GetChunkRange(ctx context.Context, address, chunkID string, offset, length uint64, callback client.ObjectWriter) error {
	m.mu.Lock()
	fn := m.getChunkRangeFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, address, chunkID, offset, length, callback)
	}
	return nil
}

func (m *threadSafeFileClient) Close() error {
	m.mu.Lock()
	fn := m.closeFn
	m.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return nil
}
