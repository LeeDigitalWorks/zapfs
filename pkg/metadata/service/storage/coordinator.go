// Package storage provides coordination between file servers and the manager
// for object storage operations.
package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue/handlers"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
)

// Coordinator manages interaction with file servers and the manager.
// It handles replication target selection, data streaming, and orphan cleanup.
type Coordinator struct {
	managerClient  client.Manager
	fileClientPool client.File
	profiles       *types.ProfileSet
	defaultProfile string
	targetCache    *TargetCache
	taskQueue      taskqueue.Queue // Optional: for queueing failed decrements
}

// CoordinatorConfig holds configuration for the storage coordinator
type CoordinatorConfig struct {
	ManagerClient  client.Manager
	FileClientPool client.File
	Profiles       *types.ProfileSet
	DefaultProfile string
	CacheConfig    *TargetCacheConfig // Optional, uses defaults if nil
	TaskQueue      taskqueue.Queue    // Optional: for queueing failed decrements
}

// NewCoordinator creates a new storage coordinator
func NewCoordinator(cfg CoordinatorConfig) *Coordinator {
	// Initialize target cache
	cacheConfig := DefaultTargetCacheConfig()
	if cfg.CacheConfig != nil {
		cacheConfig = *cfg.CacheConfig
	}

	return &Coordinator{
		managerClient:  cfg.ManagerClient,
		fileClientPool: cfg.FileClientPool,
		profiles:       cfg.Profiles,
		defaultProfile: cfg.DefaultProfile,
		targetCache:    NewTargetCache(cacheConfig),
		taskQueue:      cfg.TaskQueue,
	}
}

// Start starts the coordinator's background tasks (topology watch)
func (c *Coordinator) Start(ctx context.Context) {
	if c.targetCache != nil && c.targetCache.IsEnabled() && c.managerClient != nil {
		go c.watchTopologyLoop(ctx)
	}
}

// Stop stops the coordinator's background tasks
func (c *Coordinator) Stop() {
	if c.targetCache != nil {
		c.targetCache.Stop()
	}
}

// watchTopologyLoop subscribes to topology updates from the manager.
// Automatically reconnects on connection loss with exponential backoff.
func (c *Coordinator) watchTopologyLoop(ctx context.Context) {
	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := c.watchTopology(ctx); err != nil {
			if ctx.Err() != nil {
				return // Context cancelled, exit
			}
			logger.Warn().Err(err).Dur("backoff", backoff).Msg("Topology watch disconnected, reconnecting...")
			time.Sleep(backoff)
			// Exponential backoff with max 30 seconds
			backoff = min(backoff*2, 30*time.Second)
		} else {
			backoff = time.Second // Reset backoff on clean disconnect
		}
	}
}

// watchTopology subscribes to topology updates and processes events
func (c *Coordinator) watchTopology(ctx context.Context) error {
	stream, err := c.managerClient.WatchTopology(ctx, &manager_pb.WatchTopologyRequest{
		CurrentVersion: c.targetCache.TopologyVersion(),
	})
	if err != nil {
		return fmt.Errorf("failed to start topology watch: %w", err)
	}

	logger.Info().Msg("Connected to manager topology stream")

	for {
		event, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("topology stream error: %w", err)
		}

		c.handleTopologyEvent(event)
	}
}

// handleTopologyEvent processes a topology event and updates the cache
func (c *Coordinator) handleTopologyEvent(event *manager_pb.TopologyEvent) {
	switch event.Type {
	case manager_pb.TopologyEvent_FULL_SYNC:
		c.targetCache.Update(event.Services, event.Version)
		logger.Debug().
			Uint64("version", event.Version).
			Int("services", len(event.Services)).
			Msg("Topology full sync received")

	case manager_pb.TopologyEvent_SERVICE_ADDED:
		c.targetCache.Update(event.Services, event.Version)
		logger.Debug().
			Uint64("version", event.Version).
			Int("services", len(event.Services)).
			Msg("Topology: services added")

	case manager_pb.TopologyEvent_SERVICE_REMOVED:
		c.targetCache.Remove(event.Services, event.Version)
		logger.Debug().
			Uint64("version", event.Version).
			Int("services", len(event.Services)).
			Msg("Topology: services removed")

	case manager_pb.TopologyEvent_SERVICE_UPDATED:
		c.targetCache.Update(event.Services, event.Version)
		logger.Debug().
			Uint64("version", event.Version).
			Int("services", len(event.Services)).
			Msg("Topology: services updated")
	}
}

// writeResult holds the outcome of writing to a single target
type writeResult struct {
	target  *manager_pb.ReplicationTarget
	size    uint64
	err     error
}

// WriteObject writes data to ALL file servers in parallel.
// Flow:
// 1. Get replication targets from cache or manager
// 2. Create pipes for each target
// 3. Tee incoming data to all pipes simultaneously
// 4. Collect results and build chunk refs for successful writes
//
// This approach ensures no single point of failure and no memory buffering.
func (c *Coordinator) WriteObject(ctx context.Context, req *WriteRequest) (*WriteResult, error) {
	// Get storage profile
	profileName := req.ProfileName
	if profileName == "" {
		profileName = c.defaultProfile
	}

	profile, exists := c.profiles.Get(profileName)
	if !exists {
		return nil, fmt.Errorf("storage profile not found: %s", profileName)
	}

	replication := req.Replication
	if replication == 0 {
		replication = profile.Replication
	}

	// Try to get targets from cache first
	var targetList []*manager_pb.ReplicationTarget
	if c.targetCache != nil && c.targetCache.IsEnabled() {
		targetList = c.targetCache.SelectTargets(req.Size, uint32(replication), profileName)
	}

	// Fallback to manager if cache miss or insufficient targets
	if len(targetList) < replication {
		targets, err := c.managerClient.GetReplicationTargets(ctx, &manager_pb.GetReplicationTargetsRequest{
			FileSize:    req.Size,
			NumReplicas: uint32(replication),
			Tier:        profileName,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get replication targets: %w", err)
		}
		targetList = targets.Targets
	}

	if len(targetList) == 0 {
		return nil, fmt.Errorf("no replication targets available for profile %s", profileName)
	}

	// Stream to ALL targets in parallel using pipes
	return c.writeToAllTargets(ctx, req, targetList)
}

// writeToAllTargets streams data to all targets in parallel.
// Uses io.Pipe to create independent streams for each target.
func (c *Coordinator) writeToAllTargets(ctx context.Context, req *WriteRequest, targets []*manager_pb.ReplicationTarget) (*WriteResult, error) {
	numTargets := len(targets)

	// Create a pipe for each target
	type targetPipe struct {
		target *manager_pb.ReplicationTarget
		reader *io.PipeReader
		writer *io.PipeWriter
	}
	pipes := make([]targetPipe, numTargets)
	writers := make([]io.Writer, numTargets)

	for i, target := range targets {
		pr, pw := io.Pipe()
		pipes[i] = targetPipe{target: target, reader: pr, writer: pw}
		writers[i] = pw
	}

	// Results channel
	results := make(chan writeResult, numTargets)

	// Start a goroutine for each target to consume from its pipe
	var wg sync.WaitGroup
	for i := range pipes {
		wg.Add(1)
		go func(p targetPipe) {
			defer wg.Done()
			defer p.reader.Close()

			result, err := c.fileClientPool.PutObject(
				ctx,
				p.target.Location.Address,
				req.ObjectID,
				p.reader,
				req.Size,
			)

			if err != nil {
				results <- writeResult{target: p.target, err: err}
				return
			}

			results <- writeResult{
				target: p.target,
				size:   result.Size,
			}
		}(pipes[i])
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Tee incoming data to all pipes while computing hash
	// Use MultiWriter to write to all pipes simultaneously
	multiWriter := io.MultiWriter(writers...)
	hash := utils.Md5PoolGetHasher()
	defer utils.Md5PoolPutHasher(hash)
	teeReader := io.TeeReader(req.Body, hash)

	var bytesWritten uint64
	buf := make([]byte, 64*1024) // 64KB buffer

	// Copy data from input to all targets
	copyErr := func() error {
		defer func() {
			// Close all pipe writers when done (or on error)
			for _, p := range pipes {
				p.writer.Close()
			}
		}()

		for {
			n, err := teeReader.Read(buf)
			if n > 0 {
				written, writeErr := multiWriter.Write(buf[:n])
				if writeErr != nil {
					return fmt.Errorf("failed to write to targets: %w", writeErr)
				}
				bytesWritten += uint64(written)
			}
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("failed to read input: %w", err)
			}
		}
	}()

	if copyErr != nil {
		// Close all pipes with error
		for _, p := range pipes {
			p.writer.CloseWithError(copyErr)
		}
		return nil, copyErr
	}

	// Collect results
	var successfulTargets []*manager_pb.ReplicationTarget
	var failedTargets []string
	var resultSize uint64

	for result := range results {
		if result.err != nil {
			logger.Warn().
				Err(result.err).
				Str("file_server", result.target.Location.Address).
				Str("object_id", req.ObjectID).
				Msg("failed to write to file server")
			failedTargets = append(failedTargets, result.target.Location.Address)
		} else {
			successfulTargets = append(successfulTargets, result.target)
			resultSize = result.size
		}
	}

	// Check if we have enough successful writes
	minRequired := 1 // At least one must succeed
	if len(successfulTargets) < minRequired {
		return nil, fmt.Errorf("failed to write to enough targets: %d/%d succeeded", len(successfulTargets), numTargets)
	}

	// Build chunk refs from successful targets only
	chunkRefs := make([]types.ChunkRef, 0, len(successfulTargets))
	for _, t := range successfulTargets {
		chunkRefs = append(chunkRefs, types.ChunkRef{
			ChunkID:        types.ChunkID(req.ObjectID),
			Offset:         0,
			Size:           resultSize,
			BackendID:      t.BackendId,
			FileServerAddr: t.Location.Address,
		})
	}

	// Log results
	etag := hex.EncodeToString(hash.Sum(nil))
	logger.Info().
		Str("object_id", req.ObjectID).
		Uint64("bytes", bytesWritten).
		Str("etag", etag).
		Int("successful", len(successfulTargets)).
		Int("failed", len(failedTargets)).
		Msg("WriteObject completed")

	return &WriteResult{
		Size:      resultSize,
		ETag:      etag,
		ChunkRefs: chunkRefs,
	}, nil
}

// ReadObject reads data from file servers with failover.
// It tries each chunk reference until one succeeds.
func (c *Coordinator) ReadObject(ctx context.Context, req *ReadRequest, writer io.Writer) error {
	if len(req.ChunkRefs) == 0 {
		return fmt.Errorf("no chunk refs provided")
	}

	var lastErr error
	for _, chunkRef := range req.ChunkRefs {
		if chunkRef.FileServerAddr == "" {
			continue
		}

		_, err := c.fileClientPool.GetObject(ctx, chunkRef.FileServerAddr, string(chunkRef.ChunkID), func(chunk []byte) error {
			_, writeErr := writer.Write(chunk)
			return writeErr
		})

		if err == nil {
			return nil
		}

		lastErr = err
		logger.Warn().Err(err).
			Str("file_server", chunkRef.FileServerAddr).
			Str("chunk_id", string(chunkRef.ChunkID)).
			Msg("failed to read from file server, trying next")
	}

	if lastErr != nil {
		return fmt.Errorf("failed to read from any file server: %w", lastErr)
	}
	return fmt.Errorf("no file servers available")
}

// ReadObjectRange reads a range of bytes from file servers with failover.
func (c *Coordinator) ReadObjectRange(ctx context.Context, req *ReadRangeRequest, writer io.Writer) error {
	if len(req.ChunkRefs) == 0 {
		return fmt.Errorf("no chunk refs provided")
	}

	var lastErr error
	for _, chunkRef := range req.ChunkRefs {
		if chunkRef.FileServerAddr == "" {
			continue
		}

		_, err := c.fileClientPool.GetObjectRange(ctx, chunkRef.FileServerAddr, string(chunkRef.ChunkID), req.Offset, req.Length, func(chunk []byte) error {
			_, writeErr := writer.Write(chunk)
			return writeErr
		})

		if err == nil {
			return nil
		}

		lastErr = err
		logger.Warn().Err(err).
			Str("file_server", chunkRef.FileServerAddr).
			Str("chunk_id", string(chunkRef.ChunkID)).
			Uint64("offset", req.Offset).
			Uint64("length", req.Length).
			Msg("failed to read range from file server, trying next")
	}

	if lastErr != nil {
		return fmt.Errorf("failed to read range from any file server: %w", lastErr)
	}
	return fmt.Errorf("no file servers available")
}

// ReadObjectToBuffer reads full object data into a buffer.
// This is used for encrypted objects that need full decryption.
func (c *Coordinator) ReadObjectToBuffer(ctx context.Context, chunkRefs []types.ChunkRef) ([]byte, error) {
	if len(chunkRefs) == 0 {
		return nil, fmt.Errorf("no chunk refs provided")
	}

	var lastErr error
	for _, chunkRef := range chunkRefs {
		if chunkRef.FileServerAddr == "" {
			continue
		}

		var data []byte
		_, err := c.fileClientPool.GetObject(ctx, chunkRef.FileServerAddr, string(chunkRef.ChunkID), func(chunk []byte) error {
			data = append(data, chunk...)
			return nil
		})

		if err == nil {
			return data, nil
		}

		lastErr = err
		logger.Warn().Err(err).
			Str("file_server", chunkRef.FileServerAddr).
			Str("chunk_id", string(chunkRef.ChunkID)).
			Msg("failed to read from file server, trying next")
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to read from any file server: %w", lastErr)
	}
	return nil, fmt.Errorf("no file servers available")
}

// CacheStats returns statistics about the target cache.
// Returns nil if caching is disabled.
func (c *Coordinator) CacheStats() *CacheStats {
	if c.targetCache == nil || !c.targetCache.IsEnabled() {
		return nil
	}
	stats := c.targetCache.Stats()
	return &stats
}

// DecrementChunkRefCounts decrements reference counts for chunks on their file servers.
// Returns a list of failed decrements that should be retried.
// Groups chunks by file server address and uses batch calls for efficiency.
// If a taskqueue is configured, failed decrements are automatically queued for retry.
func (c *Coordinator) DecrementChunkRefCounts(ctx context.Context, chunks []types.ChunkRef) []FailedDecrement {
	if len(chunks) == 0 {
		return nil
	}

	// Group chunks by file server address
	byServer := make(map[string][]client.DecrementRefCountRequest)
	for _, chunk := range chunks {
		if chunk.FileServerAddr == "" {
			continue
		}
		byServer[chunk.FileServerAddr] = append(byServer[chunk.FileServerAddr], client.DecrementRefCountRequest{
			ChunkID:          string(chunk.ChunkID),
			ExpectedRefCount: 0, // Don't use CAS for now - just decrement
		})
	}

	var failed []FailedDecrement

	// Process each file server
	for addr, reqs := range byServer {
		results, err := c.fileClientPool.DecrementRefCountBatch(ctx, addr, reqs)
		if err != nil {
			// Entire batch failed - queue all for retry
			logger.Warn().Err(err).
				Str("file_server", addr).
				Int("chunks", len(reqs)).
				Msg("failed to decrement ref counts, queuing for retry")
			for _, req := range reqs {
				failed = append(failed, FailedDecrement{
					ChunkID:        req.ChunkID,
					FileServerAddr: addr,
					Error:          err.Error(),
				})
			}
			continue
		}

		// Check individual results
		for _, r := range results {
			if !r.Success {
				logger.Debug().
					Str("chunk_id", r.ChunkID).
					Str("file_server", addr).
					Str("error", r.Error).
					Msg("chunk ref count decrement failed")
				failed = append(failed, FailedDecrement{
					ChunkID:        r.ChunkID,
					FileServerAddr: addr,
					Error:          r.Error,
				})
			} else {
				logger.Debug().
					Str("chunk_id", r.ChunkID).
					Uint32("new_ref_count", r.NewRefCount).
					Msg("decremented chunk ref count")
			}
		}
	}

	// Queue failed decrements for retry if taskqueue is configured
	if len(failed) > 0 && c.taskQueue != nil {
		for _, f := range failed {
			task, err := handlers.NewGCDecrementTask(f.ChunkID, f.FileServerAddr, 0)
			if err != nil {
				logger.Error().Err(err).
					Str("chunk_id", f.ChunkID).
					Msg("failed to create gc decrement task")
				continue
			}
			if err := c.taskQueue.Enqueue(ctx, task); err != nil {
				logger.Error().Err(err).
					Str("chunk_id", f.ChunkID).
					Msg("failed to enqueue gc decrement task")
			} else {
				logger.Debug().
					Str("chunk_id", f.ChunkID).
					Str("task_id", task.ID).
					Msg("queued gc decrement for retry")
			}
		}
	}

	return failed
}

// FailedDecrement represents a failed ref count decrement that should be retried
type FailedDecrement struct {
	ChunkID        string
	FileServerAddr string
	Error          string
}
