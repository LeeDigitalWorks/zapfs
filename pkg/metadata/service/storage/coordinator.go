// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package storage provides coordination between file servers and the manager
// for object storage operations.
package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/federation"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
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

	// Federation support (optional)
	federationClientPool *federation.ClientPool
}

// CoordinatorConfig holds configuration for the storage coordinator
type CoordinatorConfig struct {
	ManagerClient  client.Manager
	FileClientPool client.File
	Profiles       *types.ProfileSet
	DefaultProfile string
	CacheConfig    *TargetCacheConfig // Optional, uses defaults if nil
	TaskQueue      taskqueue.Queue    // Optional: for queueing failed decrements

	// FederationClientPool is optional. When set, enables dual-write to external S3.
	FederationClientPool *federation.ClientPool
}

// NewCoordinator creates a new storage coordinator
func NewCoordinator(cfg CoordinatorConfig) *Coordinator {
	// Initialize target cache
	cacheConfig := DefaultTargetCacheConfig()
	if cfg.CacheConfig != nil {
		cacheConfig = *cfg.CacheConfig
	}

	return &Coordinator{
		managerClient:        cfg.ManagerClient,
		fileClientPool:       cfg.FileClientPool,
		profiles:             cfg.Profiles,
		defaultProfile:       cfg.DefaultProfile,
		targetCache:          NewTargetCache(cacheConfig),
		taskQueue:            cfg.TaskQueue,
		federationClientPool: cfg.FederationClientPool,
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
	target *manager_pb.ReplicationTarget
	size   uint64
	chunks []client.ChunkInfo // Actual chunk IDs from file server
	err    error
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
	// Pass compression algorithm from the storage profile
	return c.writeToAllTargets(ctx, req, targetList, profile.Compression)
}

// writeToAllTargets streams data to all targets in parallel.
// Uses io.Pipe to create independent streams for each target.
// The compression parameter specifies the algorithm to use (e.g., "lz4", "zstd", "snappy", or "" for none).
func (c *Coordinator) writeToAllTargets(ctx context.Context, req *WriteRequest, targets []*manager_pb.ReplicationTarget, compression string) (*WriteResult, error) {
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

			result, err := c.fileClientPool.PutObjectWithCompression(
				ctx,
				p.target.Location.Address,
				req.ObjectID,
				p.reader,
				req.Size,
				compression,
			)

			if err != nil {
				results <- writeResult{target: p.target, err: err}
				return
			}

			results <- writeResult{
				target: p.target,
				size:   result.Size,
				chunks: result.Chunks,
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
	type successResult struct {
		target *manager_pb.ReplicationTarget
		size   uint64
		chunks []client.ChunkInfo
	}
	var successfulResults []successResult
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
			successfulResults = append(successfulResults, successResult{
				target: result.target,
				size:   result.size,
				chunks: result.chunks,
			})
			resultSize = result.size
		}
	}

	// Check if we have enough successful writes
	minRequired := 1 // At least one must succeed
	if len(successfulResults) < minRequired {
		return nil, fmt.Errorf("failed to write to enough targets: %d/%d succeeded, failed: %v", len(successfulResults), numTargets, failedTargets)
	}

	// Build chunk refs from successful targets using actual chunk IDs from file servers
	chunkRefs := make([]types.ChunkRef, 0)
	for _, sr := range successfulResults {
		for _, chunk := range sr.chunks {
			chunkRefs = append(chunkRefs, types.ChunkRef{
				ChunkID:        types.ChunkID(chunk.ChunkID),
				Offset:         chunk.Offset,
				Size:           chunk.Size,
				OriginalSize:   chunk.OriginalSize,
				Compression:    chunk.Compression,
				BackendID:      sr.target.BackendId,
				FileServerAddr: sr.target.Location.Address,
			})
		}
	}

	// Log results
	etag := hex.EncodeToString(hash.Sum(nil))

	return &WriteResult{
		Size:      resultSize,
		ETag:      etag,
		ChunkRefs: chunkRefs,
	}, nil
}

// ReadObject reads data from file servers with failover.
// It tries each chunk reference until one succeeds.
// Returns nil for empty objects (no chunks to read).
func (c *Coordinator) ReadObject(ctx context.Context, req *ReadRequest, writer io.Writer) error {
	// Empty object (size=0) has no chunks - nothing to read
	if len(req.ChunkRefs) == 0 {
		return nil
	}

	// Group chunk refs by ChunkID (replicas have the same ChunkID)
	// For multipart objects, different parts have different ChunkIDs
	chunkGroups := groupChunksByID(req.ChunkRefs)

	// Read each unique chunk in offset order and write to output
	for _, group := range chunkGroups {
		if err := c.readChunkGroup(ctx, group, writer); err != nil {
			logger.Error().Err(err).
				Str("chunk_id", string(group.chunkID)).
				Msg("ReadObject: failed to read chunk group")
			return err
		}
	}

	return nil
}

// chunkGroup represents a set of replicas for a single chunk
type chunkGroup struct {
	chunkID  types.ChunkID
	offset   uint64
	replicas []types.ChunkRef
}

// groupChunksByID groups chunk refs by ChunkID and sorts by offset.
// Each group represents replicas of the same chunk data.
func groupChunksByID(refs []types.ChunkRef) []chunkGroup {
	// Build map of ChunkID -> replicas
	groupMap := make(map[types.ChunkID]*chunkGroup)
	for _, ref := range refs {
		if ref.FileServerAddr == "" {
			continue
		}
		g, exists := groupMap[ref.ChunkID]
		if !exists {
			g = &chunkGroup{
				chunkID:  ref.ChunkID,
				offset:   ref.Offset,
				replicas: make([]types.ChunkRef, 0, 1),
			}
			groupMap[ref.ChunkID] = g
		}
		g.replicas = append(g.replicas, ref)
	}

	// Convert map to slice and sort by offset
	groups := make([]chunkGroup, 0, len(groupMap))
	for _, g := range groupMap {
		groups = append(groups, *g)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].offset < groups[j].offset
	})

	return groups
}

// readChunkGroup reads data from one of the replicas in the group (with failover)
func (c *Coordinator) readChunkGroup(ctx context.Context, group chunkGroup, writer io.Writer) error {
	var lastErr error
	for _, replica := range group.replicas {
		// Use GetChunk to read by chunk ID (SHA-256 content hash)
		err := c.fileClientPool.GetChunk(ctx, replica.FileServerAddr, string(replica.ChunkID), func(chunk []byte) error {
			_, writeErr := writer.Write(chunk)
			return writeErr
		})

		if err == nil {
			return nil
		}

		lastErr = err
		logger.Warn().Err(err).
			Str("file_server", replica.FileServerAddr).
			Str("chunk_id", string(replica.ChunkID)).
			Msg("failed to read from file server, trying next replica")
	}

	if lastErr != nil {
		return fmt.Errorf("failed to read chunk %s from any replica: %w", group.chunkID, lastErr)
	}
	return fmt.Errorf("no replicas available for chunk %s", group.chunkID)
}

// ReadObjectRange reads a range of bytes from file servers with failover.
// For multi-chunk objects (e.g., multipart uploads), it reads from all chunks
// that overlap with the requested range and concatenates the results.
// Returns nil for empty objects (no chunks to read).
func (c *Coordinator) ReadObjectRange(ctx context.Context, req *ReadRangeRequest, writer io.Writer) error {
	// Empty object (size=0) has no chunks - nothing to read
	if len(req.ChunkRefs) == 0 {
		return nil
	}

	// Group chunk refs by ChunkID and sort by offset
	chunkGroups := groupChunksByID(req.ChunkRefs)

	// Calculate the requested range end
	rangeStart := req.Offset
	rangeEnd := req.Offset + req.Length

	// Read from each chunk that overlaps with the requested range
	for _, group := range chunkGroups {
		chunkStart := group.offset
		chunkEnd := group.offset + group.replicas[0].Size

		// Check if this chunk overlaps with requested range
		if chunkEnd <= rangeStart || chunkStart >= rangeEnd {
			continue // No overlap
		}

		// Calculate the overlapping region
		overlapStart := max(chunkStart, rangeStart)
		overlapEnd := min(chunkEnd, rangeEnd)

		// Calculate local offset and length within this chunk
		localOffset := overlapStart - chunkStart
		localLength := overlapEnd - overlapStart

		// Read the overlapping portion from this chunk
		if err := c.readChunkGroupRange(ctx, group, localOffset, localLength, writer); err != nil {
			return err
		}
	}

	return nil
}

// readChunkGroupRange reads a range from one of the replicas in the group (with failover)
func (c *Coordinator) readChunkGroupRange(ctx context.Context, group chunkGroup, offset, length uint64, writer io.Writer) error {
	var lastErr error
	for _, replica := range group.replicas {
		// Use GetChunkRange to read by chunk ID (SHA-256 content hash)
		err := c.fileClientPool.GetChunkRange(ctx, replica.FileServerAddr, string(replica.ChunkID), offset, length, func(chunk []byte) error {
			_, writeErr := writer.Write(chunk)
			return writeErr
		})

		if err == nil {
			return nil
		}

		lastErr = err
		logger.Warn().Err(err).
			Str("file_server", replica.FileServerAddr).
			Str("chunk_id", string(replica.ChunkID)).
			Uint64("offset", offset).
			Uint64("length", length).
			Msg("failed to read range from file server, trying next replica")
	}

	if lastErr != nil {
		return fmt.Errorf("failed to read chunk %s range from any replica: %w", group.chunkID, lastErr)
	}
	return fmt.Errorf("no replicas available for chunk %s", group.chunkID)
}

// ReadObjectToBuffer reads full object data into a buffer.
// This is used for encrypted objects that need full decryption.
// For multi-chunk objects, it reads all chunks and concatenates them.
// Returns empty byte slice for empty objects (no chunks to read).
func (c *Coordinator) ReadObjectToBuffer(ctx context.Context, chunkRefs []types.ChunkRef) ([]byte, error) {
	// Empty object (size=0) has no chunks - return empty slice
	if len(chunkRefs) == 0 {
		return []byte{}, nil
	}

	// Group chunk refs by ChunkID and sort by offset
	chunkGroups := groupChunksByID(chunkRefs)

	// Read each unique chunk in order
	var result []byte
	for _, group := range chunkGroups {
		data, err := c.readChunkGroupToBuffer(ctx, group)
		if err != nil {
			return nil, err
		}
		result = append(result, data...)
	}

	return result, nil
}

// readChunkGroupToBuffer reads data from one of the replicas in the group into a buffer
func (c *Coordinator) readChunkGroupToBuffer(ctx context.Context, group chunkGroup) ([]byte, error) {
	var lastErr error
	for _, replica := range group.replicas {
		var data []byte
		// Use GetChunk to read by chunk ID (SHA-256 content hash)
		err := c.fileClientPool.GetChunk(ctx, replica.FileServerAddr, string(replica.ChunkID), func(chunk []byte) error {
			data = append(data, chunk...)
			return nil
		})

		if err == nil {
			return data, nil
		}

		lastErr = err
		logger.Warn().Err(err).
			Str("file_server", replica.FileServerAddr).
			Str("chunk_id", string(replica.ChunkID)).
			Msg("failed to read from file server, trying next replica")
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to read chunk %s from any replica: %w", group.chunkID, lastErr)
	}
	return nil, fmt.Errorf("no replicas available for chunk %s", group.chunkID)
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

// WriteWithFederation writes to both local storage and external S3 simultaneously.
// This is used for dual-write when a bucket is in MIGRATING mode with dual-write enabled.
// Local write must succeed; external write failure is logged but not fatal.
//
// The method uses io.MultiWriter to tee data to both destinations:
//   - Local: streams to file servers via the standard WriteObject path
//   - External: streams to external S3 via the federation client pool
//
// Returns FederatedWriteResult which includes both local and external results.
func (c *Coordinator) WriteWithFederation(ctx context.Context, req *FederatedWriteRequest) (*FederatedWriteResult, error) {
	if c.federationClientPool == nil {
		return nil, fmt.Errorf("federation client pool not configured")
	}

	// Build external S3 config
	extConfig := &federation.ExternalS3Config{
		Endpoint:        req.ExternalEndpoint,
		Region:          req.ExternalRegion,
		AccessKeyID:     req.ExternalAccessKeyID,
		SecretAccessKey: req.ExternalSecretAccessKey,
		Bucket:          req.ExternalBucket,
		PathStyle:       req.ExternalPathStyle,
	}

	// Create external writer and start async upload
	extWriter := NewExternalWriter(c.federationClientPool)
	extReq := &ExternalWriteRequest{
		Config:      extConfig,
		Key:         req.Key,
		Size:        int64(req.Size),
		ContentType: req.ContentType,
	}
	extPipeWriter, extResultCh := extWriter.WriteAsync(ctx, extReq)

	// Create a pipe for local storage
	localPR, localPW := io.Pipe()

	// Use MultiWriter to tee data to both local and external
	multiWriter := io.MultiWriter(localPW, extPipeWriter)

	// Track results
	var wg sync.WaitGroup
	var localResult *WriteResult
	var localErr error

	// Start local write in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer localPR.Close()

		localResult, localErr = c.WriteObject(ctx, &WriteRequest{
			Bucket:      req.Bucket,
			ObjectID:    req.ObjectID,
			Body:        localPR,
			Size:        req.Size,
			ProfileName: req.ProfileName,
			Replication: req.Replication,
		})
	}()

	// Copy input to both destinations
	buf := make([]byte, 64*1024) // 64KB buffer
	copyErr := func() error {
		defer localPW.Close()
		defer extPipeWriter.Close()

		for {
			n, err := req.Body.Read(buf)
			if n > 0 {
				if _, writeErr := multiWriter.Write(buf[:n]); writeErr != nil {
					return fmt.Errorf("multi-write: %w", writeErr)
				}
			}
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("read input: %w", err)
			}
		}
	}()

	// Wait for local write to complete
	wg.Wait()

	// Wait for external write result
	extResult := <-extResultCh

	// If copy failed, return error
	if copyErr != nil {
		return nil, copyErr
	}

	// If local write failed, return error (this is required to succeed)
	if localErr != nil {
		return nil, localErr
	}

	// Build result
	result := &FederatedWriteResult{
		WriteResult: *localResult,
	}

	// External write is optional - log errors but don't fail
	if extResult.Err != nil {
		result.ExternalError = extResult.Err
		logger.Warn().
			Err(extResult.Err).
			Str("bucket", req.Bucket).
			Str("key", req.Key).
			Str("external_bucket", req.ExternalBucket).
			Msg("External S3 write failed during dual-write (local succeeded)")
	} else {
		result.ExternalETag = extResult.ETag
		result.ExternalVersionID = extResult.VersionID
	}

	return result, nil
}

// HasFederationSupport returns true if the coordinator has federation enabled.
func (c *Coordinator) HasFederationSupport() bool {
	return c.federationClientPool != nil
}
