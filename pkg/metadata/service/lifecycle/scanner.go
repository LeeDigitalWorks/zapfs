// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package lifecycle provides lifecycle policy scanning and execution.
//
// The scanner runs periodically to find objects matching lifecycle rules
// and enqueues tasks for execution. The handler processes those tasks
// to perform the actual delete/transition operations.
//
// Community edition supports:
// - Expiration (delete current version)
// - Noncurrent version expiration
// - Abort incomplete multipart uploads
// - Delete marker cleanup
//
// Enterprise edition adds:
// - Storage class transitions
// - Noncurrent version transitions
// - Lifecycle event logging
package lifecycle

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"

	"github.com/google/uuid"
)

// Config configures the lifecycle scanner
type Config struct {
	// How often to run scan loop (default: 1h)
	ScanInterval time.Duration

	// Objects per batch when listing (default: 1000)
	BatchSize int

	// Parallel bucket processing (default: 5)
	Concurrency int

	// Max tasks enqueued per scan run (default: 10000)
	// Prevents flooding the task queue
	MaxTasksPerScan int

	// Skip buckets scanned within this duration (default: 30m)
	MinScanAge time.Duration

	// Enabled flag
	Enabled bool
}

// DefaultConfig returns the default scanner configuration
func DefaultConfig() Config {
	return Config{
		ScanInterval:    time.Hour,
		BatchSize:       1000,
		Concurrency:     5,
		MaxTasksPerScan: 10000,
		MinScanAge:      30 * time.Minute,
		Enabled:         true,
	}
}

// Scanner scans buckets for lifecycle actions
type Scanner struct {
	db     db.DB
	queue  taskqueue.Queue
	config Config

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewScanner creates a new lifecycle scanner
func NewScanner(database db.DB, queue taskqueue.Queue, config Config) *Scanner {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scanner{
		db:     database,
		queue:  queue,
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins the scanner loop
func (s *Scanner) Start() {
	if !s.config.Enabled {
		logger.Info().Msg("Lifecycle scanner disabled")
		return
	}

	s.wg.Add(1)
	go s.scanLoop()

	logger.Info().
		Dur("interval", s.config.ScanInterval).
		Int("concurrency", s.config.Concurrency).
		Int("batch_size", s.config.BatchSize).
		Msg("Started lifecycle scanner")
}

// Stop stops the scanner and waits for completion
func (s *Scanner) Stop() {
	s.cancel()
	s.wg.Wait()
	logger.Info().Msg("Stopped lifecycle scanner")
}

// scanLoop runs the periodic scan
func (s *Scanner) scanLoop() {
	defer s.wg.Done()

	// Initial scan on startup (after jittered delay for service stabilization)
	// Jitter prevents multiple instances from scanning simultaneously on startup
	initialDelay := utils.Jitter(10*time.Second, 0.5)
	select {
	case <-time.After(initialDelay):
		s.runScan()
	case <-s.ctx.Done():
		return
	}

	// Use jittered ticker to prevent thundering herd across instances
	// 10% jitter on 1h interval = ±6 minutes
	tickCh, stopTicker := utils.JitteredTicker(s.config.ScanInterval, 0.1)
	defer stopTicker()

	for {
		select {
		case <-tickCh:
			s.runScan()
		case <-s.ctx.Done():
			return
		}
	}
}

// runScan performs one full scan cycle
func (s *Scanner) runScan() {
	scansTotal.Inc()

	// Get buckets that need scanning
	buckets, err := s.db.GetBucketsNeedingScan(s.ctx, s.config.MinScanAge, 100)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get buckets needing scan")
		scanErrors.Inc()
		return
	}

	if len(buckets) == 0 {
		logger.Debug().Msg("No buckets need lifecycle scanning")
		return
	}

	// Update metric for buckets with lifecycle configuration
	bucketsScanned.Set(float64(len(buckets)))

	logger.Info().Int("buckets", len(buckets)).Msg("Starting lifecycle scan")

	// Process buckets with limited concurrency
	sem := make(chan struct{}, s.config.Concurrency)
	var wg sync.WaitGroup
	var totalEnqueued int
	var mu sync.Mutex

	for _, bucket := range buckets {
		// Check if we've hit the max tasks limit
		mu.Lock()
		if totalEnqueued >= s.config.MaxTasksPerScan {
			mu.Unlock()
			logger.Info().Int("enqueued", totalEnqueued).Msg("Hit max tasks per scan, stopping")
			break
		}
		mu.Unlock()

		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			return
		}

		wg.Add(1)
		go func(b string) {
			defer wg.Done()
			defer func() { <-sem }()

			enqueued, err := s.scanBucket(b)
			if err != nil {
				logger.Error().Err(err).Str("bucket", b).Msg("Failed to scan bucket")
				scanErrors.Inc()
				return
			}

			mu.Lock()
			totalEnqueued += enqueued
			mu.Unlock()
		}(bucket)
	}

	wg.Wait()
	logger.Info().Int("enqueued", totalEnqueued).Msg("Completed lifecycle scan")
}

// scanBucket scans a single bucket for lifecycle actions
func (s *Scanner) scanBucket(bucket string) (int, error) {
	startTime := time.Now()
	defer func() {
		scanDuration.Observe(time.Since(startTime).Seconds())
	}()

	// Get lifecycle configuration
	lifecycle, err := s.db.GetBucketLifecycle(s.ctx, bucket)
	if err != nil {
		if errors.Is(err, db.ErrLifecycleNotFound) {
			// Lifecycle was deleted, reset scan state
			s.db.ResetScanState(s.ctx, bucket)
			return 0, nil
		}
		return 0, err
	}

	// Create evaluator
	evaluator := s3types.NewEvaluator(lifecycle)
	now := time.Now().UTC()

	// Get or create scan state
	state, err := s.db.GetScanState(s.ctx, bucket)
	if err != nil {
		if errors.Is(err, db.ErrLifecycleScanStateNotFound) {
			state = &db.LifecycleScanState{Bucket: bucket}
		} else {
			return 0, err
		}
	}

	// Mark scan as started
	state.ScanStartedAt = now
	state.ObjectsScanned = 0
	state.ActionsEnqueued = 0
	state.LastError = ""
	if err := s.db.UpdateScanState(s.ctx, state); err != nil {
		return 0, err
	}

	// Scan objects in batches
	var totalEnqueued int
	marker := state.LastKey

	for {
		// Check context
		select {
		case <-s.ctx.Done():
			return totalEnqueued, s.ctx.Err()
		default:
		}

		// List objects
		objects, nextMarker, err := s.listObjects(bucket, marker, s.config.BatchSize)
		if err != nil {
			state.LastError = err.Error()
			state.ConsecutiveErrors++
			s.db.UpdateScanState(s.ctx, state)
			return totalEnqueued, err
		}

		// Evaluate each object
		for _, obj := range objects {
			objectsEvaluated.Inc()
			state.ObjectsScanned++

			event := evaluator.Eval(obj, now)
			if event.Action == s3types.NoneAction {
				continue
			}

			// Enqueue task
			if err := s.enqueueAction(bucket, event, obj); err != nil {
				logger.Warn().Err(err).
					Str("bucket", bucket).
					Str("key", obj.Name).
					Msg("Failed to enqueue lifecycle action")
				continue
			}

			totalEnqueued++
			state.ActionsEnqueued++
			actionsEnqueued.WithLabelValues(event.Action.String()).Inc()
		}

		// Checkpoint progress
		state.LastKey = marker
		if err := s.db.UpdateScanState(s.ctx, state); err != nil {
			logger.Warn().Err(err).Str("bucket", bucket).Msg("Failed to checkpoint scan state")
		}

		// Check if done
		if nextMarker == "" || len(objects) < s.config.BatchSize {
			break
		}
		marker = nextMarker
	}

	// Mark scan complete
	state.ScanCompletedAt = time.Now()
	state.LastKey = "" // Reset for next full scan
	state.ConsecutiveErrors = 0
	if err := s.db.UpdateScanState(s.ctx, state); err != nil {
		return totalEnqueued, err
	}

	logger.Info().
		Str("bucket", bucket).
		Int("scanned", state.ObjectsScanned).
		Int("enqueued", state.ActionsEnqueued).
		Dur("duration", time.Since(startTime)).
		Msg("Completed bucket lifecycle scan")

	return totalEnqueued, nil
}

// listObjects lists objects from the bucket with pagination
func (s *Scanner) listObjects(bucket, marker string, limit int) ([]s3types.ObjectState, string, error) {
	// List objects from database
	objects, err := s.db.ListObjects(s.ctx, bucket, marker, limit)
	if err != nil {
		return nil, "", err
	}

	// Convert to ObjectState for evaluation
	states := make([]s3types.ObjectState, 0, len(objects))
	for _, obj := range objects {
		state := s3types.ObjectState{
			Name:     obj.Key,
			Size:     int64(obj.Size),
			ModTime:  time.Unix(0, obj.CreatedAt),
			IsLatest: true, // TODO: Handle versioning
			// TODO: Load tags for tag-based rules
		}
		states = append(states, state)
	}

	var nextMarker string
	if len(objects) == limit && len(objects) > 0 {
		nextMarker = objects[len(objects)-1].Key
	}

	return states, nextMarker, nil
}

// enqueueAction creates a task for the lifecycle action
func (s *Scanner) enqueueAction(bucket string, event s3types.Event, obj s3types.ObjectState) error {
	action := actionFromEvent(event.Action)

	payload := taskqueue.LifecyclePayload{
		Bucket:          bucket,
		Key:             obj.Name,
		VersionID:       obj.VersionID,
		Action:          action,
		StorageClass:    event.StorageClass,
		RuleID:          event.RuleID,
		EvaluatedAt:     time.Now().UnixNano(),
		ExpectedModTime: obj.ModTime.UnixNano(),
	}

	payloadBytes, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		return err
	}

	task := &taskqueue.Task{
		ID:         uuid.New().String(),
		Type:       taskqueue.TaskTypeLifecycle,
		Status:     taskqueue.StatusPending,
		Priority:   taskqueue.PriorityNormal,
		Payload:    payloadBytes,
		MaxRetries: 3,
	}

	return s.queue.Enqueue(s.ctx, task)
}

// actionFromEvent converts s3types.Action to string action
func actionFromEvent(action s3types.Action) string {
	switch action {
	case s3types.DeleteAction:
		return taskqueue.LifecycleActionDelete
	case s3types.DeleteVersionAction:
		return taskqueue.LifecycleActionDeleteVersion
	case s3types.TransitionAction, s3types.TransitionVersionAction:
		return taskqueue.LifecycleActionTransition
	case s3types.AbortMultipartUploadAction:
		return taskqueue.LifecycleActionAbortMPU
	default:
		return ""
	}
}
