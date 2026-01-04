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
	"net/url"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"

	"github.com/google/uuid"
)

// Intelligent Tiering thresholds (per AWS spec)
const (
	// IntelligentTieringFrequentThreshold is the number of days before moving
	// from Frequent Access to Infrequent Access tier (30 days).
	IntelligentTieringFrequentThreshold = 30 * 24 * time.Hour

	// IntelligentTieringInfrequentThreshold is the number of days before moving
	// from Infrequent Access to Archive Instant Access tier (90 days).
	IntelligentTieringInfrequentThreshold = 90 * 24 * time.Hour

	// IntelligentTieringMinSize is the minimum object size for intelligent tiering.
	// AWS uses 128KB - objects smaller than this are always in Frequent Access.
	IntelligentTieringMinSize = 128 * 1024 // 128KB
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

	// IntelligentTieringEnabled enables automatic demotion of INTELLIGENT_TIERING objects
	IntelligentTieringEnabled bool
}

// DefaultConfig returns the default scanner configuration
func DefaultConfig() Config {
	return Config{
		ScanInterval:              time.Hour,
		BatchSize:                 1000,
		Concurrency:               5,
		MaxTasksPerScan:           10000,
		MinScanAge:                30 * time.Minute,
		Enabled:                   true,
		IntelligentTieringEnabled: true,
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

	// Also scan for intelligent tiering demotions
	if s.config.IntelligentTieringEnabled {
		itEnqueued, err := s.scanIntelligentTiering()
		if err != nil {
			logger.Error().Err(err).Msg("Failed to scan intelligent tiering objects")
		} else if itEnqueued > 0 {
			logger.Info().Int("enqueued", itEnqueued).Msg("Completed intelligent tiering scan")
		}
	}
}

// scanIntelligentTiering scans for cold INTELLIGENT_TIERING objects and enqueues
// transition tasks to move them to lower-cost tiers.
func (s *Scanner) scanIntelligentTiering() (int, error) {
	now := time.Now()
	var totalEnqueued int

	// Scan for objects that haven't been accessed in 30 days (-> Infrequent Access)
	threshold30 := now.Add(-IntelligentTieringFrequentThreshold).UnixNano()
	objects30, err := s.db.GetColdIntelligentTieringObjects(s.ctx, threshold30, IntelligentTieringMinSize, s.config.BatchSize)
	if err != nil {
		return 0, err
	}

	for _, obj := range objects30 {
		// Skip if already in a lower tier (we track this via profile_id or separate field)
		// For now, we transition to STANDARD_IA as the first tier transition
		if err := s.enqueueIntelligentTieringTransition(obj, "STANDARD_IA"); err != nil {
			logger.Warn().Err(err).
				Str("bucket", obj.Bucket).
				Str("key", obj.Key).
				Msg("Failed to enqueue intelligent tiering transition")
			continue
		}
		totalEnqueued++
	}

	// Scan for objects that haven't been accessed in 90 days (-> Archive Instant Access)
	// These should have already been transitioned to STANDARD_IA, but if not, catch them here
	threshold90 := now.Add(-IntelligentTieringInfrequentThreshold).UnixNano()
	objects90, err := s.db.GetColdIntelligentTieringObjects(s.ctx, threshold90, IntelligentTieringMinSize, s.config.BatchSize)
	if err != nil {
		return totalEnqueued, err
	}

	for _, obj := range objects90 {
		// For 90+ day cold objects, transition to GLACIER_IR (Glacier Instant Retrieval)
		if err := s.enqueueIntelligentTieringTransition(obj, "GLACIER_IR"); err != nil {
			logger.Warn().Err(err).
				Str("bucket", obj.Bucket).
				Str("key", obj.Key).
				Msg("Failed to enqueue intelligent tiering transition")
			continue
		}
		totalEnqueued++
	}

	return totalEnqueued, nil
}

// enqueueIntelligentTieringTransition creates a transition task for intelligent tiering
func (s *Scanner) enqueueIntelligentTieringTransition(obj *types.ObjectRef, targetClass string) error {
	payload := taskqueue.LifecyclePayload{
		Bucket:          obj.Bucket,
		Key:             obj.Key,
		VersionID:       obj.ID.String(), // Use object ID as version identifier
		Action:          taskqueue.LifecycleActionTransition,
		StorageClass:    targetClass,
		RuleID:          "intelligent-tiering-auto",
		EvaluatedAt:     time.Now().UnixNano(),
		ExpectedModTime: obj.CreatedAt,
	}

	payloadBytes, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		return err
	}

	task := &taskqueue.Task{
		ID:         uuid.New().String(),
		Type:       taskqueue.TaskTypeLifecycle,
		Status:     taskqueue.StatusPending,
		Priority:   taskqueue.PriorityLow, // Lower priority than explicit lifecycle rules
		Payload:    payloadBytes,
		MaxRetries: 3,
	}

	return s.queue.Enqueue(s.ctx, task)
}

// scanBucket scans a single bucket for lifecycle actions
func (s *Scanner) scanBucket(bucket string) (int, error) {
	startTime := time.Now()
	defer func() {
		scanDuration.Observe(time.Since(startTime).Seconds())
	}()

	// Skip federated buckets - delegate lifecycle to upstream external S3
	// Check if bucket has a federation config (indicates bucket is in passthrough/migrating mode)
	_, fedErr := s.db.GetFederationConfig(s.ctx, bucket)
	if fedErr == nil {
		// Federation config exists - bucket is federated, skip lifecycle processing
		logger.Debug().
			Str("bucket", bucket).
			Msg("Skipping federated bucket for lifecycle scan - defer to external S3")
		return 0, nil
	}
	// If error is "not found", bucket is not federated - continue with scan
	// For other errors, log and continue (don't block lifecycle on federation lookup failures)
	if fedErr != nil && !errors.Is(fedErr, db.ErrFederationNotFound) {
		logger.Warn().Err(fedErr).Str("bucket", bucket).
			Msg("Failed to check federation config, proceeding with lifecycle scan")
	}

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

	// Check if we need to load tags for tag-based rules
	needsTags := lifecycleHasTagFilter(lifecycle)

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
		for i := range objects {
			obj := &objects[i]
			objectsEvaluated.Inc()
			state.ObjectsScanned++

			// Load tags if needed for tag-based rules
			if needsTags && obj.UserTags == "" {
				tagSet, err := s.db.GetObjectTagging(s.ctx, bucket, obj.Name)
				if err == nil && tagSet != nil {
					obj.UserTags = tagSetToQueryString(tagSet)
				}
				// Ignore tag loading errors - object may not have tags
			}

			event := evaluator.Eval(*obj, now)
			if event.Action == s3types.NoneAction {
				continue
			}

			// Enqueue task
			if err := s.enqueueAction(bucket, event, *obj); err != nil {
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
// Uses ListObjectVersions to properly handle versioned buckets and get
// accurate IsLatest and DeleteMarker information.
func (s *Scanner) listObjects(bucket, marker string, limit int) ([]s3types.ObjectState, string, error) {
	// Use ListObjectVersions to get proper version information including IsLatest
	// This works correctly for both versioned and non-versioned buckets.
	// For non-versioned buckets, each object has a single version with IsLatest=true.
	versions, _, nextKeyMarker, _, err := s.db.ListObjectVersions(
		s.ctx,
		bucket,
		"",     // prefix - scan all objects
		marker, // keyMarker
		"",     // versionIDMarker - not used for lifecycle scanning
		"",     // delimiter - no hierarchical listing needed
		limit,
	)
	if err != nil {
		return nil, "", err
	}

	// Convert to ObjectState for evaluation
	// Note: Tags are loaded lazily in scanBucket when needed for tag-based rules
	states := make([]s3types.ObjectState, 0, len(versions))
	for _, v := range versions {
		state := s3types.ObjectState{
			Name:         v.Key,
			Size:         v.Size,
			ModTime:      time.Unix(0, v.LastModified),
			IsLatest:     v.IsLatest,
			DeleteMarker: v.IsDeleteMarker,
			VersionID:    v.VersionID,
		}
		states = append(states, state)
	}

	return states, nextKeyMarker, nil
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

// lifecycleHasTagFilter checks if any rule in the lifecycle has a tag filter.
// This is used to optimize scanning - we only load tags when needed.
func lifecycleHasTagFilter(lc *s3types.Lifecycle) bool {
	if lc == nil {
		return false
	}
	for _, rule := range lc.Rules {
		if rule.Status != s3types.LifecycleStatusEnabled {
			continue
		}
		if rule.Filter != nil {
			if rule.Filter.Tag != nil {
				return true
			}
			if rule.Filter.And != nil && len(rule.Filter.And.Tags) > 0 {
				return true
			}
		}
	}
	return false
}

// tagSetToQueryString converts a TagSet to URL query string format.
// e.g., []Tag{{Key:"env", Value:"prod"}, {Key:"team", Value:"eng"}} -> "env=prod&team=eng"
func tagSetToQueryString(tagSet *s3types.TagSet) string {
	if tagSet == nil || len(tagSet.Tags) == 0 {
		return ""
	}
	values := url.Values{}
	for _, tag := range tagSet.Tags {
		values.Set(tag.Key, tag.Value)
	}
	return values.Encode()
}
