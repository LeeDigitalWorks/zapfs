//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package usage

import (
	"context"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	"github.com/rs/zerolog/log"
)

// enterpriseCollector implements Collector with buffered writes and streaming.
type enterpriseCollector struct {
	cfg   Config
	store Store

	// Buffer for pending events
	mu     sync.Mutex
	buffer []UsageEvent

	// Background processing
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Streaming support
	streamMu    sync.RWMutex
	subscribers map[string][]chan UsageEvent // ownerID -> subscribers
}

// NewCollector creates a new enterprise usage collector.
func NewCollector(cfg Config, store Store) Collector {
	cfg.Validate()
	return &enterpriseCollector{
		cfg:         cfg,
		store:       store,
		buffer:      make([]UsageEvent, 0, cfg.FlushSize),
		subscribers: make(map[string][]chan UsageEvent),
	}
}

// RecordRequest records an API request event.
func (c *enterpriseCollector) RecordRequest(ownerID, bucket, operation string) {
	if !c.isEnabled() {
		return
	}

	event := UsageEvent{
		EventTime: time.Now().UTC(),
		OwnerID:   ownerID,
		Bucket:    bucket,
		EventType: EventTypeRequest,
		Operation: operation,
	}

	c.addEvent(event)
}

// RecordBandwidth records data transfer.
func (c *enterpriseCollector) RecordBandwidth(ownerID, bucket string, bytes int64, direction Direction) {
	if !c.isEnabled() {
		return
	}

	event := UsageEvent{
		EventTime:  time.Now().UTC(),
		OwnerID:    ownerID,
		Bucket:     bucket,
		EventType:  EventTypeBandwidth,
		BytesDelta: bytes,
		Direction:  direction,
	}

	c.addEvent(event)
}

// RecordStorageDelta records a change in storage.
func (c *enterpriseCollector) RecordStorageDelta(ownerID, bucket string, bytesDelta int64, objectDelta int, storageClass string) {
	if !c.isEnabled() {
		return
	}

	now := time.Now().UTC()

	// Record storage delta
	if bytesDelta != 0 {
		event := UsageEvent{
			EventTime:    now,
			OwnerID:      ownerID,
			Bucket:       bucket,
			EventType:    EventTypeStorageDelta,
			BytesDelta:   bytesDelta,
			StorageClass: storageClass,
		}
		c.addEvent(event)
	}

	// Record object count delta
	if objectDelta != 0 {
		event := UsageEvent{
			EventTime:  now,
			OwnerID:    ownerID,
			Bucket:     bucket,
			EventType:  EventTypeObjectDelta,
			CountDelta: objectDelta,
		}
		c.addEvent(event)
	}
}

// Start begins background processing.
func (c *enterpriseCollector) Start(ctx context.Context) {
	c.ctx, c.cancel = context.WithCancel(ctx)

	c.wg.Add(1)
	go c.flushLoop()

	log.Info().
		Int("flush_size", c.cfg.FlushSize).
		Dur("flush_interval", c.cfg.FlushInterval).
		Int("retention_days", c.cfg.RetentionDays).
		Msg("usage collector started")
}

// Stop gracefully shuts down the collector.
func (c *enterpriseCollector) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()

	// Final flush
	c.flush()

	log.Info().Msg("usage collector stopped")
}

// Subscribe registers a channel to receive events for an owner.
// Used for streaming real-time updates.
func (c *enterpriseCollector) Subscribe(ownerID string, ch chan UsageEvent) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	c.subscribers[ownerID] = append(c.subscribers[ownerID], ch)
}

// Unsubscribe removes a channel from receiving events.
func (c *enterpriseCollector) Unsubscribe(ownerID string, ch chan UsageEvent) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	subs := c.subscribers[ownerID]
	for i, sub := range subs {
		if sub == ch {
			c.subscribers[ownerID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
}

// isEnabled checks if usage reporting is enabled via license.
func (c *enterpriseCollector) isEnabled() bool {
	if !c.cfg.Enabled {
		return false
	}

	mgr := license.GetManager()
	if mgr == nil {
		return false
	}

	// Check for either AdvancedMetrics or MultiTenancy feature
	return mgr.CheckFeature(license.FeatureAdvancedMetrics) == nil ||
		mgr.CheckFeature(license.FeatureMultiTenancy) == nil
}

// addEvent adds an event to the buffer and notifies subscribers.
func (c *enterpriseCollector) addEvent(event UsageEvent) {
	// Notify streaming subscribers (non-blocking)
	c.notifySubscribers(event)

	// Add to buffer
	c.mu.Lock()
	c.buffer = append(c.buffer, event)
	shouldFlush := len(c.buffer) >= c.cfg.FlushSize
	c.mu.Unlock()

	if shouldFlush {
		go c.flush()
	}
}

// notifySubscribers sends the event to all subscribers for the owner.
func (c *enterpriseCollector) notifySubscribers(event UsageEvent) {
	c.streamMu.RLock()
	subs := c.subscribers[event.OwnerID]
	c.streamMu.RUnlock()

	for _, ch := range subs {
		select {
		case ch <- event:
		default:
			// Channel full, skip (non-blocking)
		}
	}
}

// flushLoop runs the periodic flush timer.
func (c *enterpriseCollector) flushLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.flush()
		}
	}
}

// flush writes buffered events to the store.
func (c *enterpriseCollector) flush() {
	c.mu.Lock()
	if len(c.buffer) == 0 {
		c.mu.Unlock()
		return
	}

	// Swap buffer
	events := c.buffer
	c.buffer = make([]UsageEvent, 0, c.cfg.FlushSize)
	c.mu.Unlock()

	// Write to store
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := c.store.InsertEvents(ctx, events); err != nil {
		log.Error().Err(err).Int("count", len(events)).Msg("failed to flush usage events")
		// Re-add failed events to buffer (best effort)
		c.mu.Lock()
		c.buffer = append(events, c.buffer...)
		// Cap buffer to prevent unbounded growth
		if len(c.buffer) > c.cfg.FlushSize*3 {
			log.Warn().Int("dropped", len(c.buffer)-c.cfg.FlushSize*2).Msg("dropping oldest usage events")
			c.buffer = c.buffer[len(c.buffer)-c.cfg.FlushSize*2:]
		}
		c.mu.Unlock()
		return
	}

	log.Debug().Int("count", len(events)).Msg("flushed usage events")
}
