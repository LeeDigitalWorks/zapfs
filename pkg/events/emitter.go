// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"

	"github.com/google/uuid"
)

// Emitter queues S3 events for async delivery via the taskqueue.
//
// In community edition, events are queued but the EventHandler stub
// simply drops them. In enterprise edition, EventHandler delivers
// events to configured destinations (Redis, Kafka, Webhook).
type Emitter struct {
	queue   taskqueue.Queue
	enabled bool
	region  string

	// Sequencer state - monotonic counter for event ordering
	sequencer atomic.Uint64
}

// EmitterConfig configures the event emitter.
type EmitterConfig struct {
	// Queue is the taskqueue for persisting events.
	// If nil, events are silently dropped.
	Queue taskqueue.Queue

	// Enabled controls whether events are queued.
	// If false, Emit() is a no-op.
	Enabled bool

	// Region is the region identifier for events.
	Region string
}

// NewEmitter creates an event emitter.
func NewEmitter(cfg EmitterConfig) *Emitter {
	return &Emitter{
		queue:   cfg.Queue,
		enabled: cfg.Enabled && cfg.Queue != nil,
		region:  cfg.Region,
	}
}

// NoopEmitter returns an emitter that drops all events.
// Use this when event notifications are disabled.
func NoopEmitter() *Emitter {
	return &Emitter{enabled: false}
}

// Emit queues an S3 event for delivery.
// Returns immediately; delivery is async via the taskqueue.
//
// If the emitter is disabled or the queue is nil, events are silently dropped.
// Errors are logged but not returned to avoid blocking S3 operations.
func (e *Emitter) Emit(ctx context.Context, payload *taskqueue.EventPayload) {
	if !e.enabled {
		EventsDroppedTotal.Inc()
		return
	}

	// Generate sequencer if not provided
	if payload.Sequencer == "" {
		payload.Sequencer = e.nextSequencer()
	}

	// Set timestamp if not provided
	if payload.Timestamp == 0 {
		payload.Timestamp = time.Now().UnixMilli()
	}

	// Marshal payload
	data, err := taskqueue.MarshalPayload(payload)
	if err != nil {
		EventsErrorsTotal.WithLabelValues("marshal").Inc()
		logger.Warn().
			Err(err).
			Str("event", payload.EventName).
			Str("bucket", payload.Bucket).
			Msg("failed to marshal event payload")
		return
	}

	// Create task
	task := &taskqueue.Task{
		ID:         uuid.New().String(),
		Type:       taskqueue.TaskTypeEvent,
		Status:     taskqueue.StatusPending,
		Priority:   taskqueue.PriorityNormal,
		Payload:    data,
		MaxRetries: 3,
		Region:     e.region,
	}

	// Enqueue - fire and forget
	if err := e.queue.Enqueue(ctx, task); err != nil {
		EventsErrorsTotal.WithLabelValues("enqueue").Inc()
		logger.Warn().
			Err(err).
			Str("event", payload.EventName).
			Str("bucket", payload.Bucket).
			Str("key", payload.Key).
			Msg("failed to queue event")
		return
	}

	EventsEmittedTotal.WithLabelValues(payload.EventName).Inc()
	logger.Debug().
		Str("event", payload.EventName).
		Str("bucket", payload.Bucket).
		Str("key", payload.Key).
		Str("task_id", task.ID).
		Msg("queued S3 event")
}

// EmitObjectCreated emits an object creation event.
func (e *Emitter) EmitObjectCreated(ctx context.Context, eventType EventType, bucket, key string, size int64, etag, versionID, ownerID, requestID, sourceIP string) {
	e.Emit(ctx, &taskqueue.EventPayload{
		EventName: string(eventType),
		Bucket:    bucket,
		Key:       key,
		Size:      size,
		ETag:      etag,
		VersionID: versionID,
		OwnerID:   ownerID,
		RequestID: requestID,
		SourceIP:  sourceIP,
	})
}

// EmitObjectRemoved emits an object deletion event.
func (e *Emitter) EmitObjectRemoved(ctx context.Context, eventType EventType, bucket, key, versionID, ownerID, requestID, sourceIP string) {
	e.Emit(ctx, &taskqueue.EventPayload{
		EventName: string(eventType),
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		OwnerID:   ownerID,
		RequestID: requestID,
		SourceIP:  sourceIP,
	})
}

// EmitObjectTagging emits an object tagging event.
func (e *Emitter) EmitObjectTagging(ctx context.Context, eventType EventType, bucket, key, versionID, ownerID, requestID, sourceIP string) {
	e.Emit(ctx, &taskqueue.EventPayload{
		EventName: string(eventType),
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		OwnerID:   ownerID,
		RequestID: requestID,
		SourceIP:  sourceIP,
	})
}

// Stats returns emitter statistics.
// Note: Detailed metrics are available via Prometheus at /metrics endpoint.
func (e *Emitter) Stats() EmitterStats {
	return EmitterStats{
		Enabled: e.enabled,
		Region:  e.region,
	}
}

// EmitterStats contains emitter status information.
// Detailed metrics are exposed via Prometheus (zapfs_events_*).
type EmitterStats struct {
	Enabled bool   `json:"enabled"`
	Region  string `json:"region,omitempty"`
}

// nextSequencer generates a unique, monotonically increasing sequencer value.
// Format: hex(timestamp_ms) + hex(counter) + random_suffix
func (e *Emitter) nextSequencer() string {
	ts := time.Now().UnixMilli()
	seq := e.sequencer.Add(1)

	// 8 bytes random suffix for uniqueness across processes
	suffix := make([]byte, 4)
	rand.Read(suffix)

	return hex.EncodeToString([]byte{
		byte(ts >> 40), byte(ts >> 32), byte(ts >> 24), byte(ts >> 16),
		byte(ts >> 8), byte(ts),
		byte(seq >> 8), byte(seq),
	}) + hex.EncodeToString(suffix)
}

// IsEnabled returns whether the emitter is enabled.
func (e *Emitter) IsEnabled() bool {
	return e.enabled
}
