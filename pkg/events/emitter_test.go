// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmitterDisabled(t *testing.T) {
	t.Parallel()

	// Noop emitter
	emitter := NoopEmitter()
	assert.False(t, emitter.IsEnabled())

	// Should not panic
	emitter.Emit(context.Background(), &taskqueue.EventPayload{
		EventName: "s3:ObjectCreated:Put",
		Bucket:    "test-bucket",
		Key:       "test-key",
	})

	stats := emitter.Stats()
	assert.False(t, stats.Enabled)
	// Metrics are tracked via Prometheus (zapfs_events_dropped_total)
}

func TestEmitterEnabledNoQueue(t *testing.T) {
	t.Parallel()

	// Enabled but no queue - should be disabled
	emitter := NewEmitter(EmitterConfig{
		Enabled: true,
		Queue:   nil,
	})
	assert.False(t, emitter.IsEnabled())
}

func TestEmitterWithMemoryQueue(t *testing.T) {
	t.Parallel()

	// Create memory queue
	queue := taskqueue.NewMemoryQueue()

	emitter := NewEmitter(EmitterConfig{
		Enabled: true,
		Queue:   queue,
		Region:  "us-west-2",
	})
	assert.True(t, emitter.IsEnabled())

	ctx := context.Background()

	// Emit an event
	emitter.Emit(ctx, &taskqueue.EventPayload{
		EventName: "s3:ObjectCreated:Put",
		Bucket:    "test-bucket",
		Key:       "test-key",
		Size:      1024,
		ETag:      "abc123",
		OwnerID:   "owner1",
		RequestID: "req1",
		SourceIP:  "127.0.0.1",
	})

	stats := emitter.Stats()
	assert.True(t, stats.Enabled)
	assert.Equal(t, "us-west-2", stats.Region)
	// Metrics are tracked via Prometheus (zapfs_events_emitted_total)

	// Verify task was enqueued
	qstats, err := queue.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), qstats.Pending)
	assert.Equal(t, int64(1), qstats.ByType[taskqueue.TaskTypeEvent])

	// Dequeue and verify
	task, err := queue.Dequeue(ctx, "test-worker", taskqueue.TaskTypeEvent)
	require.NoError(t, err)
	require.NotNil(t, task)

	assert.Equal(t, taskqueue.TaskTypeEvent, task.Type)
	assert.Equal(t, taskqueue.StatusRunning, task.Status)
	assert.Equal(t, "us-west-2", task.Region)

	// Use the generic unmarshal helper
	payload, err := taskqueue.UnmarshalPayload[taskqueue.EventPayload](task.Payload)
	require.NoError(t, err)

	assert.Equal(t, "s3:ObjectCreated:Put", payload.EventName)
	assert.Equal(t, "test-bucket", payload.Bucket)
	assert.Equal(t, "test-key", payload.Key)
	assert.Equal(t, int64(1024), payload.Size)
	assert.NotEmpty(t, payload.Sequencer)
	assert.NotZero(t, payload.Timestamp)
}

func TestEmitterHelperMethods(t *testing.T) {
	t.Parallel()

	queue := taskqueue.NewMemoryQueue()
	emitter := NewEmitter(EmitterConfig{
		Enabled: true,
		Queue:   queue,
		Region:  "us-east-1",
	})

	ctx := context.Background()

	// Test EmitObjectCreated
	emitter.EmitObjectCreated(ctx, EventObjectCreatedPut, "bucket1", "key1", 2048, "etag1", "v1", "owner1", "req1", "10.0.0.1")

	// Test EmitObjectRemoved
	emitter.EmitObjectRemoved(ctx, EventObjectRemovedDelete, "bucket2", "key2", "v2", "owner2", "req2", "10.0.0.2")

	// Test EmitObjectTagging
	emitter.EmitObjectTagging(ctx, EventObjectTaggingPut, "bucket3", "key3", "v3", "owner3", "req3", "10.0.0.3")

	// Verify tasks were enqueued
	qstats, err := queue.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), qstats.Pending)
}

func TestEmitterSequencer(t *testing.T) {
	t.Parallel()

	queue := taskqueue.NewMemoryQueue()
	emitter := NewEmitter(EmitterConfig{
		Enabled: true,
		Queue:   queue,
	})

	ctx := context.Background()

	// Emit multiple events
	for i := 0; i < 10; i++ {
		emitter.Emit(ctx, &taskqueue.EventPayload{
			EventName: "s3:ObjectCreated:Put",
			Bucket:    "bucket",
			Key:       "key",
		})
	}

	// Verify all tasks have unique sequencers
	sequencers := make(map[string]bool)
	for i := 0; i < 10; i++ {
		task, err := queue.Dequeue(ctx, "test-worker", taskqueue.TaskTypeEvent)
		require.NoError(t, err)
		require.NotNil(t, task)

		payload, err := taskqueue.UnmarshalPayload[taskqueue.EventPayload](task.Payload)
		require.NoError(t, err)

		assert.NotEmpty(t, payload.Sequencer)
		assert.False(t, sequencers[payload.Sequencer], "duplicate sequencer: %s", payload.Sequencer)
		sequencers[payload.Sequencer] = true

		// Complete task so it's removed
		queue.Complete(ctx, task.ID)
	}
}

func TestEmitterStats(t *testing.T) {
	t.Parallel()

	emitter := NewEmitter(EmitterConfig{
		Enabled: true,
		Queue:   taskqueue.NewMemoryQueue(),
		Region:  "eu-west-1",
	})

	stats := emitter.Stats()
	assert.True(t, stats.Enabled)
	assert.Equal(t, "eu-west-1", stats.Region)
}
