// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package taskqueue

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// synctestHandler implements Handler for synctest tests.
type synctestHandler struct {
	taskType TaskType
	handleFn func(ctx context.Context, task *Task) error
}

func (h *synctestHandler) Type() TaskType {
	return h.taskType
}

func (h *synctestHandler) Handle(ctx context.Context, task *Task) error {
	if h.handleFn != nil {
		return h.handleFn(ctx, task)
	}
	return nil
}

// TestMemoryQueue_Enqueue_Synctest tests MemoryQueue.Enqueue with controlled time.
func TestMemoryQueue_Enqueue_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemoryQueue()
		ctx := context.Background()

		// Synctest starts at 2000-01-01 00:00:00 UTC
		now := time.Now().UTC()
		assert.Equal(t, 2000, now.Year())

		task := &Task{
			Type:    "test-task",
			Payload: []byte(`{"key": "value"}`),
		}

		err := q.Enqueue(ctx, task)
		require.NoError(t, err)

		// Verify task was enqueued with current time
		assert.NotEmpty(t, task.ID)
		assert.Equal(t, StatusPending, task.Status)
		// Compare UTC to handle timezone differences
		assert.True(t, now.Equal(task.CreatedAt), "CreatedAt should match synctest time")
		assert.True(t, now.Equal(task.ScheduledAt), "ScheduledAt should match synctest time")
	})
}

// TestMemoryQueue_Dequeue_Synctest tests MemoryQueue.Dequeue with controlled time.
func TestMemoryQueue_Dequeue_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemoryQueue()
		ctx := context.Background()

		// Enqueue a task
		task := &Task{
			Type:    "test-task",
			Payload: []byte(`{}`),
		}
		_ = q.Enqueue(ctx, task)

		// Dequeue it
		dequeued, err := q.Dequeue(ctx, "worker-1", "test-task")
		require.NoError(t, err)
		require.NotNil(t, dequeued)

		assert.Equal(t, task.ID, dequeued.ID)
		assert.Equal(t, StatusRunning, dequeued.Status)
		assert.Equal(t, "worker-1", dequeued.WorkerID)
		assert.NotNil(t, dequeued.StartedAt)
	})
}

// TestMemoryQueue_ScheduledTask_Synctest tests that scheduled tasks wait until their time.
func TestMemoryQueue_ScheduledTask_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemoryQueue()
		ctx := context.Background()

		// Enqueue a task scheduled 1 hour from now
		now := time.Now()
		task := &Task{
			Type:        "scheduled-task",
			ScheduledAt: now.Add(1 * time.Hour),
		}
		_ = q.Enqueue(ctx, task)

		// Should not dequeue immediately
		dequeued, _ := q.Dequeue(ctx, "worker-1", "scheduled-task")
		assert.Nil(t, dequeued, "task should not be dequeued before scheduled time")

		// Advance time by 30 minutes
		time.Sleep(30 * time.Minute)
		dequeued, _ = q.Dequeue(ctx, "worker-1", "scheduled-task")
		assert.Nil(t, dequeued, "task should still not be dequeued")

		// Advance time past scheduled time
		time.Sleep(31 * time.Minute)
		dequeued, _ = q.Dequeue(ctx, "worker-1", "scheduled-task")
		assert.NotNil(t, dequeued, "task should now be dequeued")
	})
}

// TestMemoryQueue_RetryBackoff_Synctest tests exponential backoff on failure.
func TestMemoryQueue_RetryBackoff_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemoryQueue()
		ctx := context.Background()

		task := &Task{
			Type:       "retry-task",
			MaxRetries: 3,
		}
		_ = q.Enqueue(ctx, task)

		// Dequeue and fail
		dequeued, _ := q.Dequeue(ctx, "worker-1", "retry-task")
		require.NotNil(t, dequeued)

		err := q.Fail(ctx, dequeued.ID, assert.AnError)
		require.NoError(t, err)

		// Should not be immediately available (backoff is 2s for first retry)
		dequeued, _ = q.Dequeue(ctx, "worker-1", "retry-task")
		assert.Nil(t, dequeued, "task should be in backoff")

		// Advance time past backoff (2s)
		time.Sleep(3 * time.Second)
		dequeued, _ = q.Dequeue(ctx, "worker-1", "retry-task")
		assert.NotNil(t, dequeued, "task should be available after backoff")

		// Fail again
		_ = q.Fail(ctx, dequeued.ID, assert.AnError)

		// Next backoff is 4s
		time.Sleep(3 * time.Second)
		dequeued, _ = q.Dequeue(ctx, "worker-1", "retry-task")
		assert.Nil(t, dequeued, "task should still be in backoff")

		time.Sleep(2 * time.Second)
		dequeued, _ = q.Dequeue(ctx, "worker-1", "retry-task")
		assert.NotNil(t, dequeued, "task should be available after 4s backoff")
	})
}

// TestMemoryQueue_Cleanup_Synctest tests cleanup of old completed tasks.
func TestMemoryQueue_Cleanup_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemoryQueue()
		ctx := context.Background()

		// Enqueue and complete a task
		task := &Task{Type: "cleanup-task"}
		_ = q.Enqueue(ctx, task)
		dequeued, _ := q.Dequeue(ctx, "worker-1", "cleanup-task")
		_ = q.Complete(ctx, dequeued.ID)

		// Verify task exists
		stats, _ := q.Stats(ctx)
		assert.Equal(t, int64(1), stats.Completed)

		// Cleanup with 1 hour retention - should not remove
		count, _ := q.Cleanup(ctx, 1*time.Hour)
		assert.Equal(t, 0, count)

		// Advance time by 2 hours
		time.Sleep(2 * time.Hour)

		// Now cleanup should remove it
		count, _ = q.Cleanup(ctx, 1*time.Hour)
		assert.Equal(t, 1, count)

		// Verify removed
		stats, _ = q.Stats(ctx)
		assert.Equal(t, int64(0), stats.Completed)
	})
}

// TestWorker_ProcessTask_Synctest tests the real Worker processing a task.
func TestWorker_ProcessTask_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemoryQueue()
		var processed atomic.Bool

		w := NewWorker(WorkerConfig{
			Queue:        q,
			Concurrency:  1,
			PollInterval: 100 * time.Millisecond,
		})

		w.RegisterHandler(&synctestHandler{
			taskType: "test-type",
			handleFn: func(ctx context.Context, task *Task) error {
				processed.Store(true)
				return nil
			},
		})

		// Enqueue a task
		_ = q.Enqueue(context.Background(), &Task{Type: "test-type"})

		ctx, cancel := context.WithCancel(context.Background())

		// Start worker
		w.Start(ctx)

		// Wait for poll interval + processing
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()

		assert.True(t, processed.Load(), "task should have been processed")

		cancel()
		w.Stop()
	})
}

// TestWorker_MultiplePolls_Synctest tests multiple poll cycles.
func TestWorker_MultiplePolls_Synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewMemoryQueue()
		var processCount atomic.Int32

		w := NewWorker(WorkerConfig{
			Queue:        q,
			Concurrency:  1,
			PollInterval: 50 * time.Millisecond,
		})

		w.RegisterHandler(&synctestHandler{
			taskType: "test-type",
			handleFn: func(ctx context.Context, task *Task) error {
				processCount.Add(1)
				return nil
			},
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start worker
		w.Start(ctx)

		// Enqueue 3 tasks
		for i := range 3 {
			_ = q.Enqueue(context.Background(), &Task{
				Type:    "test-type",
				Payload: []byte{byte(i)},
			})
		}

		// Wait enough time for all tasks to be processed
		// (3 tasks * poll interval + buffer for processing)
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()

		assert.Equal(t, int32(3), processCount.Load())

		w.Stop()
	})
}
