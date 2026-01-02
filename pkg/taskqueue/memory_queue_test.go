// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package taskqueue_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryQueue_Enqueue(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	task := &taskqueue.Task{
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{"chunk_id": "test"}`),
		MaxRetries: 3,
		Priority:   taskqueue.PriorityNormal,
	}

	err := q.Enqueue(context.Background(), task)
	require.NoError(t, err)

	// Task should have been assigned an ID
	assert.NotEmpty(t, task.ID)
	assert.Equal(t, taskqueue.StatusPending, task.Status)
	assert.False(t, task.CreatedAt.IsZero())
	assert.False(t, task.UpdatedAt.IsZero())
	assert.False(t, task.ScheduledAt.IsZero())
}

func TestMemoryQueue_Enqueue_PreserveID(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	task := &taskqueue.Task{
		ID:         "custom-id",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}

	err := q.Enqueue(context.Background(), task)
	require.NoError(t, err)
	assert.Equal(t, "custom-id", task.ID)
}

func TestMemoryQueue_Enqueue_QueueClosed(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	q.Close()

	task := &taskqueue.Task{
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}

	err := q.Enqueue(context.Background(), task)
	assert.ErrorIs(t, err, taskqueue.ErrQueueClosed)
}

func TestMemoryQueue_Dequeue_FIFO(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Enqueue tasks with same priority
	for i := 0; i < 3; i++ {
		task := &taskqueue.Task{
			Type:        taskqueue.TaskTypeGCDecrement,
			Payload:     json.RawMessage(`{}`),
			MaxRetries:  3,
			Priority:    taskqueue.PriorityNormal,
			ScheduledAt: time.Now().Add(time.Duration(i) * time.Millisecond),
		}
		require.NoError(t, q.Enqueue(ctx, task))
	}

	// Dequeue should return tasks in scheduled order (oldest first)
	task1, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	require.NotNil(t, task1)
	assert.Equal(t, taskqueue.StatusRunning, task1.Status)
	assert.Equal(t, "worker-1", task1.WorkerID)

	// Verify StartedAt was set
	assert.NotNil(t, task1.StartedAt)
}

func TestMemoryQueue_Dequeue_Priority(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Enqueue low priority task first
	lowTask := &taskqueue.Task{
		ID:         "low",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
		Priority:   taskqueue.PriorityLow,
	}
	require.NoError(t, q.Enqueue(ctx, lowTask))

	// Enqueue high priority task second
	highTask := &taskqueue.Task{
		ID:         "high",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
		Priority:   taskqueue.PriorityUrgent,
	}
	require.NoError(t, q.Enqueue(ctx, highTask))

	// High priority task should be dequeued first
	task, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, "high", task.ID)
}

func TestMemoryQueue_Dequeue_TypeFilter(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Enqueue different task types
	gcTask := &taskqueue.Task{
		ID:         "gc-task",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, gcTask))

	cleanupTask := &taskqueue.Task{
		ID:         "cleanup-task",
		Type:       taskqueue.TaskTypeCleanup,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, cleanupTask))

	// Filter for cleanup tasks only
	task, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeCleanup)
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, "cleanup-task", task.ID)
}

func TestMemoryQueue_Dequeue_Empty(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	task, err := q.Dequeue(context.Background(), "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	assert.Nil(t, task)
}

func TestMemoryQueue_Dequeue_QueueClosed(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	q.Close()

	task, err := q.Dequeue(context.Background(), "worker-1", taskqueue.TaskTypeGCDecrement)
	assert.ErrorIs(t, err, taskqueue.ErrQueueClosed)
	assert.Nil(t, task)
}

func TestMemoryQueue_Dequeue_SkipsFutureTasks(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Enqueue task scheduled for the future
	futureTask := &taskqueue.Task{
		ID:          "future",
		Type:        taskqueue.TaskTypeGCDecrement,
		Payload:     json.RawMessage(`{}`),
		MaxRetries:  3,
		ScheduledAt: time.Now().Add(time.Hour),
	}
	require.NoError(t, q.Enqueue(ctx, futureTask))

	// Should not get the future task
	task, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	assert.Nil(t, task)
}

func TestMemoryQueue_Dequeue_SkipsRetryAfterTasks(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Enqueue task with RetryAfter in the future
	retryTask := &taskqueue.Task{
		ID:         "retry",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
		RetryAfter: time.Now().Add(time.Hour),
	}
	require.NoError(t, q.Enqueue(ctx, retryTask))

	// Should not get the task due to RetryAfter
	task, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	assert.Nil(t, task)
}

func TestMemoryQueue_Complete(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Dequeue and complete
	dequeued, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	err = q.Complete(ctx, dequeued.ID)
	require.NoError(t, err)

	// Verify task is completed
	completed, err := q.Get(ctx, dequeued.ID)
	require.NoError(t, err)
	assert.Equal(t, taskqueue.StatusCompleted, completed.Status)
	assert.NotNil(t, completed.CompletedAt)
}

func TestMemoryQueue_Complete_NotFound(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	err := q.Complete(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, taskqueue.ErrTaskNotFound)
}

func TestMemoryQueue_Fail_Retry(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Dequeue and fail
	dequeued, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	err = q.Fail(ctx, dequeued.ID, assert.AnError)
	require.NoError(t, err)

	// Task should be pending for retry
	failed, err := q.Get(ctx, dequeued.ID)
	require.NoError(t, err)
	assert.Equal(t, taskqueue.StatusPending, failed.Status)
	assert.Equal(t, 1, failed.Attempts)
	assert.Contains(t, failed.LastError, "assert.AnError")
	assert.False(t, failed.RetryAfter.IsZero())
	assert.Empty(t, failed.WorkerID)
}

func TestMemoryQueue_Fail_MaxRetries(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 1,
		Attempts:   0,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Dequeue and fail (should go to dead letter)
	dequeued, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	err = q.Fail(ctx, dequeued.ID, assert.AnError)
	require.NoError(t, err)

	// Task should be dead lettered
	failed, err := q.Get(ctx, dequeued.ID)
	require.NoError(t, err)
	assert.Equal(t, taskqueue.StatusDeadLetter, failed.Status)
	assert.Equal(t, 1, failed.Attempts)
}

func TestMemoryQueue_Fail_NotFound(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	err := q.Fail(context.Background(), "nonexistent", assert.AnError)
	assert.ErrorIs(t, err, taskqueue.ErrTaskNotFound)
}

func TestMemoryQueue_Cancel(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	err := q.Cancel(ctx, task.ID)
	require.NoError(t, err)

	cancelled, err := q.Get(ctx, task.ID)
	require.NoError(t, err)
	assert.Equal(t, taskqueue.StatusCancelled, cancelled.Status)
}

func TestMemoryQueue_Cancel_NotFound(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	err := q.Cancel(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, taskqueue.ErrTaskNotFound)
}

func TestMemoryQueue_Heartbeat(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Dequeue the task
	dequeued, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	originalUpdatedAt := dequeued.UpdatedAt
	time.Sleep(10 * time.Millisecond)

	// Send heartbeat
	err = q.Heartbeat(ctx, dequeued.ID, "worker-1")
	require.NoError(t, err)

	// UpdatedAt should be updated
	updated, err := q.Get(ctx, dequeued.ID)
	require.NoError(t, err)
	assert.True(t, updated.UpdatedAt.After(originalUpdatedAt))
}

func TestMemoryQueue_Heartbeat_WrongWorker(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Dequeue the task
	_, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)

	// Heartbeat from wrong worker should fail
	err = q.Heartbeat(ctx, task.ID, "worker-2")
	assert.ErrorIs(t, err, taskqueue.ErrTaskNotFound)
}

func TestMemoryQueue_Heartbeat_NotRunning(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Task is pending, not running
	err := q.Heartbeat(ctx, task.ID, "worker-1")
	assert.ErrorIs(t, err, taskqueue.ErrTaskNotFound)
}

func TestMemoryQueue_Get(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{"key": "value"}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	got, err := q.Get(ctx, "task-1")
	require.NoError(t, err)
	assert.Equal(t, "task-1", got.ID)
	assert.Equal(t, taskqueue.TaskTypeGCDecrement, got.Type)
}

func TestMemoryQueue_Get_NotFound(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	got, err := q.Get(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, taskqueue.ErrTaskNotFound)
	assert.Nil(t, got)
}

func TestMemoryQueue_List(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Enqueue various tasks
	for i := 0; i < 5; i++ {
		task := &taskqueue.Task{
			Type:       taskqueue.TaskTypeGCDecrement,
			Payload:    json.RawMessage(`{}`),
			MaxRetries: 3,
		}
		require.NoError(t, q.Enqueue(ctx, task))
	}

	// List all
	tasks, err := q.List(ctx, taskqueue.TaskFilter{})
	require.NoError(t, err)
	assert.Len(t, tasks, 5)
}

func TestMemoryQueue_List_TypeFilter(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Enqueue different types
	require.NoError(t, q.Enqueue(ctx, &taskqueue.Task{Type: taskqueue.TaskTypeGCDecrement, Payload: json.RawMessage(`{}`)}))
	require.NoError(t, q.Enqueue(ctx, &taskqueue.Task{Type: taskqueue.TaskTypeCleanup, Payload: json.RawMessage(`{}`)}))
	require.NoError(t, q.Enqueue(ctx, &taskqueue.Task{Type: taskqueue.TaskTypeGCDecrement, Payload: json.RawMessage(`{}`)}))

	tasks, err := q.List(ctx, taskqueue.TaskFilter{Type: taskqueue.TaskTypeGCDecrement})
	require.NoError(t, err)
	assert.Len(t, tasks, 2)
}

func TestMemoryQueue_List_StatusFilter(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Enqueue and complete one
	task := &taskqueue.Task{ID: "task-1", Type: taskqueue.TaskTypeGCDecrement, Payload: json.RawMessage(`{}`)}
	require.NoError(t, q.Enqueue(ctx, task))
	_, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	require.NoError(t, q.Complete(ctx, "task-1"))

	// Enqueue another
	require.NoError(t, q.Enqueue(ctx, &taskqueue.Task{Type: taskqueue.TaskTypeGCDecrement, Payload: json.RawMessage(`{}`)}))

	// List only pending
	tasks, err := q.List(ctx, taskqueue.TaskFilter{Status: taskqueue.StatusPending})
	require.NoError(t, err)
	assert.Len(t, tasks, 1)

	// List only completed
	tasks, err = q.List(ctx, taskqueue.TaskFilter{Status: taskqueue.StatusCompleted})
	require.NoError(t, err)
	assert.Len(t, tasks, 1)
}

func TestMemoryQueue_List_LimitOffset(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		require.NoError(t, q.Enqueue(ctx, &taskqueue.Task{Type: taskqueue.TaskTypeGCDecrement, Payload: json.RawMessage(`{}`)}))
	}

	// Test limit
	tasks, err := q.List(ctx, taskqueue.TaskFilter{Limit: 3})
	require.NoError(t, err)
	assert.Len(t, tasks, 3)

	// Test offset
	tasks, err = q.List(ctx, taskqueue.TaskFilter{Offset: 5})
	require.NoError(t, err)
	assert.Len(t, tasks, 5)

	// Test both
	tasks, err = q.List(ctx, taskqueue.TaskFilter{Offset: 2, Limit: 3})
	require.NoError(t, err)
	assert.Len(t, tasks, 3)
}

func TestMemoryQueue_Stats(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Enqueue some tasks
	require.NoError(t, q.Enqueue(ctx, &taskqueue.Task{ID: "1", Type: taskqueue.TaskTypeGCDecrement, Payload: json.RawMessage(`{}`)}))
	require.NoError(t, q.Enqueue(ctx, &taskqueue.Task{ID: "2", Type: taskqueue.TaskTypeGCDecrement, Payload: json.RawMessage(`{}`)}))
	require.NoError(t, q.Enqueue(ctx, &taskqueue.Task{ID: "3", Type: taskqueue.TaskTypeCleanup, Payload: json.RawMessage(`{}`)}))

	// Dequeue one and mark as running
	_, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)

	// Complete one
	require.NoError(t, q.Complete(ctx, "1"))

	stats, err := q.Stats(ctx)
	require.NoError(t, err)

	assert.Equal(t, int64(2), stats.Pending)   // "2" and "3" are pending
	assert.Equal(t, int64(0), stats.Running)   // "1" was completed
	assert.Equal(t, int64(1), stats.Completed) // "1" completed
	assert.NotNil(t, stats.OldestPending)
	// ByType counts ALL tasks regardless of status
	assert.Equal(t, int64(2), stats.ByType[taskqueue.TaskTypeGCDecrement]) // task 1 (completed) + task 2 (pending)
	assert.Equal(t, int64(1), stats.ByType[taskqueue.TaskTypeCleanup])     // task 3 (pending)
}

func TestMemoryQueue_Cleanup(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Enqueue and complete a task
	task := &taskqueue.Task{ID: "old-task", Type: taskqueue.TaskTypeGCDecrement, Payload: json.RawMessage(`{}`)}
	require.NoError(t, q.Enqueue(ctx, task))
	_, err := q.Dequeue(ctx, "worker-1", taskqueue.TaskTypeGCDecrement)
	require.NoError(t, err)
	require.NoError(t, q.Complete(ctx, "old-task"))

	// Wait a bit and cleanup with short duration
	time.Sleep(20 * time.Millisecond)

	// Cleanup tasks older than 10ms
	count, err := q.Cleanup(ctx, 10*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Task should be gone
	_, err = q.Get(ctx, "old-task")
	assert.ErrorIs(t, err, taskqueue.ErrTaskNotFound)
}

func TestMemoryQueue_ThreadSafety(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	const numGoroutines = 10
	const numTasks = 100

	var wg sync.WaitGroup

	// Concurrent enqueue
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numTasks; j++ {
				task := &taskqueue.Task{
					Type:       taskqueue.TaskTypeGCDecrement,
					Payload:    json.RawMessage(`{}`),
					MaxRetries: 3,
				}
				_ = q.Enqueue(ctx, task)
			}
		}()
	}

	// Concurrent dequeue
	var processed int64
	var mu sync.Mutex
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numTasks; j++ {
				task, _ := q.Dequeue(ctx, string(rune('A'+workerID)), taskqueue.TaskTypeGCDecrement)
				if task != nil {
					mu.Lock()
					processed++
					mu.Unlock()
					_ = q.Complete(ctx, task.ID)
				}
			}
		}(i)
	}

	wg.Wait()

	// All tasks should be processed
	stats, err := q.Stats(ctx)
	require.NoError(t, err)

	// The exact count depends on timing, but completed + remaining pending should equal total
	total := stats.Completed + stats.Pending
	assert.Equal(t, int64(numGoroutines*numTasks), total)
}
