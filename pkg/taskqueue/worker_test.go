// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package taskqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testHandler implements Handler for testing
type testHandler struct {
	taskType taskqueue.TaskType
	handleFn func(ctx context.Context, task *taskqueue.Task) error
}

func (h *testHandler) Type() taskqueue.TaskType {
	return h.taskType
}

func (h *testHandler) Handle(ctx context.Context, task *taskqueue.Task) error {
	if h.handleFn != nil {
		return h.handleFn(ctx, task)
	}
	return nil
}

func TestWorker_NewWorker(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:    "test-worker",
		Queue: q,
	})

	assert.NotNil(t, worker)
	assert.Equal(t, q, worker.Queue())
}

func TestWorker_NewWorker_CustomConfig(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 2 * time.Second,
		Concurrency:  10,
	})

	assert.NotNil(t, worker)
}

func TestWorker_RegisterHandler(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:    "test-worker",
		Queue: q,
	})

	handler := &testHandler{taskType: taskqueue.TaskTypeCleanup}
	worker.RegisterHandler(handler)

	types := worker.HandlerTypes()
	assert.Len(t, types, 1)
	assert.Contains(t, types, taskqueue.TaskTypeCleanup)
}

func TestWorker_RegisterHandler_Nil(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:    "test-worker",
		Queue: q,
	})

	// Should not panic
	worker.RegisterHandler(nil)
	assert.Empty(t, worker.HandlerTypes())
}

func TestWorker_HandlerTypes(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:    "test-worker",
		Queue: q,
	})

	handler1 := &testHandler{taskType: taskqueue.TaskTypeCleanup}
	handler2 := &testHandler{taskType: taskqueue.TaskTypeLifecycle}

	worker.RegisterHandler(handler1)
	worker.RegisterHandler(handler2)

	types := worker.HandlerTypes()
	assert.Len(t, types, 2)
	assert.Contains(t, types, taskqueue.TaskTypeCleanup)
	assert.Contains(t, types, taskqueue.TaskTypeLifecycle)
}

func TestWorker_StartStop(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 10 * time.Millisecond,
		Concurrency:  2,
	})

	handler := &testHandler{taskType: taskqueue.TaskTypeCleanup}
	worker.RegisterHandler(handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should not block
	worker.Start(ctx)

	// Give it time to start workers
	time.Sleep(50 * time.Millisecond)

	// Stop should be graceful
	worker.Stop()
}

func TestWorker_StartStop_NoHandlers(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 10 * time.Millisecond,
	})

	// No handlers registered

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Should return immediately
	worker.Start(ctx)

	// Stop should work even though no workers started
	worker.Stop()
}

func TestWorker_ProcessTask_Success(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	var processed int64
	handler := &testHandler{
		taskType: taskqueue.TaskTypeCleanup,
		handleFn: func(ctx context.Context, task *taskqueue.Task) error {
			atomic.AddInt64(&processed, 1)
			return nil
		},
	}

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 10 * time.Millisecond,
		Concurrency:  1,
	})
	worker.RegisterHandler(handler)

	// Enqueue a task
	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeCleanup,
		Payload:    json.RawMessage(`{"test": true}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Start worker
	workerCtx, cancel := context.WithCancel(ctx)
	worker.Start(workerCtx)

	// Wait for task to be processed
	time.Sleep(100 * time.Millisecond)
	cancel()
	worker.Stop()

	// Task should be completed
	completed, err := q.Get(ctx, "task-1")
	require.NoError(t, err)
	assert.Equal(t, taskqueue.StatusCompleted, completed.Status)
	assert.Equal(t, int64(1), atomic.LoadInt64(&processed))
}

func TestWorker_ProcessTask_Failure_Retry(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	taskErr := errors.New("processing failed")

	handler := &testHandler{
		taskType: taskqueue.TaskTypeCleanup,
		handleFn: func(ctx context.Context, task *taskqueue.Task) error {
			return taskErr
		},
	}

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 10 * time.Millisecond,
		Concurrency:  1,
	})
	worker.RegisterHandler(handler)

	// Enqueue a task
	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeCleanup,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Start worker
	workerCtx, cancel := context.WithCancel(ctx)
	worker.Start(workerCtx)

	// Wait for task to be processed
	time.Sleep(100 * time.Millisecond)
	cancel()
	worker.Stop()

	// Task should be failed but pending for retry
	failed, err := q.Get(ctx, "task-1")
	require.NoError(t, err)
	assert.Equal(t, taskqueue.StatusPending, failed.Status)
	assert.Equal(t, 1, failed.Attempts)
	assert.Contains(t, failed.LastError, "processing failed")
}

func TestWorker_ProcessTask_MaxRetries_DeadLetter(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	taskErr := errors.New("always fails")

	handler := &testHandler{
		taskType: taskqueue.TaskTypeCleanup,
		handleFn: func(ctx context.Context, task *taskqueue.Task) error {
			return taskErr
		},
	}

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 10 * time.Millisecond,
		Concurrency:  1,
	})
	worker.RegisterHandler(handler)

	// Enqueue a task that's already at max retries
	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeCleanup,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 1, // Will dead letter after 1 failure
		Attempts:   0,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Start worker
	workerCtx, cancel := context.WithCancel(ctx)
	worker.Start(workerCtx)

	// Wait for task to be processed
	time.Sleep(100 * time.Millisecond)
	cancel()
	worker.Stop()

	// Task should be dead lettered
	deadLettered, err := q.Get(ctx, "task-1")
	require.NoError(t, err)
	assert.Equal(t, taskqueue.StatusDeadLetter, deadLettered.Status)
}

func TestWorker_NoHandlerForTaskType(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	// Register handler for Cleanup type
	handler := &testHandler{
		taskType: taskqueue.TaskTypeCleanup,
	}

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 10 * time.Millisecond,
		Concurrency:  1,
	})
	worker.RegisterHandler(handler)

	// Enqueue a task of a different type (Lifecycle)
	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeLifecycle, // Different type!
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Start worker
	workerCtx, cancel := context.WithCancel(ctx)
	worker.Start(workerCtx)

	// Wait a bit
	time.Sleep(100 * time.Millisecond)
	cancel()
	worker.Stop()

	// Task should still be pending (worker only handles Cleanup)
	pending, err := q.Get(ctx, "task-1")
	require.NoError(t, err)
	assert.Equal(t, taskqueue.StatusPending, pending.Status)
}

func TestWorker_ContextCancellation(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	handler := &testHandler{taskType: taskqueue.TaskTypeCleanup}

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 10 * time.Millisecond,
		Concurrency:  2,
	})
	worker.RegisterHandler(handler)

	ctx, cancel := context.WithCancel(context.Background())
	worker.Start(ctx)

	// Cancel context should stop workers
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Stop should complete quickly
	done := make(chan struct{})
	go func() {
		worker.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Workers stopped
	case <-time.After(time.Second):
		t.Fatal("workers did not stop after context cancellation")
	}
}

func TestWorker_ConcurrentWorkers(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	const numTasks = 20
	var processedCount int64

	handler := &testHandler{
		taskType: taskqueue.TaskTypeCleanup,
		handleFn: func(ctx context.Context, task *taskqueue.Task) error {
			time.Sleep(10 * time.Millisecond) // Simulate work
			atomic.AddInt64(&processedCount, 1)
			return nil
		},
	}

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 5 * time.Millisecond,
		Concurrency:  5,
	})
	worker.RegisterHandler(handler)

	// Enqueue tasks
	for i := 0; i < numTasks; i++ {
		task := &taskqueue.Task{
			Type:       taskqueue.TaskTypeCleanup,
			Payload:    json.RawMessage(`{}`),
			MaxRetries: 3,
		}
		require.NoError(t, q.Enqueue(ctx, task))
	}

	// Start worker
	workerCtx, cancel := context.WithCancel(ctx)
	worker.Start(workerCtx)

	// Wait for tasks to be processed
	time.Sleep(500 * time.Millisecond)
	cancel()
	worker.Stop()

	// All tasks should be processed
	assert.Equal(t, int64(numTasks), atomic.LoadInt64(&processedCount))
}

func TestWorker_GracefulShutdown(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	startedProcessing := make(chan struct{})
	canFinish := make(chan struct{})

	handler := &testHandler{
		taskType: taskqueue.TaskTypeCleanup,
		handleFn: func(ctx context.Context, task *taskqueue.Task) error {
			close(startedProcessing)
			<-canFinish // Wait until we're told to finish
			return nil
		},
	}

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 10 * time.Millisecond,
		Concurrency:  1,
	})
	worker.RegisterHandler(handler)

	// Enqueue a task
	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeCleanup,
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 3,
	}
	require.NoError(t, q.Enqueue(ctx, task))

	// Start worker
	workerCtx, cancel := context.WithCancel(ctx)
	worker.Start(workerCtx)

	// Wait for task to start processing
	select {
	case <-startedProcessing:
	case <-time.After(time.Second):
		t.Fatal("task did not start processing")
	}

	// Allow task to finish
	close(canFinish)

	// Cancel and stop
	cancel()
	worker.Stop()

	// Task should be completed
	completed, err := q.Get(ctx, "task-1")
	require.NoError(t, err)
	assert.Equal(t, taskqueue.StatusCompleted, completed.Status)
}

func TestWorker_Queue(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:    "test-worker",
		Queue: q,
	})

	assert.Equal(t, q, worker.Queue())
}

func TestWorker_MultipleHandlers(t *testing.T) {
	t.Parallel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()
	ctx := context.Background()

	var lifecycleProcessed, cleanupProcessed int64

	lifecycleHandler := &testHandler{
		taskType: taskqueue.TaskTypeLifecycle,
		handleFn: func(ctx context.Context, task *taskqueue.Task) error {
			atomic.AddInt64(&lifecycleProcessed, 1)
			return nil
		},
	}

	cleanupHandler := &testHandler{
		taskType: taskqueue.TaskTypeCleanup,
		handleFn: func(ctx context.Context, task *taskqueue.Task) error {
			atomic.AddInt64(&cleanupProcessed, 1)
			return nil
		},
	}

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 10 * time.Millisecond,
		Concurrency:  2,
	})
	worker.RegisterHandler(lifecycleHandler)
	worker.RegisterHandler(cleanupHandler)

	// Enqueue both types
	for i := 0; i < 5; i++ {
		require.NoError(t, q.Enqueue(ctx, &taskqueue.Task{
			Type:       taskqueue.TaskTypeLifecycle,
			Payload:    json.RawMessage(`{}`),
			MaxRetries: 3,
		}))
		require.NoError(t, q.Enqueue(ctx, &taskqueue.Task{
			Type:       taskqueue.TaskTypeCleanup,
			Payload:    json.RawMessage(`{}`),
			MaxRetries: 3,
		}))
	}

	// Start worker
	workerCtx, cancel := context.WithCancel(ctx)
	worker.Start(workerCtx)

	// Wait for tasks to be processed
	time.Sleep(300 * time.Millisecond)
	cancel()
	worker.Stop()

	assert.Equal(t, int64(5), atomic.LoadInt64(&lifecycleProcessed))
	assert.Equal(t, int64(5), atomic.LoadInt64(&cleanupProcessed))
}
