//go:build enterprise

package taskqueue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"zapfs/pkg/taskqueue"
)

func TestNewReplicationTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload ReplicationPayload
		wantErr bool
	}{
		{
			name: "valid task",
			payload: ReplicationPayload{
				SourceRegion: "us-west-2",
				SourceBucket: "my-bucket",
				SourceKey:    "path/to/file.txt",
				DestRegion:   "eu-west-1",
			},
			wantErr: false,
		},
		{
			name: "with explicit dest bucket",
			payload: ReplicationPayload{
				SourceRegion: "us-west-2",
				SourceBucket: "source",
				SourceKey:    "key",
				DestRegion:   "eu-west-1",
				DestBucket:   "destination",
			},
			wantErr: false,
		},
		{
			name: "missing source bucket",
			payload: ReplicationPayload{
				SourceRegion: "us-west-2",
				SourceKey:    "key",
				DestRegion:   "eu-west-1",
			},
			wantErr: true,
		},
		{
			name: "missing dest region",
			payload: ReplicationPayload{
				SourceRegion: "us-west-2",
				SourceBucket: "bucket",
				SourceKey:    "key",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			task, err := NewReplicationTask(tt.payload)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, task)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, task)
				assert.Equal(t, TaskTypeReplication, task.Type)
				assert.Equal(t, taskqueue.StatusPending, task.Status)
				assert.Equal(t, 3, task.MaxRetries)
			}
		})
	}
}

func TestMemoryQueue_EnqueueDequeue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	// Create and enqueue a task
	task, err := NewReplicationTask(ReplicationPayload{
		SourceRegion: "us-west-2",
		SourceBucket: "bucket",
		SourceKey:    "key",
		DestRegion:   "eu-west-1",
	})
	require.NoError(t, err)

	err = q.Enqueue(ctx, task)
	require.NoError(t, err)

	// Dequeue should return the task
	dequeued, err := q.Dequeue(ctx, "worker-1", TaskTypeReplication)
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.Equal(t, task.ID, dequeued.ID)
	assert.Equal(t, taskqueue.StatusRunning, dequeued.Status)
	assert.Equal(t, "worker-1", dequeued.WorkerID)

	// Second dequeue should return nil (no tasks available)
	dequeued2, err := q.Dequeue(ctx, "worker-2", TaskTypeReplication)
	require.NoError(t, err)
	assert.Nil(t, dequeued2)
}

func TestMemoryQueue_Complete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	task, _ := NewReplicationTask(ReplicationPayload{
		SourceRegion: "us-west-2",
		SourceBucket: "bucket",
		SourceKey:    "key",
		DestRegion:   "eu-west-1",
	})

	q.Enqueue(ctx, task)
	q.Dequeue(ctx, "worker", TaskTypeReplication)

	err := q.Complete(ctx, task.ID)
	require.NoError(t, err)

	// Verify status
	completed, _ := q.Get(ctx, task.ID)
	assert.Equal(t, taskqueue.StatusCompleted, completed.Status)
	assert.NotNil(t, completed.CompletedAt)
}

func TestMemoryQueue_FailAndRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	task, _ := NewReplicationTask(ReplicationPayload{
		SourceRegion: "us-west-2",
		SourceBucket: "bucket",
		SourceKey:    "key",
		DestRegion:   "eu-west-1",
	})
	task.MaxRetries = 3

	q.Enqueue(ctx, task)
	q.Dequeue(ctx, "worker", TaskTypeReplication)

	// First failure - should retry
	err := q.Fail(ctx, task.ID, errors.New("network error"))
	require.NoError(t, err)

	failed, _ := q.Get(ctx, task.ID)
	assert.Equal(t, taskqueue.StatusPending, failed.Status) // Back to pending for retry
	assert.Equal(t, 1, failed.Attempts)
	assert.Equal(t, "network error", failed.LastError)

	// Fail two more times
	q.Dequeue(ctx, "worker", TaskTypeReplication)
	q.Fail(ctx, task.ID, errors.New("error 2"))
	q.Dequeue(ctx, "worker", TaskTypeReplication)
	q.Fail(ctx, task.ID, errors.New("error 3"))

	// Should be in dead letter now
	deadLetter, _ := q.Get(ctx, task.ID)
	assert.Equal(t, taskqueue.StatusDeadLetter, deadLetter.Status)
	assert.Equal(t, 3, deadLetter.Attempts)
}

func TestMemoryQueue_Stats(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	// Add some tasks
	for i := 0; i < 5; i++ {
		task, _ := NewReplicationTask(ReplicationPayload{
			SourceRegion: "us-west-2",
			SourceBucket: "bucket",
			SourceKey:    "key",
			DestRegion:   "eu-west-1",
		})
		q.Enqueue(ctx, task)
	}

	// Dequeue and complete one
	task, _ := q.Dequeue(ctx, "worker", TaskTypeReplication)
	q.Complete(ctx, task.ID)

	// Dequeue another (running)
	q.Dequeue(ctx, "worker", TaskTypeReplication)

	stats, err := q.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), stats.Pending)
	assert.Equal(t, int64(1), stats.Running)
	assert.Equal(t, int64(1), stats.Completed)
}

func TestMemoryQueue_Priority(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	// Add low priority task first
	lowTask, _ := NewReplicationTask(ReplicationPayload{
		SourceRegion: "us-west-2",
		SourceBucket: "bucket",
		SourceKey:    "low",
		DestRegion:   "eu-west-1",
	})
	lowTask.Priority = taskqueue.PriorityLow
	q.Enqueue(ctx, lowTask)

	// Add high priority task second
	highTask, _ := NewReplicationTask(ReplicationPayload{
		SourceRegion: "us-west-2",
		SourceBucket: "bucket",
		SourceKey:    "high",
		DestRegion:   "eu-west-1",
	})
	highTask.Priority = taskqueue.PriorityHigh
	q.Enqueue(ctx, highTask)

	// Should get high priority first
	first, _ := q.Dequeue(ctx, "worker", TaskTypeReplication)
	assert.Equal(t, highTask.ID, first.ID)

	// Then low priority
	second, _ := q.Dequeue(ctx, "worker", TaskTypeReplication)
	assert.Equal(t, lowTask.ID, second.ID)
}

func TestWorker_ProcessTasks(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := taskqueue.NewMemoryQueue()
	defer q.Close()

	// Track processed tasks
	var processed int32

	// Create a simple handler
	handler := &testHandler{
		taskType: TaskTypeReplication,
		handleFn: func(ctx context.Context, task *taskqueue.Task) error {
			atomic.AddInt32(&processed, 1)
			return nil
		},
	}

	// Create worker
	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           "test-worker",
		Queue:        q,
		PollInterval: 50 * time.Millisecond,
		Concurrency:  2,
	})
	worker.RegisterHandler(handler)

	// Add tasks
	for i := 0; i < 5; i++ {
		task, _ := NewReplicationTask(ReplicationPayload{
			SourceRegion: "us-west-2",
			SourceBucket: "bucket",
			SourceKey:    "key",
			DestRegion:   "eu-west-1",
		})
		q.Enqueue(ctx, task)
	}

	// Start worker
	worker.Start(ctx)

	// Wait for processing
	time.Sleep(300 * time.Millisecond)

	worker.Stop()

	// All tasks should be processed
	assert.Equal(t, int32(5), atomic.LoadInt32(&processed))

	stats, _ := q.Stats(ctx)
	assert.Equal(t, int64(5), stats.Completed)
	assert.Equal(t, int64(0), stats.Pending)
}

// testHandler is a simple handler for testing
type testHandler struct {
	taskType taskqueue.TaskType
	handleFn func(context.Context, *taskqueue.Task) error
}

func (h *testHandler) Type() taskqueue.TaskType {
	return h.taskType
}

func (h *testHandler) Handle(ctx context.Context, task *taskqueue.Task) error {
	return h.handleFn(ctx, task)
}
