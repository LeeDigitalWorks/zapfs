// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package taskqueue

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Compile-time interface verification
var _ Queue = (*MemoryQueue)(nil)

// MemoryQueue is an in-memory implementation of Queue for testing.
// NOT for production use - tasks are not persisted.
type MemoryQueue struct {
	mu     sync.Mutex
	tasks  map[string]*Task
	closed bool
}

// NewMemoryQueue creates a new in-memory queue.
func NewMemoryQueue() Queue {
	return &MemoryQueue{
		tasks: make(map[string]*Task),
	}
}

func (q *MemoryQueue) Enqueue(ctx context.Context, task *Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	if task.ID == "" {
		task.ID = uuid.New().String()
	}
	if task.Status == "" {
		task.Status = StatusPending
	}
	if task.ScheduledAt.IsZero() {
		task.ScheduledAt = time.Now()
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	task.UpdatedAt = time.Now()
	q.tasks[task.ID] = task
	return nil
}

func (q *MemoryQueue) Dequeue(ctx context.Context, workerID string, taskTypes ...TaskType) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, ErrQueueClosed
	}

	now := time.Now()
	var best *Task

	for _, task := range q.tasks {
		// Skip non-pending or future tasks
		if task.Status != StatusPending {
			continue
		}
		if task.ScheduledAt.After(now) {
			continue
		}
		if !task.RetryAfter.IsZero() && task.RetryAfter.After(now) {
			continue
		}

		// Check task type filter
		if len(taskTypes) > 0 {
			match := false
			for _, t := range taskTypes {
				if task.Type == t {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}

		// Pick highest priority, oldest first
		if best == nil || task.Priority > best.Priority ||
			(task.Priority == best.Priority && task.ScheduledAt.Before(best.ScheduledAt)) {
			best = task
		}
	}

	if best == nil {
		return nil, nil
	}

	// Mark as running
	best.Status = StatusRunning
	best.WorkerID = workerID
	startTime := now
	best.StartedAt = &startTime
	best.UpdatedAt = now

	return best, nil
}

func (q *MemoryQueue) Complete(ctx context.Context, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}

	now := time.Now()
	task.Status = StatusCompleted
	task.CompletedAt = &now
	task.UpdatedAt = now
	return nil
}

func (q *MemoryQueue) Fail(ctx context.Context, taskID string, err error) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}

	task.Attempts++
	task.LastError = err.Error()
	task.UpdatedAt = time.Now()

	if task.Attempts >= task.MaxRetries {
		task.Status = StatusDeadLetter
	} else {
		// Exponential backoff: 1s, 2s, 4s, 8s...
		backoff := time.Duration(1<<task.Attempts) * time.Second
		task.RetryAfter = time.Now().Add(backoff)
		task.Status = StatusPending
		task.WorkerID = ""
	}

	return nil
}

func (q *MemoryQueue) Cancel(ctx context.Context, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}

	task.Status = StatusCancelled
	task.UpdatedAt = time.Now()
	return nil
}

func (q *MemoryQueue) Heartbeat(ctx context.Context, taskID string, workerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}

	if task.WorkerID != workerID || task.Status != StatusRunning {
		return ErrTaskNotFound
	}

	task.UpdatedAt = time.Now()
	return nil
}

func (q *MemoryQueue) Get(ctx context.Context, taskID string) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return task, nil
}

func (q *MemoryQueue) List(ctx context.Context, filter TaskFilter) ([]*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var result []*Task
	for _, task := range q.tasks {
		if filter.Type != "" && task.Type != filter.Type {
			continue
		}
		if filter.Status != "" && task.Status != filter.Status {
			continue
		}
		if filter.Region != "" && task.Region != filter.Region {
			continue
		}
		result = append(result, task)
	}

	// Apply limit/offset
	if filter.Offset > 0 && filter.Offset < len(result) {
		result = result[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(result) {
		result = result[:filter.Limit]
	}

	return result, nil
}

func (q *MemoryQueue) Stats(ctx context.Context) (*QueueStats, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	stats := &QueueStats{
		ByType: make(map[TaskType]int64),
	}

	var oldestPending *time.Time

	for _, task := range q.tasks {
		// Count by status
		switch task.Status {
		case StatusPending:
			stats.Pending++
			if oldestPending == nil || task.ScheduledAt.Before(*oldestPending) {
				oldestPending = &task.ScheduledAt
			}
		case StatusRunning:
			stats.Running++
		case StatusCompleted:
			stats.Completed++
		case StatusFailed:
			stats.Failed++
		case StatusDeadLetter:
			stats.DeadLetter++
		}

		// Count by type
		stats.ByType[task.Type]++
	}

	stats.OldestPending = oldestPending
	return stats, nil
}

func (q *MemoryQueue) Cleanup(ctx context.Context, olderThan time.Duration) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	count := 0

	for id, task := range q.tasks {
		if task.Status == StatusCompleted || task.Status == StatusCancelled {
			if task.CompletedAt != nil && task.CompletedAt.Before(cutoff) {
				delete(q.tasks, id)
				count++
			}
		}
	}

	return count, nil
}

func (q *MemoryQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	return nil
}
