// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package taskqueue

import (
	"context"
	"errors"
	"time"
)

// Common errors
var (
	ErrTaskNotFound       = errors.New("task not found")
	ErrQueueFull          = errors.New("task queue is full")
	ErrQueueClosed        = errors.New("task queue is closed")
	ErrInvalidPayload     = errors.New("invalid task payload")
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
)

// Queue defines the interface for task queue operations.
type Queue interface {
	// Enqueue adds a task to the queue.
	Enqueue(ctx context.Context, task *Task) error

	// Dequeue retrieves the next available task for processing.
	// Returns nil if no tasks are available.
	// Uses FOR UPDATE SKIP LOCKED for concurrent worker support.
	Dequeue(ctx context.Context, workerID string, taskTypes ...TaskType) (*Task, error)

	// Complete marks a task as successfully completed.
	Complete(ctx context.Context, taskID string) error

	// Fail marks a task as failed with an error message.
	// If retries remain, the task will be requeued.
	Fail(ctx context.Context, taskID string, err error) error

	// Cancel marks a task as cancelled.
	Cancel(ctx context.Context, taskID string) error

	// Heartbeat extends the visibility timeout for a running task.
	// Workers should call this periodically for long-running tasks.
	Heartbeat(ctx context.Context, taskID string, workerID string) error

	// Get retrieves a task by ID.
	Get(ctx context.Context, taskID string) (*Task, error)

	// List returns tasks matching the filter.
	List(ctx context.Context, filter TaskFilter) ([]*Task, error)

	// Stats returns queue statistics.
	Stats(ctx context.Context) (*QueueStats, error)

	// Cleanup removes old completed/failed tasks.
	Cleanup(ctx context.Context, olderThan time.Duration) (int, error)

	// Close shuts down the queue.
	Close() error
}

// Handler processes tasks of a specific type.
type Handler interface {
	// Type returns the task type this handler processes.
	Type() TaskType

	// Handle processes the task and returns an error if it failed.
	Handle(ctx context.Context, task *Task) error
}
