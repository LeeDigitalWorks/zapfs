package handlers

import (
	"context"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

// GCDecrementPayload is the task payload for GC decrement operations.
type GCDecrementPayload struct {
	ChunkID          string `json:"chunk_id"`
	FileServerAddr   string `json:"file_server_addr"`
	ExpectedRefCount uint32 `json:"expected_ref_count"`
}

// GCDecrementHandler processes GC decrement tasks.
type GCDecrementHandler struct {
	fileClient client.File
}

// NewGCDecrementHandler creates a new GC decrement handler.
func NewGCDecrementHandler(fileClient client.File) *GCDecrementHandler {
	return &GCDecrementHandler{fileClient: fileClient}
}

// Type returns the task type this handler processes.
func (h *GCDecrementHandler) Type() taskqueue.TaskType {
	return taskqueue.TaskTypeGCDecrement
}

// Handle processes a GC decrement task.
func (h *GCDecrementHandler) Handle(ctx context.Context, task *taskqueue.Task) error {
	payload, err := taskqueue.UnmarshalPayload[GCDecrementPayload](task.Payload)
	if err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	result, err := h.fileClient.DecrementRefCount(
		ctx,
		payload.FileServerAddr,
		payload.ChunkID,
		payload.ExpectedRefCount,
	)
	if err != nil {
		logger.Warn().
			Err(err).
			Str("task_id", task.ID).
			Str("chunk_id", payload.ChunkID).
			Str("file_server", payload.FileServerAddr).
			Msg("taskqueue: gc decrement RPC failed")
		return err // Will retry
	}

	if !result.Success {
		// "chunk not found" means it was already deleted - that's fine
		if result.Error == "chunk not found" {
			logger.Debug().
				Str("task_id", task.ID).
				Str("chunk_id", payload.ChunkID).
				Msg("taskqueue: chunk already deleted")
			return nil
		}

		// Log other errors and retry
		logger.Warn().
			Str("task_id", task.ID).
			Str("chunk_id", payload.ChunkID).
			Str("error", result.Error).
			Uint32("new_ref_count", result.NewRefCount).
			Msg("taskqueue: gc decrement failed")
		return fmt.Errorf("decrement failed: %s", result.Error)
	}

	logger.Debug().
		Str("task_id", task.ID).
		Str("chunk_id", payload.ChunkID).
		Uint32("new_ref_count", result.NewRefCount).
		Msg("taskqueue: gc decrement succeeded")

	return nil
}

// NewGCDecrementTask creates a task for a failed decrement operation.
func NewGCDecrementTask(chunkID, fileServerAddr string, expectedRefCount uint32) (*taskqueue.Task, error) {
	payload, err := taskqueue.MarshalPayload(GCDecrementPayload{
		ChunkID:          chunkID,
		FileServerAddr:   fileServerAddr,
		ExpectedRefCount: expectedRefCount,
	})
	if err != nil {
		return nil, err
	}

	return &taskqueue.Task{
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    payload,
		MaxRetries: 10,
		Priority:   taskqueue.PriorityNormal,
	}, nil
}
