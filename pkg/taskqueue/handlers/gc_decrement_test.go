// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	mocks "github.com/LeeDigitalWorks/zapfs/mocks/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGCDecrementHandler_Type(t *testing.T) {
	t.Parallel()

	mockFile := mocks.NewMockFile(t)
	handler := NewGCDecrementHandler(mockFile)

	assert.Equal(t, taskqueue.TaskTypeGCDecrement, handler.Type())
}

func TestGCDecrementHandler_Handle_Success(t *testing.T) {
	t.Parallel()

	mockFile := mocks.NewMockFile(t)
	handler := NewGCDecrementHandler(mockFile)

	payload := GCDecrementPayload{
		ChunkID:          "chunk-123",
		FileServerAddr:   "localhost:9001",
		ExpectedRefCount: 5,
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    payloadBytes,
		MaxRetries: 3,
	}

	mockFile.EXPECT().
		DecrementRefCount(mock.Anything, "localhost:9001", "chunk-123", uint32(5)).
		Return(&client.DecrementRefCountResult{
			ChunkID:     "chunk-123",
			NewRefCount: 4,
			Success:     true,
		}, nil)

	err = handler.Handle(context.Background(), task)
	require.NoError(t, err)
}

func TestGCDecrementHandler_Handle_ChunkNotFound_Success(t *testing.T) {
	t.Parallel()

	mockFile := mocks.NewMockFile(t)
	handler := NewGCDecrementHandler(mockFile)

	payload := GCDecrementPayload{
		ChunkID:          "chunk-123",
		FileServerAddr:   "localhost:9001",
		ExpectedRefCount: 1,
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    payloadBytes,
		MaxRetries: 3,
	}

	// Chunk not found is a success - it was already deleted
	mockFile.EXPECT().
		DecrementRefCount(mock.Anything, "localhost:9001", "chunk-123", uint32(1)).
		Return(&client.DecrementRefCountResult{
			ChunkID: "chunk-123",
			Success: false,
			Error:   "chunk not found",
		}, nil)

	err = handler.Handle(context.Background(), task)
	require.NoError(t, err) // Should succeed without error
}

func TestGCDecrementHandler_Handle_RPCError_Retry(t *testing.T) {
	t.Parallel()

	mockFile := mocks.NewMockFile(t)
	handler := NewGCDecrementHandler(mockFile)

	payload := GCDecrementPayload{
		ChunkID:          "chunk-123",
		FileServerAddr:   "localhost:9001",
		ExpectedRefCount: 5,
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    payloadBytes,
		MaxRetries: 3,
	}

	rpcErr := errors.New("connection refused")
	mockFile.EXPECT().
		DecrementRefCount(mock.Anything, "localhost:9001", "chunk-123", uint32(5)).
		Return(nil, rpcErr)

	err = handler.Handle(context.Background(), task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

func TestGCDecrementHandler_Handle_DecrementFailed_Retry(t *testing.T) {
	t.Parallel()

	mockFile := mocks.NewMockFile(t)
	handler := NewGCDecrementHandler(mockFile)

	payload := GCDecrementPayload{
		ChunkID:          "chunk-123",
		FileServerAddr:   "localhost:9001",
		ExpectedRefCount: 5,
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    payloadBytes,
		MaxRetries: 3,
	}

	// Server returned failure (but not "chunk not found")
	mockFile.EXPECT().
		DecrementRefCount(mock.Anything, "localhost:9001", "chunk-123", uint32(5)).
		Return(&client.DecrementRefCountResult{
			ChunkID:     "chunk-123",
			NewRefCount: 5, // Unchanged
			Success:     false,
			Error:       "ref count mismatch",
		}, nil)

	err = handler.Handle(context.Background(), task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ref count mismatch")
}

func TestGCDecrementHandler_Handle_InvalidPayload(t *testing.T) {
	t.Parallel()

	mockFile := mocks.NewMockFile(t)
	handler := NewGCDecrementHandler(mockFile)

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    json.RawMessage(`{invalid json`),
		MaxRetries: 3,
	}

	err := handler.Handle(context.Background(), task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal payload")
}

func TestNewGCDecrementTask(t *testing.T) {
	t.Parallel()

	task, err := NewGCDecrementTask("chunk-123", "localhost:9001", 5)
	require.NoError(t, err)

	assert.Equal(t, taskqueue.TaskTypeGCDecrement, task.Type)
	assert.Equal(t, 10, task.MaxRetries)
	assert.Equal(t, taskqueue.PriorityNormal, task.Priority)

	// Verify payload
	var payload GCDecrementPayload
	err = json.Unmarshal(task.Payload, &payload)
	require.NoError(t, err)
	assert.Equal(t, "chunk-123", payload.ChunkID)
	assert.Equal(t, "localhost:9001", payload.FileServerAddr)
	assert.Equal(t, uint32(5), payload.ExpectedRefCount)
}

func TestGCDecrementPayload_JSON(t *testing.T) {
	t.Parallel()

	original := GCDecrementPayload{
		ChunkID:          "chunk-abc-123",
		FileServerAddr:   "192.168.1.100:9001",
		ExpectedRefCount: 42,
	}

	// Marshal
	data, err := json.Marshal(original)
	require.NoError(t, err)

	// Unmarshal
	var decoded GCDecrementPayload
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original, decoded)
}

func TestGCDecrementHandler_Handle_ContextCancelled(t *testing.T) {
	t.Parallel()

	mockFile := mocks.NewMockFile(t)
	handler := NewGCDecrementHandler(mockFile)

	payload := GCDecrementPayload{
		ChunkID:          "chunk-123",
		FileServerAddr:   "localhost:9001",
		ExpectedRefCount: 5,
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    payloadBytes,
		MaxRetries: 3,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	mockFile.EXPECT().
		DecrementRefCount(mock.Anything, "localhost:9001", "chunk-123", uint32(5)).
		Return(nil, context.Canceled)

	err = handler.Handle(ctx, task)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestGCDecrementHandler_Handle_ZeroRefCount(t *testing.T) {
	t.Parallel()

	mockFile := mocks.NewMockFile(t)
	handler := NewGCDecrementHandler(mockFile)

	// Test decrementing to zero ref count
	payload := GCDecrementPayload{
		ChunkID:          "chunk-123",
		FileServerAddr:   "localhost:9001",
		ExpectedRefCount: 1, // Will become 0
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	task := &taskqueue.Task{
		ID:         "task-1",
		Type:       taskqueue.TaskTypeGCDecrement,
		Payload:    payloadBytes,
		MaxRetries: 3,
	}

	mockFile.EXPECT().
		DecrementRefCount(mock.Anything, "localhost:9001", "chunk-123", uint32(1)).
		Return(&client.DecrementRefCountResult{
			ChunkID:     "chunk-123",
			NewRefCount: 0,
			Success:     true,
		}, nil)

	err = handler.Handle(context.Background(), task)
	require.NoError(t, err)
}
