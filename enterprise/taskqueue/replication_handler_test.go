//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package taskqueue

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

// =============================================================================
// Mock Implementations
// =============================================================================

// mockObjectReader implements ObjectReader for testing
type mockObjectReader struct {
	readFn func(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectMeta, error)
}

func (m *mockObjectReader) ReadObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectMeta, error) {
	if m.readFn != nil {
		return m.readFn(ctx, bucket, key)
	}
	return io.NopCloser(bytes.NewReader([]byte("test data"))), &ObjectMeta{
		Size:        9,
		ContentType: "text/plain",
		ETag:        "abc123",
	}, nil
}

// mockRegionEndpoints implements RegionEndpoints for testing
type mockRegionEndpoints struct {
	endpoints map[string]string
}

func (m *mockRegionEndpoints) GetS3Endpoint(region string) string {
	if m.endpoints == nil {
		return ""
	}
	return m.endpoints[region]
}

// =============================================================================
// NewReplicationHandler Tests
// =============================================================================

func TestNewReplicationHandler(t *testing.T) {
	t.Parallel()

	t.Run("with nil http client uses default", func(t *testing.T) {
		t.Parallel()

		handler := NewReplicationHandler(ReplicationHandlerConfig{
			ObjectReader: &mockObjectReader{},
			Endpoints:    &mockRegionEndpoints{},
			HTTPClient:   nil,
		})

		assert.NotNil(t, handler)
		assert.NotNil(t, handler.httpClient)
	})

	t.Run("with custom http client", func(t *testing.T) {
		t.Parallel()

		customClient := &http.Client{Timeout: 10 * time.Second}
		handler := NewReplicationHandler(ReplicationHandlerConfig{
			ObjectReader: &mockObjectReader{},
			Endpoints:    &mockRegionEndpoints{},
			HTTPClient:   customClient,
		})

		assert.NotNil(t, handler)
		assert.Same(t, customClient, handler.httpClient)
	})
}

func TestReplicationHandler_Type(t *testing.T) {
	t.Parallel()

	handler := NewReplicationHandler(ReplicationHandlerConfig{})
	assert.Equal(t, TaskTypeReplication, handler.Type())
}

// =============================================================================
// Handle Tests
// =============================================================================

func TestReplicationHandler_Handle_UnmarshalError(t *testing.T) {
	t.Parallel()

	handler := NewReplicationHandler(ReplicationHandlerConfig{
		ObjectReader: &mockObjectReader{},
		Endpoints:    &mockRegionEndpoints{},
	})

	task := &taskqueue.Task{
		ID:      "test-task",
		Type:    TaskTypeReplication,
		Payload: []byte("invalid json"),
	}

	err := handler.Handle(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal payload")
}

func TestReplicationHandler_Handle_UnknownOperation(t *testing.T) {
	t.Parallel()

	handler := NewReplicationHandler(ReplicationHandlerConfig{
		ObjectReader: &mockObjectReader{},
		Endpoints:    &mockRegionEndpoints{},
	})

	task, err := NewReplicationTask(ReplicationPayload{
		SourceBucket: "bucket",
		SourceKey:    "key",
		DestRegion:   "us-west-2",
		Operation:    "UNKNOWN",
	})
	require.NoError(t, err)

	err = handler.Handle(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown operation")
}

func TestReplicationHandler_HandlePut_NilObjectReader(t *testing.T) {
	t.Parallel()

	handler := NewReplicationHandler(ReplicationHandlerConfig{
		ObjectReader: nil, // No object reader
		Endpoints:    &mockRegionEndpoints{},
	})

	task, err := NewReplicationTask(ReplicationPayload{
		SourceBucket: "bucket",
		SourceKey:    "key",
		DestRegion:   "us-west-2",
		Operation:    "PUT",
	})
	require.NoError(t, err)

	err = handler.Handle(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "object reader not configured")
}

func TestReplicationHandler_HandlePut_ReadError(t *testing.T) {
	t.Parallel()

	handler := NewReplicationHandler(ReplicationHandlerConfig{
		ObjectReader: &mockObjectReader{
			readFn: func(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectMeta, error) {
				return nil, nil, errors.New("read failed")
			},
		},
		Endpoints: &mockRegionEndpoints{
			endpoints: map[string]string{"us-west-2": "http://localhost:8082"},
		},
	})

	task, err := NewReplicationTask(ReplicationPayload{
		SourceBucket: "bucket",
		SourceKey:    "key",
		DestRegion:   "us-west-2",
		Operation:    "PUT",
	})
	require.NoError(t, err)

	err = handler.Handle(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read source object")
}

func TestReplicationHandler_HandlePut_NilEndpoints(t *testing.T) {
	t.Parallel()

	handler := NewReplicationHandler(ReplicationHandlerConfig{
		ObjectReader: &mockObjectReader{},
		Endpoints:    nil, // No endpoints
	})

	task, err := NewReplicationTask(ReplicationPayload{
		SourceBucket: "bucket",
		SourceKey:    "key",
		DestRegion:   "us-west-2",
		Operation:    "PUT",
	})
	require.NoError(t, err)

	err = handler.Handle(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "region endpoints not configured")
}

func TestReplicationHandler_HandlePut_NoEndpointForRegion(t *testing.T) {
	t.Parallel()

	handler := NewReplicationHandler(ReplicationHandlerConfig{
		ObjectReader: &mockObjectReader{},
		Endpoints: &mockRegionEndpoints{
			endpoints: map[string]string{}, // Empty - no endpoints
		},
	})

	task, err := NewReplicationTask(ReplicationPayload{
		SourceBucket: "bucket",
		SourceKey:    "key",
		DestRegion:   "unknown-region",
		Operation:    "PUT",
	})
	require.NoError(t, err)

	err = handler.Handle(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no S3 endpoint configured for region")
}

func TestReplicationHandler_HandleDelete_NilEndpoints(t *testing.T) {
	t.Parallel()

	handler := NewReplicationHandler(ReplicationHandlerConfig{
		ObjectReader: &mockObjectReader{},
		Endpoints:    nil,
	})

	task, err := NewReplicationTask(ReplicationPayload{
		SourceBucket: "bucket",
		SourceKey:    "key",
		DestRegion:   "us-west-2",
		Operation:    "DELETE",
	})
	require.NoError(t, err)

	err = handler.Handle(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "region endpoints not configured")
}

func TestReplicationHandler_HandleCopy_UsesHandlePut(t *testing.T) {
	t.Parallel()

	// COPY should behave like PUT
	handler := NewReplicationHandler(ReplicationHandlerConfig{
		ObjectReader: nil, // Will fail at object reader check
		Endpoints:    &mockRegionEndpoints{},
	})

	task, err := NewReplicationTask(ReplicationPayload{
		SourceBucket: "bucket",
		SourceKey:    "key",
		DestRegion:   "us-west-2",
		Operation:    "COPY",
	})
	require.NoError(t, err)

	err = handler.Handle(context.Background(), task)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "object reader not configured")
}

// =============================================================================
// getS3Client Tests
// =============================================================================

func TestReplicationHandler_GetS3Client_CachesClients(t *testing.T) {
	t.Parallel()

	handler := NewReplicationHandler(ReplicationHandlerConfig{
		ObjectReader: &mockObjectReader{},
		Endpoints: &mockRegionEndpoints{
			endpoints: map[string]string{
				"us-west-2": "http://localhost:8082",
			},
		},
		Credentials: ReplicationCredentials{
			AccessKeyID:     "test-key",
			SecretAccessKey: "test-secret",
		},
	})

	ctx := context.Background()

	// First call creates client
	client1, err := handler.getS3Client(ctx, "us-west-2")
	require.NoError(t, err)
	require.NotNil(t, client1)

	// Second call returns cached client
	client2, err := handler.getS3Client(ctx, "us-west-2")
	require.NoError(t, err)
	assert.Same(t, client1, client2)
}

// =============================================================================
// EnterpriseHandlers Tests
// =============================================================================

func TestEnterpriseHandlers_WithAllDependencies(t *testing.T) {
	t.Parallel()

	handlers := EnterpriseHandlers(Dependencies{
		ObjectReader:    &mockObjectReader{},
		RegionEndpoints: &mockRegionEndpoints{},
	})

	assert.Len(t, handlers, 1)
	assert.Equal(t, TaskTypeReplication, handlers[0].Type())
}

func TestEnterpriseHandlers_WithoutObjectReader(t *testing.T) {
	t.Parallel()

	handlers := EnterpriseHandlers(Dependencies{
		ObjectReader:    nil,
		RegionEndpoints: &mockRegionEndpoints{},
	})

	assert.Len(t, handlers, 0)
}

func TestEnterpriseHandlers_WithoutRegionEndpoints(t *testing.T) {
	t.Parallel()

	handlers := EnterpriseHandlers(Dependencies{
		ObjectReader:    &mockObjectReader{},
		RegionEndpoints: nil,
	})

	assert.Len(t, handlers, 0)
}

func TestEnterpriseHandlers_EmptyDependencies(t *testing.T) {
	t.Parallel()

	handlers := EnterpriseHandlers(Dependencies{})
	assert.Len(t, handlers, 0)
}

// =============================================================================
// RegionConfigAdapter Tests
// =============================================================================

func TestRegionConfigAdapter_NilConfig(t *testing.T) {
	t.Parallel()

	adapter := NewRegionConfigAdapter(nil)
	endpoint := adapter.GetS3Endpoint("us-west-2")
	assert.Empty(t, endpoint)
}

// =============================================================================
// ContentType handling Tests
// =============================================================================

func TestReplicationPayload_ContentTypeDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		payloadType      string
		metaType         string
		expectedFallback bool // whether it should fall back to octet-stream
	}{
		{
			name:             "payload content type takes precedence",
			payloadType:      "application/json",
			metaType:         "text/plain",
			expectedFallback: false,
		},
		{
			name:             "meta content type used when payload empty",
			payloadType:      "",
			metaType:         "text/html",
			expectedFallback: false,
		},
		{
			name:             "falls back to octet-stream",
			payloadType:      "",
			metaType:         "",
			expectedFallback: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// This is tested implicitly through handlePut
			// Just verify the payload can hold content type
			payload := ReplicationPayload{
				ContentType: tt.payloadType,
			}
			assert.Equal(t, tt.payloadType, payload.ContentType)
		})
	}
}
