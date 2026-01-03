//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package events

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

// mockPublisher is a test publisher that records publish calls.
type mockPublisher struct {
	name       string
	published  []publishCall
	shouldFail bool
	failCount  int32
}

type publishCall struct {
	bucket string
	event  []byte
}

func (p *mockPublisher) Name() string {
	return p.name
}

func (p *mockPublisher) Publish(ctx context.Context, bucket string, event []byte) error {
	if p.shouldFail {
		atomic.AddInt32(&p.failCount, 1)
		return errors.New("publish failed")
	}
	p.published = append(p.published, publishCall{bucket: bucket, event: event})
	return nil
}

func (p *mockPublisher) Close() error {
	return nil
}

// mockConfigStore is a test config store.
type mockConfigStore struct {
	configs map[string]*NotificationConfig
}

func (s *mockConfigStore) GetNotificationConfig(ctx context.Context, bucket string) (*NotificationConfig, error) {
	if s.configs == nil {
		return nil, nil
	}
	return s.configs[bucket], nil
}

func TestEventHandler_Type(t *testing.T) {
	t.Parallel()

	h := NewEventHandler(nil, nil, "us-east-1")
	assert.Equal(t, taskqueue.TaskTypeEvent, h.Type())
}

func TestEventHandler_Handle_NoConfigStore(t *testing.T) {
	t.Parallel()

	pub := &mockPublisher{name: "test"}
	h := NewEventHandler([]Publisher{pub}, nil, "us-east-1")

	payload := taskqueue.EventPayload{
		EventName: "s3:ObjectCreated:Put",
		Bucket:    "my-bucket",
		Key:       "test.txt",
	}
	payloadBytes, _ := json.Marshal(payload)

	task := &taskqueue.Task{
		ID:      "task-1",
		Type:    taskqueue.TaskTypeEvent,
		Payload: payloadBytes,
	}

	err := h.Handle(context.Background(), task)
	assert.NoError(t, err)
	assert.Empty(t, pub.published) // No config store, should not publish
}

func TestEventHandler_Handle_NoNotificationConfig(t *testing.T) {
	t.Parallel()

	pub := &mockPublisher{name: "test"}
	configStore := &mockConfigStore{configs: map[string]*NotificationConfig{}}
	h := NewEventHandler([]Publisher{pub}, configStore, "us-east-1")

	payload := taskqueue.EventPayload{
		EventName: "s3:ObjectCreated:Put",
		Bucket:    "my-bucket",
		Key:       "test.txt",
	}
	payloadBytes, _ := json.Marshal(payload)

	task := &taskqueue.Task{
		ID:      "task-1",
		Type:    taskqueue.TaskTypeEvent,
		Payload: payloadBytes,
	}

	err := h.Handle(context.Background(), task)
	assert.NoError(t, err)
	assert.Empty(t, pub.published) // No config for bucket
}

func TestEventHandler_Handle_InvalidPayload(t *testing.T) {
	t.Parallel()

	pub := &mockPublisher{name: "test"}
	h := NewEventHandler([]Publisher{pub}, nil, "us-east-1")

	task := &taskqueue.Task{
		ID:      "task-1",
		Type:    taskqueue.TaskTypeEvent,
		Payload: []byte("invalid json"),
	}

	err := h.Handle(context.Background(), task)
	assert.NoError(t, err) // Should not retry malformed payload
}

func TestEventHandler_MatchesConfig_EventType(t *testing.T) {
	t.Parallel()

	h := &EventHandler{}

	tests := []struct {
		name       string
		eventName  string
		eventTypes []string
		key        string
		filter     *FilterRules
		expected   bool
	}{
		{
			name:       "exact match",
			eventName:  "s3:ObjectCreated:Put",
			eventTypes: []string{"s3:ObjectCreated:Put"},
			expected:   true,
		},
		{
			name:       "wildcard match",
			eventName:  "s3:ObjectCreated:Put",
			eventTypes: []string{"s3:ObjectCreated:*"},
			expected:   true,
		},
		{
			name:       "no match",
			eventName:  "s3:ObjectCreated:Put",
			eventTypes: []string{"s3:ObjectRemoved:*"},
			expected:   false,
		},
		{
			name:       "multiple types with match",
			eventName:  "s3:ObjectRemoved:Delete",
			eventTypes: []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"},
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := h.matchesConfig(tt.eventName, tt.key, tt.eventTypes, tt.filter)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEventHandler_MatchesConfig_KeyFilter(t *testing.T) {
	t.Parallel()

	h := &EventHandler{}

	tests := []struct {
		name       string
		eventName  string
		key        string
		eventTypes []string
		filter     *FilterRules
		expected   bool
	}{
		{
			name:       "prefix match",
			eventName:  "s3:ObjectCreated:Put",
			key:        "images/photo.jpg",
			eventTypes: []string{"s3:ObjectCreated:*"},
			filter: &FilterRules{
				Key: &KeyFilter{
					FilterRules: []FilterRule{
						{Name: "prefix", Value: "images/"},
					},
				},
			},
			expected: true,
		},
		{
			name:       "prefix no match",
			eventName:  "s3:ObjectCreated:Put",
			key:        "documents/doc.pdf",
			eventTypes: []string{"s3:ObjectCreated:*"},
			filter: &FilterRules{
				Key: &KeyFilter{
					FilterRules: []FilterRule{
						{Name: "prefix", Value: "images/"},
					},
				},
			},
			expected: false,
		},
		{
			name:       "suffix match",
			eventName:  "s3:ObjectCreated:Put",
			key:        "photo.jpg",
			eventTypes: []string{"s3:ObjectCreated:*"},
			filter: &FilterRules{
				Key: &KeyFilter{
					FilterRules: []FilterRule{
						{Name: "suffix", Value: ".jpg"},
					},
				},
			},
			expected: true,
		},
		{
			name:       "prefix and suffix match",
			eventName:  "s3:ObjectCreated:Put",
			key:        "images/photo.jpg",
			eventTypes: []string{"s3:ObjectCreated:*"},
			filter: &FilterRules{
				Key: &KeyFilter{
					FilterRules: []FilterRule{
						{Name: "prefix", Value: "images/"},
						{Name: "suffix", Value: ".jpg"},
					},
				},
			},
			expected: true,
		},
		{
			name:       "prefix matches but suffix does not",
			eventName:  "s3:ObjectCreated:Put",
			key:        "images/photo.png",
			eventTypes: []string{"s3:ObjectCreated:*"},
			filter: &FilterRules{
				Key: &KeyFilter{
					FilterRules: []FilterRule{
						{Name: "prefix", Value: "images/"},
						{Name: "suffix", Value: ".jpg"},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := h.matchesConfig(tt.eventName, tt.key, tt.eventTypes, tt.filter)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEventHandler_Handle_PublishSuccess(t *testing.T) {
	t.Parallel()

	pub := &mockPublisher{name: "test"}
	configStore := &mockConfigStore{
		configs: map[string]*NotificationConfig{
			"my-bucket": {
				TopicConfigurations: []TopicConfig{
					{
						ID:       "config-1",
						TopicArn: "arn:aws:sns:us-east-1:123456789:topic",
						Events:   []string{"s3:ObjectCreated:*"},
					},
				},
			},
		},
	}
	h := NewEventHandler([]Publisher{pub}, configStore, "us-east-1")

	payload := taskqueue.EventPayload{
		EventName: "s3:ObjectCreated:Put",
		Bucket:    "my-bucket",
		Key:       "test.txt",
		Size:      1024,
		ETag:      "abc123",
	}
	payloadBytes, _ := json.Marshal(payload)

	task := &taskqueue.Task{
		ID:      "task-1",
		Type:    taskqueue.TaskTypeEvent,
		Payload: payloadBytes,
	}

	err := h.Handle(context.Background(), task)
	require.NoError(t, err)

	// Verify publish was called
	require.Len(t, pub.published, 1)
	assert.Equal(t, "my-bucket", pub.published[0].bucket)

	// Verify event format
	var s3Event map[string]interface{}
	err = json.Unmarshal(pub.published[0].event, &s3Event)
	require.NoError(t, err)
	assert.Contains(t, s3Event, "Records")
}

func TestEventHandler_Handle_PublishFailure(t *testing.T) {
	t.Parallel()

	pub := &mockPublisher{name: "test", shouldFail: true}
	configStore := &mockConfigStore{
		configs: map[string]*NotificationConfig{
			"my-bucket": {
				QueueConfigurations: []QueueConfig{
					{
						ID:       "config-1",
						QueueArn: "arn:aws:sqs:us-east-1:123456789:queue",
						Events:   []string{"s3:ObjectCreated:*"},
					},
				},
			},
		},
	}
	h := NewEventHandler([]Publisher{pub}, configStore, "us-east-1")

	payload := taskqueue.EventPayload{
		EventName: "s3:ObjectCreated:Put",
		Bucket:    "my-bucket",
		Key:       "test.txt",
	}
	payloadBytes, _ := json.Marshal(payload)

	task := &taskqueue.Task{
		ID:      "task-1",
		Type:    taskqueue.TaskTypeEvent,
		Payload: payloadBytes,
	}

	err := h.Handle(context.Background(), task)
	assert.Error(t, err) // Should return error to trigger retry
	assert.Equal(t, int32(1), atomic.LoadInt32(&pub.failCount))
}

func TestEventHandler_Handle_MultiplePublishers(t *testing.T) {
	t.Parallel()

	pub1 := &mockPublisher{name: "pub1"}
	pub2 := &mockPublisher{name: "pub2"}
	configStore := &mockConfigStore{
		configs: map[string]*NotificationConfig{
			"my-bucket": {
				TopicConfigurations: []TopicConfig{
					{
						ID:     "config-1",
						Events: []string{"s3:ObjectCreated:*"},
					},
				},
			},
		},
	}
	h := NewEventHandler([]Publisher{pub1, pub2}, configStore, "us-east-1")

	payload := taskqueue.EventPayload{
		EventName: "s3:ObjectCreated:Put",
		Bucket:    "my-bucket",
		Key:       "test.txt",
	}
	payloadBytes, _ := json.Marshal(payload)

	task := &taskqueue.Task{
		ID:      "task-1",
		Type:    taskqueue.TaskTypeEvent,
		Payload: payloadBytes,
	}

	err := h.Handle(context.Background(), task)
	require.NoError(t, err)

	// Both publishers should receive the event
	assert.Len(t, pub1.published, 1)
	assert.Len(t, pub2.published, 1)
}

func TestEventHandler_Handle_PartialPublishFailure(t *testing.T) {
	t.Parallel()

	pub1 := &mockPublisher{name: "pub1"}
	pub2 := &mockPublisher{name: "pub2", shouldFail: true}
	configStore := &mockConfigStore{
		configs: map[string]*NotificationConfig{
			"my-bucket": {
				TopicConfigurations: []TopicConfig{
					{
						ID:     "config-1",
						Events: []string{"s3:ObjectCreated:*"},
					},
				},
			},
		},
	}
	h := NewEventHandler([]Publisher{pub1, pub2}, configStore, "us-east-1")

	payload := taskqueue.EventPayload{
		EventName: "s3:ObjectCreated:Put",
		Bucket:    "my-bucket",
		Key:       "test.txt",
	}
	payloadBytes, _ := json.Marshal(payload)

	task := &taskqueue.Task{
		ID:      "task-1",
		Type:    taskqueue.TaskTypeEvent,
		Payload: payloadBytes,
	}

	err := h.Handle(context.Background(), task)
	assert.Error(t, err) // Should return error because one publisher failed

	// First publisher should still have received the event
	assert.Len(t, pub1.published, 1)
}

func TestEnterpriseEventHandlers(t *testing.T) {
	t.Parallel()

	t.Run("no publishers", func(t *testing.T) {
		t.Parallel()
		handlers := EnterpriseEventHandlers(nil, nil, "us-east-1")
		assert.Nil(t, handlers)
	})

	t.Run("with publishers", func(t *testing.T) {
		t.Parallel()
		pub := &mockPublisher{name: "test"}
		handlers := EnterpriseEventHandlers([]Publisher{pub}, nil, "us-east-1")
		require.Len(t, handlers, 1)
		assert.Equal(t, taskqueue.TaskTypeEvent, handlers[0].Type())
	})
}
