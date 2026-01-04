//go:build !enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package events provides stubs for enterprise event notification handlers.
// The core event emitter and types are in pkg/events (community).
package events

import (
	"context"

	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

// Publisher is the interface for event notification backends.
type Publisher interface {
	// Publish sends an event to the configured destination.
	Publish(ctx context.Context, event []byte) error

	// Close cleanly shuts down the publisher.
	Close() error
}

// ConfigStore provides access to bucket notification configurations.
type ConfigStore interface {
	// GetNotificationConfig returns the notification configuration for a bucket.
	// Returns nil if no configuration exists.
	GetNotificationConfig(ctx context.Context, bucket string) (*NotificationConfig, error)
}

// NotificationConfig represents a bucket's notification configuration.
type NotificationConfig struct {
	TopicConfigurations []TopicConfig `json:"TopicConfigurations,omitempty"`
	QueueConfigurations []QueueConfig `json:"QueueConfigurations,omitempty"`
}

// TopicConfig represents an SNS topic notification configuration.
type TopicConfig struct {
	ID       string       `json:"Id,omitempty"`
	TopicArn string       `json:"TopicArn"`
	Events   []string     `json:"Events"`
	Filter   *FilterRules `json:"Filter,omitempty"`
}

// QueueConfig represents an SQS queue notification configuration.
type QueueConfig struct {
	ID       string       `json:"Id,omitempty"`
	QueueArn string       `json:"QueueArn"`
	Events   []string     `json:"Events"`
	Filter   *FilterRules `json:"Filter,omitempty"`
}

// FilterRules contains key filter rules.
type FilterRules struct {
	Key *KeyFilter `json:"Key,omitempty"`
}

// KeyFilter contains prefix/suffix filter rules.
type KeyFilter struct {
	FilterRules []FilterRule `json:"FilterRules,omitempty"`
}

// FilterRule is a single prefix or suffix filter.
type FilterRule struct {
	Name  string `json:"Name"` // "prefix" or "suffix"
	Value string `json:"Value"`
}

// EventHandler is a stub that drops events in community edition.
type EventHandler struct{}

// NewEventHandler returns a stub handler for community edition.
func NewEventHandler(_ []Publisher, _ ConfigStore, _ string) *EventHandler {
	return &EventHandler{}
}

// Type returns the task type this handler processes.
func (h *EventHandler) Type() taskqueue.TaskType {
	return taskqueue.TaskTypeEvent
}

// Handle drops events in community edition.
func (h *EventHandler) Handle(_ context.Context, _ *taskqueue.Task) error {
	// Community edition: events are silently dropped
	return nil
}

// EnterpriseEventHandlers returns nil in community edition.
func EnterpriseEventHandlers(_ []Publisher, _ ConfigStore, _ string) []taskqueue.Handler {
	return nil
}
