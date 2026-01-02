//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package events

import (
	"context"
	"encoding/json"

	"github.com/LeeDigitalWorks/zapfs/pkg/events"
	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

// Publisher is the interface for event notification backends.
type Publisher interface {
	// Name returns the publisher identifier (e.g., "redis", "kafka").
	Name() string

	// Publish sends an event to the configured destination.
	Publish(ctx context.Context, bucket string, event []byte) error

	// Close cleanly shuts down the publisher.
	Close() error
}

// ConfigStore provides access to bucket notification configurations.
type ConfigStore interface {
	// GetNotificationConfig returns the notification configuration for a bucket.
	// Returns nil, nil if no configuration exists.
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
	Name  string `json:"Name"`  // "prefix" or "suffix"
	Value string `json:"Value"`
}

// EventHandler processes event tasks and delivers them to configured publishers.
type EventHandler struct {
	publishers  []Publisher
	configStore ConfigStore
	region      string
}

// NewEventHandler creates an enterprise event handler.
func NewEventHandler(publishers []Publisher, configStore ConfigStore, region string) *EventHandler {
	return &EventHandler{
		publishers:  publishers,
		configStore: configStore,
		region:      region,
	}
}

// Type returns the task type this handler processes.
func (h *EventHandler) Type() taskqueue.TaskType {
	return taskqueue.TaskTypeEvent
}

// Handle processes an event task by delivering it to matching publishers.
func (h *EventHandler) Handle(ctx context.Context, task *taskqueue.Task) error {
	// Check license
	if err := license.CheckFeature(license.FeatureEvents); err != nil {
		logger.Debug().Msg("event notifications not licensed, dropping event")
		return nil // Don't retry - license won't change
	}

	// Parse payload
	var payload taskqueue.EventPayload
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		logger.Warn().Err(err).Msg("failed to unmarshal event payload")
		return nil // Don't retry malformed payload
	}

	// Get bucket notification configuration
	if h.configStore == nil {
		logger.Debug().Str("bucket", payload.Bucket).Msg("no config store, dropping event")
		return nil
	}

	config, err := h.configStore.GetNotificationConfig(ctx, payload.Bucket)
	if err != nil {
		logger.Warn().Err(err).Str("bucket", payload.Bucket).Msg("failed to get notification config")
		return err // Retry
	}

	if config == nil || (len(config.TopicConfigurations) == 0 && len(config.QueueConfigurations) == 0) {
		// No notification configuration for this bucket
		logger.Debug().Str("bucket", payload.Bucket).Msg("no notification config for bucket")
		return nil
	}

	// Check if event matches any configuration
	matched := false
	var configID string

	// Check topic configurations
	for _, tc := range config.TopicConfigurations {
		if h.matchesConfig(payload.EventName, payload.Key, tc.Events, tc.Filter) {
			matched = true
			configID = tc.ID
			break
		}
	}

	// Check queue configurations
	if !matched {
		for _, qc := range config.QueueConfigurations {
			if h.matchesConfig(payload.EventName, payload.Key, qc.Events, qc.Filter) {
				matched = true
				configID = qc.ID
				break
			}
		}
	}

	if !matched {
		logger.Debug().
			Str("bucket", payload.Bucket).
			Str("event", payload.EventName).
			Msg("event does not match any notification configuration")
		return nil
	}

	// Build S3 event
	s3Event := events.BuildS3Event(&payload, h.region, configID)

	// Serialize to JSON
	eventData, err := json.Marshal(s3Event)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to marshal S3 event")
		return nil // Don't retry
	}

	// Publish to all configured publishers
	var lastErr error
	for _, pub := range h.publishers {
		if err := pub.Publish(ctx, payload.Bucket, eventData); err != nil {
			logger.Warn().
				Err(err).
				Str("publisher", pub.Name()).
				Str("bucket", payload.Bucket).
				Str("event", payload.EventName).
				Msg("failed to publish event")
			lastErr = err
			events.EventsDeliveryErrorsTotal.WithLabelValues(pub.Name()).Inc()
		} else {
			events.EventsDeliveredTotal.WithLabelValues(pub.Name()).Inc()
			logger.Debug().
				Str("publisher", pub.Name()).
				Str("bucket", payload.Bucket).
				Str("event", payload.EventName).
				Msg("delivered event")
		}
	}

	return lastErr // Retry if any publisher failed
}

// matchesConfig checks if an event matches a notification configuration.
func (h *EventHandler) matchesConfig(eventName, key string, eventTypes []string, filter *FilterRules) bool {
	// Check if event type matches
	eventMatches := false
	for _, et := range eventTypes {
		if events.MatchesEventType(events.EventType(et), eventName) {
			eventMatches = true
			break
		}
	}
	if !eventMatches {
		return false
	}

	// Check filter rules
	if filter != nil && filter.Key != nil {
		var prefix, suffix string
		for _, rule := range filter.Key.FilterRules {
			switch rule.Name {
			case "prefix":
				prefix = rule.Value
			case "suffix":
				suffix = rule.Value
			}
		}
		if !events.MatchesFilterRules(key, prefix, suffix) {
			return false
		}
	}

	return true
}

// EnterpriseEventHandlers returns the event handlers for enterprise edition.
func EnterpriseEventHandlers(publishers []Publisher, configStore ConfigStore, region string) []taskqueue.Handler {
	if len(publishers) == 0 {
		return nil
	}
	return []taskqueue.Handler{
		NewEventHandler(publishers, configStore, region),
	}
}
