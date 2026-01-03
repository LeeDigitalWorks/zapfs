//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package events

import (
	"context"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// NotificationStore is the interface for accessing notification configurations from the database.
// This matches the interface in pkg/metadata/db/db.go.
type NotificationStore interface {
	GetNotificationConfiguration(ctx context.Context, bucket string) (*s3types.NotificationConfiguration, error)
}

// DBConfigStore adapts a database NotificationStore to the ConfigStore interface
// required by EventHandler.
type DBConfigStore struct {
	db NotificationStore
}

// NewDBConfigStore creates a ConfigStore that reads from the database.
func NewDBConfigStore(db NotificationStore) *DBConfigStore {
	return &DBConfigStore{db: db}
}

// GetNotificationConfig retrieves the notification configuration for a bucket
// and converts it to the format expected by EventHandler.
func (s *DBConfigStore) GetNotificationConfig(ctx context.Context, bucket string) (*NotificationConfig, error) {
	if s.db == nil {
		return nil, nil
	}

	cfg, err := s.db.GetNotificationConfiguration(ctx, bucket)
	if err != nil {
		return nil, err
	}

	if cfg == nil {
		return nil, nil
	}

	// Convert s3types.NotificationConfiguration to events.NotificationConfig
	return convertNotificationConfig(cfg), nil
}

// convertNotificationConfig converts the S3 types to enterprise events types.
func convertNotificationConfig(cfg *s3types.NotificationConfiguration) *NotificationConfig {
	if cfg == nil {
		return nil
	}

	result := &NotificationConfig{}

	// Convert topic configurations
	for _, tc := range cfg.TopicConfigurations {
		result.TopicConfigurations = append(result.TopicConfigurations, TopicConfig{
			ID:       tc.ID,
			TopicArn: tc.TopicArn,
			Events:   tc.Events,
			Filter:   convertFilter(tc.Filter),
		})
	}

	// Convert queue configurations
	for _, qc := range cfg.QueueConfigurations {
		result.QueueConfigurations = append(result.QueueConfigurations, QueueConfig{
			ID:       qc.ID,
			QueueArn: qc.QueueArn,
			Events:   qc.Events,
			Filter:   convertFilter(qc.Filter),
		})
	}

	return result
}

// convertFilter converts S3 notification filter to enterprise events filter.
func convertFilter(f *s3types.NotificationFilter) *FilterRules {
	if f == nil || f.Key == nil {
		return nil
	}

	result := &FilterRules{
		Key: &KeyFilter{},
	}

	for _, rule := range f.Key.FilterRules {
		result.Key.FilterRules = append(result.Key.FilterRules, FilterRule{
			Name:  rule.Name,
			Value: rule.Value,
		})
	}

	return result
}
