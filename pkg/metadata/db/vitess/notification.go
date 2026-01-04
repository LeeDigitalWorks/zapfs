// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// GetNotificationConfiguration retrieves the notification config for a bucket.
func (v *Vitess) GetNotificationConfiguration(ctx context.Context, bucket string) (*s3types.NotificationConfiguration, error) {
	var configJSON []byte
	err := v.Store.DB().QueryRowContext(ctx, `
		SELECT config FROM notification_configs WHERE bucket = ?
	`, bucket).Scan(&configJSON)

	if err == sql.ErrNoRows {
		return nil, nil // No config = empty response (not an error)
	}
	if err != nil {
		return nil, fmt.Errorf("get notification config: %w", err)
	}

	var config s3types.NotificationConfiguration
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return nil, fmt.Errorf("unmarshal notification config: %w", err)
	}
	return &config, nil
}

// SetNotificationConfiguration stores the notification config for a bucket.
func (v *Vitess) SetNotificationConfiguration(ctx context.Context, bucket string, config *s3types.NotificationConfiguration) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal notification config: %w", err)
	}

	_, err = v.Store.DB().ExecContext(ctx, `
		INSERT INTO notification_configs (bucket, config)
		VALUES (?, ?)
		ON DUPLICATE KEY UPDATE config = VALUES(config), updated_at = CURRENT_TIMESTAMP
	`, bucket, configJSON)
	if err != nil {
		return fmt.Errorf("set notification config: %w", err)
	}
	return nil
}

// DeleteNotificationConfiguration removes the notification config for a bucket.
func (v *Vitess) DeleteNotificationConfiguration(ctx context.Context, bucket string) error {
	_, err := v.Store.DB().ExecContext(ctx, `
		DELETE FROM notification_configs WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete notification config: %w", err)
	}
	return nil
}

// Ensure Vitess implements db.NotificationStore
var _ db.NotificationStore = (*Vitess)(nil)
