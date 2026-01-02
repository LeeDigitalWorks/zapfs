// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/google/uuid"
)

// ============================================================================
// Object Lock Operations
// ============================================================================

func (v *Vitess) GetObjectLockConfiguration(ctx context.Context, bucket string) (*s3types.ObjectLockConfiguration, error) {
	var configJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT config_json FROM bucket_object_lock WHERE bucket = ?
	`, bucket).Scan(&configJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrObjectLockNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get object lock config: %w", err)
	}

	var config s3types.ObjectLockConfiguration
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return nil, fmt.Errorf("unmarshal object lock config: %w", err)
	}
	return &config, nil
}

func (v *Vitess) SetObjectLockConfiguration(ctx context.Context, bucket string, config *s3types.ObjectLockConfiguration) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal object lock config: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_object_lock (bucket, config_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE config_json = VALUES(config_json), updated_at = VALUES(updated_at)
	`, bucket, string(configJSON), now, now)
	if err != nil {
		return fmt.Errorf("set object lock config: %w", err)
	}
	return nil
}

func (v *Vitess) GetObjectRetention(ctx context.Context, bucket, key string) (*s3types.ObjectLockRetention, error) {
	var mode, retainUntilDate string
	err := v.db.QueryRowContext(ctx, `
		SELECT mode, retain_until_date FROM object_retention WHERE bucket = ? AND object_key = ?
	`, bucket, key).Scan(&mode, &retainUntilDate)

	if err == sql.ErrNoRows {
		return nil, db.ErrRetentionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get object retention: %w", err)
	}

	return &s3types.ObjectLockRetention{
		Mode:            mode,
		RetainUntilDate: retainUntilDate,
	}, nil
}

func (v *Vitess) SetObjectRetention(ctx context.Context, bucket, key string, retention *s3types.ObjectLockRetention) error {
	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err := v.db.ExecContext(ctx, `
		INSERT INTO object_retention (id, bucket, object_key, mode, retain_until_date, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE mode = VALUES(mode), retain_until_date = VALUES(retain_until_date), updated_at = VALUES(updated_at)
	`, id, bucket, key, retention.Mode, retention.RetainUntilDate, now, now)
	if err != nil {
		return fmt.Errorf("set object retention: %w", err)
	}
	return nil
}

func (v *Vitess) GetObjectLegalHold(ctx context.Context, bucket, key string) (*s3types.ObjectLockLegalHold, error) {
	var status string
	err := v.db.QueryRowContext(ctx, `
		SELECT status FROM object_legal_hold WHERE bucket = ? AND object_key = ?
	`, bucket, key).Scan(&status)

	if err == sql.ErrNoRows {
		return nil, db.ErrLegalHoldNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get object legal hold: %w", err)
	}

	return &s3types.ObjectLockLegalHold{
		Status: status,
	}, nil
}

func (v *Vitess) SetObjectLegalHold(ctx context.Context, bucket, key string, legalHold *s3types.ObjectLockLegalHold) error {
	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err := v.db.ExecContext(ctx, `
		INSERT INTO object_legal_hold (id, bucket, object_key, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE status = VALUES(status), updated_at = VALUES(updated_at)
	`, id, bucket, key, legalHold.Status, now, now)
	if err != nil {
		return fmt.Errorf("set object legal hold: %w", err)
	}
	return nil
}
