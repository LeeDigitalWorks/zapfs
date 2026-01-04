// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

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

func (p *Postgres) GetObjectLockConfiguration(ctx context.Context, bucket string) (*s3types.ObjectLockConfiguration, error) {
	var configJSON []byte
	err := p.Store.DB().QueryRowContext(ctx, `
		SELECT config_json FROM bucket_object_lock WHERE bucket = $1
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

func (p *Postgres) SetObjectLockConfiguration(ctx context.Context, bucket string, config *s3types.ObjectLockConfiguration) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal object lock config: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = p.Store.DB().ExecContext(ctx, `
		INSERT INTO bucket_object_lock (bucket, config_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (bucket) DO UPDATE SET config_json = EXCLUDED.config_json, updated_at = EXCLUDED.updated_at
	`, bucket, string(configJSON), now, now)
	if err != nil {
		return fmt.Errorf("set object lock config: %w", err)
	}
	return nil
}

func (p *Postgres) GetObjectRetention(ctx context.Context, bucket, key string) (*s3types.ObjectLockRetention, error) {
	var mode, retainUntilDate string
	err := p.Store.DB().QueryRowContext(ctx, `
		SELECT mode, retain_until_date FROM object_retention WHERE bucket = $1 AND object_key = $2
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

func (p *Postgres) SetObjectRetention(ctx context.Context, bucket, key string, retention *s3types.ObjectLockRetention) error {
	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err := p.Store.DB().ExecContext(ctx, `
		INSERT INTO object_retention (id, bucket, object_key, mode, retain_until_date, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (bucket, object_key) DO UPDATE SET mode = EXCLUDED.mode, retain_until_date = EXCLUDED.retain_until_date, updated_at = EXCLUDED.updated_at
	`, id, bucket, key, retention.Mode, retention.RetainUntilDate, now, now)
	if err != nil {
		return fmt.Errorf("set object retention: %w", err)
	}
	return nil
}

func (p *Postgres) GetObjectLegalHold(ctx context.Context, bucket, key string) (*s3types.ObjectLockLegalHold, error) {
	var status string
	err := p.Store.DB().QueryRowContext(ctx, `
		SELECT status FROM object_legal_hold WHERE bucket = $1 AND object_key = $2
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

func (p *Postgres) SetObjectLegalHold(ctx context.Context, bucket, key string, legalHold *s3types.ObjectLockLegalHold) error {
	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err := p.Store.DB().ExecContext(ctx, `
		INSERT INTO object_legal_hold (id, bucket, object_key, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (bucket, object_key) DO UPDATE SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at
	`, id, bucket, key, legalHold.Status, now, now)
	if err != nil {
		return fmt.Errorf("set object legal hold: %w", err)
	}
	return nil
}
