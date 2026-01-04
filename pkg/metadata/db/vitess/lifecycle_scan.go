// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
)

// ============================================================================
// Lifecycle Scanner State Operations
// ============================================================================

func (v *Vitess) GetScanState(ctx context.Context, bucket string) (*db.LifecycleScanState, error) {
	var state db.LifecycleScanState
	var scanStartedAt, scanCompletedAt int64
	var lastError sql.NullString

	err := v.Store.DB().QueryRowContext(ctx, `
		SELECT bucket, last_key, last_version_id, scan_started_at, scan_completed_at,
		       objects_scanned, actions_enqueued, last_error, consecutive_errors
		FROM lifecycle_scan_state WHERE bucket = ?
	`, bucket).Scan(
		&state.Bucket, &state.LastKey, &state.LastVersionID,
		&scanStartedAt, &scanCompletedAt,
		&state.ObjectsScanned, &state.ActionsEnqueued,
		&lastError, &state.ConsecutiveErrors,
	)

	if err == sql.ErrNoRows {
		return nil, db.ErrLifecycleScanStateNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get scan state: %w", err)
	}

	state.ScanStartedAt = time.Unix(0, scanStartedAt)
	state.ScanCompletedAt = time.Unix(0, scanCompletedAt)
	if lastError.Valid {
		state.LastError = lastError.String
	}

	return &state, nil
}

func (v *Vitess) UpdateScanState(ctx context.Context, state *db.LifecycleScanState) error {
	scanStartedAt := state.ScanStartedAt.UnixNano()
	scanCompletedAt := state.ScanCompletedAt.UnixNano()

	var lastError sql.NullString
	if state.LastError != "" {
		lastError = sql.NullString{String: state.LastError, Valid: true}
	}

	_, err := v.Store.DB().ExecContext(ctx, `
		INSERT INTO lifecycle_scan_state
			(bucket, last_key, last_version_id, scan_started_at, scan_completed_at,
			 objects_scanned, actions_enqueued, last_error, consecutive_errors)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			last_key = VALUES(last_key),
			last_version_id = VALUES(last_version_id),
			scan_started_at = VALUES(scan_started_at),
			scan_completed_at = VALUES(scan_completed_at),
			objects_scanned = VALUES(objects_scanned),
			actions_enqueued = VALUES(actions_enqueued),
			last_error = VALUES(last_error),
			consecutive_errors = VALUES(consecutive_errors)
	`, state.Bucket, state.LastKey, state.LastVersionID,
		scanStartedAt, scanCompletedAt,
		state.ObjectsScanned, state.ActionsEnqueued,
		lastError, state.ConsecutiveErrors)

	if err != nil {
		return fmt.Errorf("update scan state: %w", err)
	}
	return nil
}

func (v *Vitess) ListBucketsWithLifecycle(ctx context.Context) ([]string, error) {
	rows, err := v.Store.DB().QueryContext(ctx, `
		SELECT bucket FROM bucket_lifecycle
	`)
	if err != nil {
		return nil, fmt.Errorf("list buckets with lifecycle: %w", err)
	}
	defer rows.Close()

	var buckets []string
	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}
		buckets = append(buckets, bucket)
	}
	return buckets, rows.Err()
}

func (v *Vitess) GetBucketsNeedingScan(ctx context.Context, minAge time.Duration, limit int) ([]string, error) {
	// Get buckets with lifecycle config that either:
	// 1. Have never been scanned (no entry in lifecycle_scan_state)
	// 2. Were last scanned more than minAge ago
	cutoff := time.Now().Add(-minAge).UnixNano()

	rows, err := v.Store.DB().QueryContext(ctx, `
		SELECT bl.bucket
		FROM bucket_lifecycle bl
		LEFT JOIN lifecycle_scan_state lss ON bl.bucket = lss.bucket
		WHERE lss.bucket IS NULL
		   OR lss.scan_completed_at < ?
		   OR lss.scan_completed_at = 0
		ORDER BY COALESCE(lss.scan_completed_at, 0) ASC
		LIMIT ?
	`, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("get buckets needing scan: %w", err)
	}
	defer rows.Close()

	var buckets []string
	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}
		buckets = append(buckets, bucket)
	}
	return buckets, rows.Err()
}

func (v *Vitess) ResetScanState(ctx context.Context, bucket string) error {
	_, err := v.Store.DB().ExecContext(ctx, `
		DELETE FROM lifecycle_scan_state WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("reset scan state: %w", err)
	}
	return nil
}
