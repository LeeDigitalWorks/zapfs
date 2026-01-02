// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/google/uuid"
)

// ============================================================================
// Versioning Operations
// ============================================================================

func (v *Vitess) ListObjectVersions(ctx context.Context, bucket, prefix, keyMarker, versionIDMarker, delimiter string, maxKeys int) ([]*types.ObjectVersion, bool, string, string, error) {
	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, profile_id, is_latest
		FROM objects
		WHERE bucket = ?
	`
	args := []any{bucket}

	if prefix != "" {
		query += " AND object_key LIKE ?"
		args = append(args, prefix+"%")
	}

	if keyMarker != "" {
		if versionIDMarker != "" {
			query += " AND (object_key > ? OR (object_key = ? AND id > ?))"
			args = append(args, keyMarker, keyMarker, versionIDMarker)
		} else {
			query += " AND object_key > ?"
			args = append(args, keyMarker)
		}
	}

	query += " ORDER BY object_key, created_at DESC"

	if maxKeys > 0 {
		query += " LIMIT ?"
		args = append(args, maxKeys+1)
	}

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, "", "", fmt.Errorf("list object versions: %w", err)
	}
	defer rows.Close()

	var versions []*types.ObjectVersion
	for rows.Next() {
		var idStr string
		var bucketName, key, etag string
		var size int64
		var version uint64
		var createdAt, deletedAt int64
		var profileID sql.NullString
		var isLatest int

		err := rows.Scan(&idStr, &bucketName, &key, &size, &version, &etag, &createdAt, &deletedAt, &profileID, &isLatest)
		if err != nil {
			return nil, false, "", "", fmt.Errorf("scan object version: %w", err)
		}

		versions = append(versions, &types.ObjectVersion{
			Key:            key,
			VersionID:      idStr,
			IsLatest:       isLatest == 1,
			IsDeleteMarker: deletedAt > 0,
			LastModified:   createdAt,
			ETag:           etag,
			Size:           size,
			StorageClass:   "STANDARD",
		})
	}

	isTruncated := false
	var nextKeyMarker, nextVersionIDMarker string
	if maxKeys > 0 && len(versions) > maxKeys {
		versions = versions[:maxKeys]
		isTruncated = true
		if len(versions) > 0 {
			last := versions[len(versions)-1]
			nextKeyMarker = last.Key
			nextVersionIDMarker = last.VersionID
		}
	}

	return versions, isTruncated, nextKeyMarker, nextVersionIDMarker, nil
}

func (v *Vitess) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*types.ObjectRef, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = ? AND object_key = ? AND id = ?
	`, bucket, key, versionID)

	return scanObject(row)
}

func (v *Vitess) DeleteObjectVersion(ctx context.Context, bucket, key, versionID string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM objects WHERE bucket = ? AND object_key = ? AND id = ?
	`, bucket, key, versionID)
	if err != nil {
		return fmt.Errorf("delete object version: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrObjectNotFound
	}
	return nil
}

func (v *Vitess) PutDeleteMarker(ctx context.Context, bucket, key, ownerID string) (string, error) {
	id := uuid.New()
	now := time.Now().UnixNano()

	// Mark existing versions as not latest
	_, err := v.db.ExecContext(ctx, `
		UPDATE objects SET is_latest = 0 WHERE bucket = ? AND object_key = ? AND is_latest = 1
	`, bucket, key)
	if err != nil {
		return "", fmt.Errorf("mark old versions not latest: %w", err)
	}

	// Insert delete marker
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, is_latest)
		VALUES (?, ?, ?, 0, 1, '', ?, ?, 1)
	`, id.String(), bucket, key, now, now)
	if err != nil {
		return "", fmt.Errorf("put delete marker: %w", err)
	}

	return id.String(), nil
}
