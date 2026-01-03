// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

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

func (p *Postgres) ListObjectVersions(ctx context.Context, bucket, prefix, keyMarker, versionIDMarker, delimiter string, maxKeys int) ([]*types.ObjectVersion, bool, string, string, error) {
	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, profile_id, storage_class, is_latest
		FROM objects
		WHERE bucket = $1
	`
	args := []any{bucket}
	argIdx := 2

	if prefix != "" {
		query += fmt.Sprintf(" AND object_key LIKE $%d", argIdx)
		args = append(args, prefix+"%")
		argIdx++
	}

	if keyMarker != "" {
		if versionIDMarker != "" {
			query += fmt.Sprintf(" AND (object_key > $%d OR (object_key = $%d AND id > $%d))", argIdx, argIdx+1, argIdx+2)
			args = append(args, keyMarker, keyMarker, versionIDMarker)
			argIdx += 3
		} else {
			query += fmt.Sprintf(" AND object_key > $%d", argIdx)
			args = append(args, keyMarker)
			argIdx++
		}
	}

	query += " ORDER BY object_key, created_at DESC"

	if maxKeys > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, maxKeys+1)
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
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
		var profileID, storageClass sql.NullString
		var isLatest bool

		err := rows.Scan(&idStr, &bucketName, &key, &size, &version, &etag, &createdAt, &deletedAt, &profileID, &storageClass, &isLatest)
		if err != nil {
			return nil, false, "", "", fmt.Errorf("scan object version: %w", err)
		}

		sc := "STANDARD"
		if storageClass.Valid && storageClass.String != "" {
			sc = storageClass.String
		}

		versions = append(versions, &types.ObjectVersion{
			Key:            key,
			VersionID:      idStr,
			IsLatest:       isLatest,
			IsDeleteMarker: deletedAt > 0,
			LastModified:   createdAt,
			ETag:           etag,
			Size:           size,
			StorageClass:   sc,
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

func (p *Postgres) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*types.ObjectRef, error) {
	row := p.db.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, storage_class, transitioned_at, transitioned_ref, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = $1 AND object_key = $2 AND id = $3
	`, bucket, key, versionID)

	return scanObject(row)
}

func (p *Postgres) DeleteObjectVersion(ctx context.Context, bucket, key, versionID string) error {
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM objects WHERE bucket = $1 AND object_key = $2 AND id = $3
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

func (p *Postgres) PutDeleteMarker(ctx context.Context, bucket, key, ownerID string) (string, error) {
	id := uuid.New()
	now := time.Now().UnixNano()

	// Mark existing versions as not latest
	_, err := p.db.ExecContext(ctx, `
		UPDATE objects SET is_latest = FALSE WHERE bucket = $1 AND object_key = $2 AND is_latest = TRUE
	`, bucket, key)
	if err != nil {
		return "", fmt.Errorf("mark old versions not latest: %w", err)
	}

	// Insert delete marker
	_, err = p.db.ExecContext(ctx, `
		INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, is_latest)
		VALUES ($1, $2, $3, 0, 1, '', $4, $4, TRUE)
	`, id.String(), bucket, key, now)
	if err != nil {
		return "", fmt.Errorf("put delete marker: %w", err)
	}

	return id.String(), nil
}
