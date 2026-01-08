// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

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

// ListObjectVersions lists all versions of objects in a bucket.
func (s *Store) ListObjectVersions(ctx context.Context, bucket, prefix, keyMarker, versionIDMarker, delimiter string, maxKeys int) ([]*types.ObjectVersion, bool, string, string, error) {
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
			// For pagination, we need to skip past the marker. Since we order by
			// object_key ASC, created_at DESC, we need to find versions where:
			// - object_key > keyMarker, OR
			// - object_key = keyMarker AND created_at < marker's created_at
			// First look up the created_at of the version ID marker
			var markerCreatedAt int64
			err := s.QueryRow(ctx, `
				SELECT created_at FROM objects WHERE bucket = $1 AND object_key = $2 AND id = $3
			`, bucket, keyMarker, versionIDMarker).Scan(&markerCreatedAt)
			if err != nil && err != sql.ErrNoRows {
				return nil, false, "", "", fmt.Errorf("lookup version marker: %w", err)
			}
			if markerCreatedAt > 0 {
				query += fmt.Sprintf(" AND (object_key > $%d OR (object_key = $%d AND created_at < $%d))", argIdx, argIdx+1, argIdx+2)
				args = append(args, keyMarker, keyMarker, markerCreatedAt)
				argIdx += 3
			} else {
				// Marker not found, just skip to next key
				query += fmt.Sprintf(" AND object_key > $%d", argIdx)
				args = append(args, keyMarker)
				argIdx++
			}
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

	rows, err := s.Query(ctx, query, args...)
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
		isLatestScanner := s.dialect.ScanBool()

		err := rows.Scan(&idStr, &bucketName, &key, &size, &version, &etag, &createdAt, &deletedAt, &profileID, &storageClass, isLatestScanner.Dest())
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
			IsLatest:       isLatestScanner.Value(),
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

// GetObjectVersion retrieves a specific version of an object.
func (s *Store) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*types.ObjectRef, error) {
	row := s.QueryRow(ctx, `
		SELECT `+ObjectColumns+`
		FROM objects
		WHERE bucket = $1 AND object_key = $2 AND id = $3
	`, bucket, key, versionID)

	return ScanObject(row, s.dialect)
}

// DeleteObjectVersion deletes a specific version of an object.
// After deletion, if the deleted version was the latest, it recalculates
// which remaining version should be marked as latest.
func (s *Store) DeleteObjectVersion(ctx context.Context, bucket, key, versionID string) error {
	// First check if this version exists and if it's the latest
	var isLatest bool
	isLatestScanner := s.dialect.ScanBool()
	err := s.QueryRow(ctx, `
		SELECT is_latest FROM objects WHERE bucket = $1 AND object_key = $2 AND id = $3
	`, bucket, key, versionID).Scan(isLatestScanner.Dest())
	if err != nil {
		if err == sql.ErrNoRows {
			return db.ErrObjectNotFound
		}
		return fmt.Errorf("check version is_latest: %w", err)
	}
	isLatest = isLatestScanner.Value()

	// Delete the version
	result, err := s.Exec(ctx, `
		DELETE FROM objects WHERE bucket = $1 AND object_key = $2 AND id = $3
	`, bucket, key, versionID)
	if err != nil {
		return fmt.Errorf("delete object version: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrObjectNotFound
	}

	// If the deleted version was the latest, promote the next most recent version
	if isLatest {
		// Find the most recent remaining version (by created_at DESC)
		var nextLatestID string
		err = s.QueryRow(ctx, `
			SELECT id FROM objects
			WHERE bucket = $1 AND object_key = $2
			ORDER BY created_at DESC
			LIMIT 1
		`, bucket, key).Scan(&nextLatestID)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("find next latest version: %w", err)
		}

		// Mark the next version as latest (if any remain)
		if nextLatestID != "" {
			_, err = s.Exec(ctx, fmt.Sprintf(`
				UPDATE objects SET is_latest = %s
				WHERE bucket = $1 AND object_key = $2 AND id = $3
			`, s.dialect.BoolLiteral(true)), bucket, key, nextLatestID)
			if err != nil {
				return fmt.Errorf("promote next latest version: %w", err)
			}
		}
	}

	return nil
}

// PutDeleteMarker creates a delete marker for an object (versioned delete).
func (s *Store) PutDeleteMarker(ctx context.Context, bucket, key, ownerID string) (string, error) {
	id := uuid.New()
	now := time.Now().UnixNano()

	// Mark existing versions as not latest
	_, err := s.Exec(ctx, fmt.Sprintf(`
		UPDATE objects SET is_latest = %s WHERE bucket = $1 AND object_key = $2 AND is_latest = %s
	`, s.dialect.BoolLiteral(false), s.dialect.BoolLiteral(true)), bucket, key)
	if err != nil {
		return "", fmt.Errorf("mark old versions not latest: %w", err)
	}

	// Insert delete marker
	// Note: Pass now twice for created_at and deleted_at since MySQL requires separate arguments for each ?
	_, err = s.Exec(ctx, fmt.Sprintf(`
		INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, is_latest)
		VALUES ($1, $2, $3, 0, 1, '', $4, $5, %s)
	`, s.dialect.BoolLiteral(true)), id.String(), bucket, key, now, now)
	if err != nil {
		return "", fmt.Errorf("put delete marker: %w", err)
	}

	return id.String(), nil
}
