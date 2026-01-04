// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
)

// ============================================================================
// Transaction Chunk Registry Operations
// ============================================================================

// IncrementChunkRefCount increments the reference count for a chunk.
// Creates the chunk registry entry if it doesn't exist.
func (t *TxStore) IncrementChunkRefCount(ctx context.Context, chunkID string, size int64) error {
	now := time.Now().UnixNano()

	// Use raw SQL for upsert since we need custom increment logic
	if t.dialect.Name() == "mysql" {
		_, err := t.Exec(ctx, `
			INSERT INTO chunk_registry (chunk_id, size, ref_count, created_at, zero_ref_since)
			VALUES ($1, $2, 1, $3, 0)
			ON DUPLICATE KEY UPDATE
				ref_count = ref_count + 1,
				zero_ref_since = 0
		`, chunkID, size, now)
		if err != nil {
			return fmt.Errorf("increment chunk ref count: %w", err)
		}
	} else {
		_, err := t.Exec(ctx, `
			INSERT INTO chunk_registry (chunk_id, size, ref_count, created_at, zero_ref_since)
			VALUES ($1, $2, 1, $3, 0)
			ON CONFLICT (chunk_id) DO UPDATE SET
				ref_count = chunk_registry.ref_count + 1,
				zero_ref_since = 0
		`, chunkID, size, now)
		if err != nil {
			return fmt.Errorf("increment chunk ref count: %w", err)
		}
	}
	return nil
}

// DecrementChunkRefCount decrements the reference count for a chunk.
// Sets zero_ref_since when count reaches zero.
func (t *TxStore) DecrementChunkRefCount(ctx context.Context, chunkID string) error {
	now := time.Now().UnixNano()
	var result sql.Result
	var err error

	if t.dialect.Name() == "mysql" {
		result, err = t.Exec(ctx, `
			UPDATE chunk_registry
			SET ref_count = ref_count - 1,
			    zero_ref_since = CASE WHEN ref_count = 1 THEN $1 ELSE zero_ref_since END
			WHERE chunk_id = $2 AND ref_count > 0
		`, now, chunkID)
	} else {
		result, err = t.Exec(ctx, `
			UPDATE chunk_registry
			SET ref_count = ref_count - 1,
			    zero_ref_since = CASE WHEN ref_count = 1 THEN $1 ELSE zero_ref_since END
			WHERE chunk_id = $2 AND ref_count > 0
		`, now, chunkID)
	}
	if err != nil {
		return fmt.Errorf("decrement chunk ref count: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return db.ErrChunkNotFound
	}
	return nil
}

// IncrementChunkRefCountBatch increments reference counts for multiple chunks.
func (t *TxStore) IncrementChunkRefCountBatch(ctx context.Context, chunks []db.ChunkInfo) error {
	for _, chunk := range chunks {
		if err := t.IncrementChunkRefCount(ctx, chunk.ChunkID, chunk.Size); err != nil {
			return fmt.Errorf("batch increment chunk %s: %w", chunk.ChunkID, err)
		}
		if chunk.ServerID != "" {
			if err := t.AddChunkReplica(ctx, chunk.ChunkID, chunk.ServerID, chunk.BackendID); err != nil {
				return fmt.Errorf("batch add replica for chunk %s: %w", chunk.ChunkID, err)
			}
		}
	}
	return nil
}

// DecrementChunkRefCountBatch decrements reference counts for multiple chunks.
func (t *TxStore) DecrementChunkRefCountBatch(ctx context.Context, chunkIDs []string) error {
	for _, chunkID := range chunkIDs {
		if err := t.DecrementChunkRefCount(ctx, chunkID); err != nil {
			if err != db.ErrChunkNotFound {
				return fmt.Errorf("batch decrement chunk %s: %w", chunkID, err)
			}
		}
	}
	return nil
}

// GetChunkRefCount returns the reference count for a chunk.
func (t *TxStore) GetChunkRefCount(ctx context.Context, chunkID string) (int, error) {
	var refCount int
	err := t.QueryRow(ctx, `
		SELECT ref_count FROM chunk_registry WHERE chunk_id = $1
	`, chunkID).Scan(&refCount)
	if err == sql.ErrNoRows {
		return 0, db.ErrChunkNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("get chunk ref count: %w", err)
	}
	return refCount, nil
}

// AddChunkReplica adds a replica location for a chunk.
func (t *TxStore) AddChunkReplica(ctx context.Context, chunkID, serverID, backendID string) error {
	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT %sINTO chunk_replicas (chunk_id, server_id, backend_id, verified_at)
		VALUES ($1, $2, $3, $4)
		%s
	`, t.dialect.InsertIgnorePrefix(), t.dialect.InsertIgnoreSuffix("chunk_id, server_id"))

	_, err := t.Exec(ctx, query, chunkID, serverID, backendID, now)
	if err != nil {
		return fmt.Errorf("add chunk replica: %w", err)
	}
	return nil
}

// RemoveChunkReplica removes a replica location for a chunk.
func (t *TxStore) RemoveChunkReplica(ctx context.Context, chunkID, serverID string) error {
	_, err := t.Exec(ctx, `
		DELETE FROM chunk_replicas WHERE chunk_id = $1 AND server_id = $2
	`, chunkID, serverID)
	if err != nil {
		return fmt.Errorf("remove chunk replica: %w", err)
	}
	return nil
}

// GetChunkReplicas returns all replica locations for a chunk.
func (t *TxStore) GetChunkReplicas(ctx context.Context, chunkID string) ([]db.ReplicaInfo, error) {
	rows, err := t.Query(ctx, `
		SELECT server_id, backend_id, verified_at
		FROM chunk_replicas
		WHERE chunk_id = $1
	`, chunkID)
	if err != nil {
		return nil, fmt.Errorf("get chunk replicas: %w", err)
	}
	defer rows.Close()

	var replicas []db.ReplicaInfo
	for rows.Next() {
		var r db.ReplicaInfo
		if err := rows.Scan(&r.ServerID, &r.BackendID, &r.VerifiedAt); err != nil {
			return nil, fmt.Errorf("scan chunk replica: %w", err)
		}
		replicas = append(replicas, r)
	}
	return replicas, rows.Err()
}

// GetChunksByServer returns all chunk IDs stored on a server.
func (t *TxStore) GetChunksByServer(ctx context.Context, serverID string) ([]string, error) {
	rows, err := t.Query(ctx, `
		SELECT chunk_id FROM chunk_replicas WHERE server_id = $1
	`, serverID)
	if err != nil {
		return nil, fmt.Errorf("get chunks by server: %w", err)
	}
	defer rows.Close()

	var chunkIDs []string
	for rows.Next() {
		var chunkID string
		if err := rows.Scan(&chunkID); err != nil {
			return nil, fmt.Errorf("scan chunk id: %w", err)
		}
		chunkIDs = append(chunkIDs, chunkID)
	}
	return chunkIDs, rows.Err()
}

// GetZeroRefChunks returns chunks with zero references older than the cutoff.
func (t *TxStore) GetZeroRefChunks(ctx context.Context, olderThan time.Time, limit int) ([]db.ZeroRefChunk, error) {
	cutoff := olderThan.UnixNano()

	rows, err := t.Query(ctx, `
		SELECT cr.chunk_id, cr.size, rep.server_id, rep.backend_id
		FROM chunk_registry cr
		LEFT JOIN chunk_replicas rep ON cr.chunk_id = rep.chunk_id
		WHERE cr.ref_count = 0
		  AND cr.zero_ref_since > 0
		  AND cr.zero_ref_since < $1
		ORDER BY cr.chunk_id
		LIMIT $2
	`, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("get zero ref chunks: %w", err)
	}
	defer rows.Close()

	chunksMap := make(map[string]*db.ZeroRefChunk)
	var orderedIDs []string

	for rows.Next() {
		var chunkID string
		var size int64
		var serverID, backendID sql.NullString

		if err := rows.Scan(&chunkID, &size, &serverID, &backendID); err != nil {
			return nil, fmt.Errorf("scan zero ref chunk: %w", err)
		}

		chunk, exists := chunksMap[chunkID]
		if !exists {
			chunk = &db.ZeroRefChunk{
				ChunkID:  chunkID,
				Size:     size,
				Replicas: []db.ReplicaInfo{},
			}
			chunksMap[chunkID] = chunk
			orderedIDs = append(orderedIDs, chunkID)
		}

		if serverID.Valid {
			chunk.Replicas = append(chunk.Replicas, db.ReplicaInfo{
				ServerID:  serverID.String,
				BackendID: backendID.String,
			})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate zero ref chunks: %w", err)
	}

	result := make([]db.ZeroRefChunk, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		result = append(result, *chunksMap[id])
	}
	return result, nil
}

// DeleteChunkRegistry deletes a chunk from the registry.
func (t *TxStore) DeleteChunkRegistry(ctx context.Context, chunkID string) error {
	_, err := t.Exec(ctx, `
		DELETE FROM chunk_registry WHERE chunk_id = $1
	`, chunkID)
	if err != nil {
		return fmt.Errorf("delete chunk registry: %w", err)
	}
	return nil
}
