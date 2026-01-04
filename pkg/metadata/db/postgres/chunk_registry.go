// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
)

// ============================================================================
// Chunk Registry Operations - Centralized RefCount Tracking
// ============================================================================

// IncrementChunkRefCount increments the reference count for a chunk.
// If the chunk doesn't exist, it creates it with ref_count=1.
// Clears zero_ref_since if the chunk was previously at ref_count=0.
func (p *Postgres) IncrementChunkRefCount(ctx context.Context, chunkID string, size int64) error {
	now := time.Now().UnixNano()
	_, err := p.Store.DB().ExecContext(ctx, `
		INSERT INTO chunk_registry (chunk_id, size, ref_count, created_at, zero_ref_since)
		VALUES ($1, $2, 1, $3, 0)
		ON CONFLICT (chunk_id) DO UPDATE SET
			ref_count = chunk_registry.ref_count + 1,
			zero_ref_since = 0
	`, chunkID, size, now)
	if err != nil {
		return fmt.Errorf("increment chunk ref count: %w", err)
	}
	return nil
}

// DecrementChunkRefCount decrements the reference count for a chunk.
// If ref_count reaches 0, sets zero_ref_since to current time for GC grace period.
func (p *Postgres) DecrementChunkRefCount(ctx context.Context, chunkID string) error {
	now := time.Now().UnixNano()
	result, err := p.Store.DB().ExecContext(ctx, `
		UPDATE chunk_registry
		SET ref_count = ref_count - 1,
		    zero_ref_since = CASE WHEN ref_count = 1 THEN $1 ELSE zero_ref_since END
		WHERE chunk_id = $2 AND ref_count > 0
	`, now, chunkID)
	if err != nil {
		return fmt.Errorf("decrement chunk ref count: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return db.ErrChunkNotFound
	}
	return nil
}

// IncrementChunkRefCountBatch increments ref counts for multiple chunks atomically.
func (p *Postgres) IncrementChunkRefCountBatch(ctx context.Context, chunks []db.ChunkInfo) error {
	if len(chunks) == 0 {
		return nil
	}

	for _, chunk := range chunks {
		if err := p.IncrementChunkRefCount(ctx, chunk.ChunkID, chunk.Size); err != nil {
			return fmt.Errorf("batch increment chunk %s: %w", chunk.ChunkID, err)
		}
		if chunk.ServerID != "" {
			if err := p.AddChunkReplica(ctx, chunk.ChunkID, chunk.ServerID, chunk.BackendID); err != nil {
				return fmt.Errorf("batch add replica for chunk %s: %w", chunk.ChunkID, err)
			}
		}
	}
	return nil
}

// DecrementChunkRefCountBatch decrements ref counts for multiple chunks atomically.
func (p *Postgres) DecrementChunkRefCountBatch(ctx context.Context, chunkIDs []string) error {
	if len(chunkIDs) == 0 {
		return nil
	}

	for _, chunkID := range chunkIDs {
		if err := p.DecrementChunkRefCount(ctx, chunkID); err != nil {
			if err != db.ErrChunkNotFound {
				return fmt.Errorf("batch decrement chunk %s: %w", chunkID, err)
			}
		}
	}
	return nil
}

// GetChunkRefCount returns the current reference count for a chunk.
// Used by GC to re-check ref count within a transaction before deleting.
func (p *Postgres) GetChunkRefCount(ctx context.Context, chunkID string) (int, error) {
	var refCount int
	err := p.Store.DB().QueryRowContext(ctx, `
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

// ============================================================================
// Chunk Replica Operations
// ============================================================================

// AddChunkReplica records that a chunk exists on a specific file server.
func (p *Postgres) AddChunkReplica(ctx context.Context, chunkID, serverID, backendID string) error {
	now := time.Now().UnixNano()
	_, err := p.Store.DB().ExecContext(ctx, `
		INSERT INTO chunk_replicas (chunk_id, server_id, backend_id, verified_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (chunk_id, server_id) DO NOTHING
	`, chunkID, serverID, backendID, now)
	if err != nil {
		return fmt.Errorf("add chunk replica: %w", err)
	}
	return nil
}

// RemoveChunkReplica removes the record of a chunk on a file server.
func (p *Postgres) RemoveChunkReplica(ctx context.Context, chunkID, serverID string) error {
	_, err := p.Store.DB().ExecContext(ctx, `
		DELETE FROM chunk_replicas WHERE chunk_id = $1 AND server_id = $2
	`, chunkID, serverID)
	if err != nil {
		return fmt.Errorf("remove chunk replica: %w", err)
	}
	return nil
}

// GetChunkReplicas returns all replica locations for a chunk.
func (p *Postgres) GetChunkReplicas(ctx context.Context, chunkID string) ([]db.ReplicaInfo, error) {
	rows, err := p.Store.DB().QueryContext(ctx, `
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

// GetChunksByServer returns all chunk IDs stored on a specific server.
// Used for server decommissioning and rebalancing.
func (p *Postgres) GetChunksByServer(ctx context.Context, serverID string) ([]string, error) {
	rows, err := p.Store.DB().QueryContext(ctx, `
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

// ============================================================================
// GC Operations
// ============================================================================

// GetZeroRefChunks returns chunks with ref_count=0 where zero_ref_since is older
// than the given time (past the grace period). Includes replica info for deletion.
func (p *Postgres) GetZeroRefChunks(ctx context.Context, olderThan time.Time, limit int) ([]db.ZeroRefChunk, error) {
	cutoff := olderThan.UnixNano()

	rows, err := p.Store.DB().QueryContext(ctx, `
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

	// Group by chunk_id since we're joining with replicas
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

	// Return in order
	result := make([]db.ZeroRefChunk, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		result = append(result, *chunksMap[id])
	}
	return result, nil
}

// DeleteChunkRegistry removes a chunk from the registry.
// The chunk_replicas entries are automatically deleted via CASCADE.
func (p *Postgres) DeleteChunkRegistry(ctx context.Context, chunkID string) error {
	_, err := p.Store.DB().ExecContext(ctx, `
		DELETE FROM chunk_registry WHERE chunk_id = $1
	`, chunkID)
	if err != nil {
		return fmt.Errorf("delete chunk registry: %w", err)
	}
	return nil
}
