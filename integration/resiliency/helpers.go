//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package resiliency

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// ChunkRegistryEntry represents a row in the chunk_registry table
type ChunkRegistryEntry struct {
	ChunkID      string
	Size         int64
	RefCount     int32
	ZeroRefSince sql.NullInt64
	CreatedAt    time.Time
}

// ChunkReplica represents a row in the chunk_replicas table
type ChunkReplica struct {
	ChunkID    string
	ServerID   string
	BackendID  string
	VerifiedAt sql.NullTime
}

// DBClient wraps a database connection for test queries
type DBClient struct {
	db *sql.DB
	t  *testing.T
}

// NewDBClient creates a database client for testing
func NewDBClient(t *testing.T, dsn string) *DBClient {
	t.Helper()

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database connection")

	err = db.Ping()
	require.NoError(t, err, "failed to ping database")

	t.Cleanup(func() {
		db.Close()
	})

	return &DBClient{db: db, t: t}
}

// GetChunkRegistry queries the chunk_registry table for a specific chunk
func (c *DBClient) GetChunkRegistry(chunkID string) (*ChunkRegistryEntry, error) {
	c.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
		SELECT chunk_id, size, ref_count, zero_ref_since, created_at
		FROM chunk_registry WHERE chunk_id = ?`

	row := c.db.QueryRowContext(ctx, query, chunkID)

	var entry ChunkRegistryEntry
	err := row.Scan(&entry.ChunkID, &entry.Size, &entry.RefCount, &entry.ZeroRefSince, &entry.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

// GetChunkRegistryOrFail queries the chunk_registry table and fails on error
func (c *DBClient) GetChunkRegistryOrFail(chunkID string) *ChunkRegistryEntry {
	c.t.Helper()

	entry, err := c.GetChunkRegistry(chunkID)
	require.NoError(c.t, err, "failed to get chunk registry entry for %s", chunkID)
	return entry
}

// GetChunkReplicas queries the chunk_replicas table for a specific chunk
func (c *DBClient) GetChunkReplicas(chunkID string) ([]ChunkReplica, error) {
	c.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
		SELECT chunk_id, server_id, backend_id, verified_at
		FROM chunk_replicas WHERE chunk_id = ?`

	rows, err := c.db.QueryContext(ctx, query, chunkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var replicas []ChunkReplica
	for rows.Next() {
		var r ChunkReplica
		if err := rows.Scan(&r.ChunkID, &r.ServerID, &r.BackendID, &r.VerifiedAt); err != nil {
			return nil, err
		}
		replicas = append(replicas, r)
	}
	return replicas, rows.Err()
}

// GetChunkReplicasOrFail queries the chunk_replicas table and fails on error
func (c *DBClient) GetChunkReplicasOrFail(chunkID string) []ChunkReplica {
	c.t.Helper()

	replicas, err := c.GetChunkReplicas(chunkID)
	require.NoError(c.t, err, "failed to get chunk replicas for %s", chunkID)
	return replicas
}

// CountChunksOnServer counts the number of chunks on a specific server
func (c *DBClient) CountChunksOnServer(serverID string) int {
	c.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `SELECT COUNT(*) FROM chunk_replicas WHERE server_id = ?`
	row := c.db.QueryRowContext(ctx, query, serverID)

	var count int
	err := row.Scan(&count)
	require.NoError(c.t, err, "failed to count chunks on server %s", serverID)
	return count
}

// ListAllChunksInRegistry returns all chunk IDs in the registry
func (c *DBClient) ListAllChunksInRegistry() []string {
	c.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := `SELECT chunk_id FROM chunk_registry`
	rows, err := c.db.QueryContext(ctx, query)
	require.NoError(c.t, err, "failed to list chunks in registry")
	defer rows.Close()

	var chunks []string
	for rows.Next() {
		var chunkID string
		require.NoError(c.t, rows.Scan(&chunkID))
		chunks = append(chunks, chunkID)
	}
	require.NoError(c.t, rows.Err())
	return chunks
}

// ChunkExistsInRegistry checks if a chunk exists in the registry
func (c *DBClient) ChunkExistsInRegistry(chunkID string) bool {
	c.t.Helper()

	_, err := c.GetChunkRegistry(chunkID)
	return err == nil
}

// WaitForCondition waits for a condition to be true, polling at interval
func WaitForCondition(t *testing.T, timeout time.Duration, interval time.Duration, condition func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}
