//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// AdminClient provides access to file server admin endpoints
type AdminClient struct {
	t       *testing.T
	baseURL string
	client  *http.Client
}

// NewAdminClient creates an admin client for the given debug port address
func NewAdminClient(t *testing.T, addr string) *AdminClient {
	// Use a transport that doesn't keep connections alive to avoid goroutine leaks
	transport := &http.Transport{
		DisableKeepAlives: true,
	}
	client := &http.Client{Transport: transport}

	ac := &AdminClient{
		t:       t,
		baseURL: "http://" + addr,
		client:  client,
	}

	// Register cleanup to close idle connections
	t.Cleanup(func() {
		transport.CloseIdleConnections()
	})

	return ac
}

// ChunkInfo represents chunk metadata returned by the admin API
type ChunkInfo struct {
	ID           string `json:"id"`
	BackendID    string `json:"backend_id"`
	Path         string `json:"path"`
	Size         uint64 `json:"size"`
	Checksum     string `json:"checksum"`
	RefCount     uint32 `json:"ref_count"`
	ZeroRefSince int64  `json:"zero_ref_since"`
}

// IndexStats represents index statistics returned by the admin API
type IndexStats struct {
	TotalChunks   int64 `json:"total_chunks"`
	TotalBytes    int64 `json:"total_bytes"`
	ZeroRefChunks int64 `json:"zero_ref_chunks"`
	ZeroRefBytes  int64 `json:"zero_ref_bytes"`
}

// ForceGCResponse represents the response from force GC
type ForceGCResponse struct {
	Status     string `json:"status"`
	BackendID  string `json:"backend_id,omitempty"`
	WorkersRun int    `json:"workers_run"`
}

// GetChunkInfo retrieves chunk metadata by ID
func (ac *AdminClient) GetChunkInfo(chunkID string) (*ChunkInfo, error) {
	resp, err := ac.client.Get(ac.baseURL + "/admin/chunks/" + chunkID)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("chunk not found: %s", chunkID)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var chunk ChunkInfo
	if err := json.NewDecoder(resp.Body).Decode(&chunk); err != nil {
		return nil, err
	}
	return &chunk, nil
}

// MustGetChunkInfo retrieves chunk metadata and fails the test on error
func (ac *AdminClient) MustGetChunkInfo(chunkID string) *ChunkInfo {
	ac.t.Helper()
	chunk, err := ac.GetChunkInfo(chunkID)
	require.NoError(ac.t, err, "failed to get chunk info for %s", chunkID)
	return chunk
}

// GetIndexStats retrieves index statistics
func (ac *AdminClient) GetIndexStats() (*IndexStats, error) {
	resp, err := ac.client.Get(ac.baseURL + "/admin/index/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var stats IndexStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, err
	}
	return &stats, nil
}

// MustGetIndexStats retrieves index statistics and fails the test on error
func (ac *AdminClient) MustGetIndexStats() *IndexStats {
	ac.t.Helper()
	stats, err := ac.GetIndexStats()
	require.NoError(ac.t, err, "failed to get index stats")
	return stats
}

// ForceGC triggers an immediate GC run on the file server
func (ac *AdminClient) ForceGC(backendID string) (*ForceGCResponse, error) {
	url := ac.baseURL + "/admin/gc/run"
	if backendID != "" {
		url += "?backend=" + backendID
	}

	resp, err := ac.client.Post(url, "application/json", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var gcResp ForceGCResponse
	if err := json.NewDecoder(resp.Body).Decode(&gcResp); err != nil {
		return nil, err
	}
	return &gcResp, nil
}

// MustForceGC triggers GC and fails the test on error
func (ac *AdminClient) MustForceGC(backendID string) *ForceGCResponse {
	ac.t.Helper()
	resp, err := ac.ForceGC(backendID)
	require.NoError(ac.t, err, "failed to force GC")
	return resp
}
