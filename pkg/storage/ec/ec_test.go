package ec

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"zapfs/pkg/storage/backend"
	"zapfs/pkg/storage/index"
	"zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testSetup creates an EC manager with in-memory backends for testing
func testSetup(t *testing.T, scheme types.ECScheme) (*ECManager, *backend.Manager, []*types.Backend) {
	t.Helper()

	totalShards := scheme.TotalShards()

	// Create in-memory indexers
	chunkIdx, err := index.NewMemoryIndexer[types.ChunkID, types.Chunk]()
	require.NoError(t, err)

	ecIdx, err := index.NewMemoryIndexer[uuid.UUID, types.ECGroup]()
	require.NoError(t, err)

	// Create backends (one per shard)
	backendMgr := backend.NewManager()
	backends := make([]*types.Backend, totalShards)

	for i := 0; i < totalShards; i++ {
		id := uuid.New().String()
		backends[i] = &types.Backend{
			ID:         id,
			Type:       types.StorageTypeLocal,
			TotalBytes: 1 << 30, // 1GB
		}

		// Use in-memory backend for tests
		err := backendMgr.AddMemory(id)
		require.NoError(t, err)
	}

	// Create placer
	placer := &testPlacer{backends: backends}

	// Create encoder
	encoder, err := NewReedSolomonCoder(scheme.DataShards, scheme.ParityShards)
	require.NoError(t, err)

	mgr := NewECManager(chunkIdx, ecIdx, placer, backendMgr, encoder, scheme)

	return mgr, backendMgr, backends
}

// testPlacer always returns all backends in order
type testPlacer struct {
	backends []*types.Backend
}

func (p *testPlacer) SelectBackend(ctx context.Context, sizeBytes uint64, preferredID string) (*types.Backend, error) {
	if len(p.backends) == 0 {
		return nil, nil
	}
	return p.backends[0], nil
}

func (p *testPlacer) SelectBackends(ctx context.Context, count int) ([]*types.Backend, error) {
	if count > len(p.backends) {
		count = len(p.backends)
	}
	return p.backends[:count], nil
}

func (p *testPlacer) Refresh(backends []*types.Backend) {
	p.backends = backends
}

// generateTestData creates random data of the specified size
func generateTestData(t *testing.T, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

func TestReedSolomonCoder_EncodeDecodeBasic(t *testing.T) {
	coder, err := NewReedSolomonCoder(4, 2)
	require.NoError(t, err)

	data := generateTestData(t, 1024)

	// Encode
	shards, err := coder.EncodeData(data)
	require.NoError(t, err)
	assert.Len(t, shards, 6) // 4 data + 2 parity

	// All shards should be non-nil
	for i, shard := range shards {
		assert.NotNil(t, shard, "shard %d should not be nil", i)
	}

	// Verify decode with all shards (no reconstruction needed)
	err = coder.DecodeData(shards)
	assert.NoError(t, err)
}

func TestReedSolomonCoder_RecoveryWithMissingShards(t *testing.T) {
	coder, err := NewReedSolomonCoder(4, 2)
	require.NoError(t, err)

	data := generateTestData(t, 1024)

	// Encode
	shards, err := coder.EncodeData(data)
	require.NoError(t, err)

	// Keep original shards for comparison
	originalShards := make([][]byte, len(shards))
	for i, s := range shards {
		originalShards[i] = make([]byte, len(s))
		copy(originalShards[i], s)
	}

	testCases := []struct {
		name        string
		missingIdxs []int
		shouldWork  bool
	}{
		{
			name:        "missing 1 data shard",
			missingIdxs: []int{0},
			shouldWork:  true,
		},
		{
			name:        "missing 2 data shards",
			missingIdxs: []int{0, 1},
			shouldWork:  true,
		},
		{
			name:        "missing 1 parity shard",
			missingIdxs: []int{4},
			shouldWork:  true,
		},
		{
			name:        "missing 2 parity shards",
			missingIdxs: []int{4, 5},
			shouldWork:  true,
		},
		{
			name:        "missing 1 data + 1 parity",
			missingIdxs: []int{0, 5},
			shouldWork:  true,
		},
		{
			name:        "missing 3 shards (exceeds tolerance)",
			missingIdxs: []int{0, 1, 2},
			shouldWork:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Copy original shards
			testShards := make([][]byte, len(originalShards))
			for i, s := range originalShards {
				testShards[i] = make([]byte, len(s))
				copy(testShards[i], s)
			}

			// Remove specified shards (simulate failure)
			for _, idx := range tc.missingIdxs {
				testShards[idx] = nil
			}

			// Attempt reconstruction
			err := coder.DecodeData(testShards)

			if tc.shouldWork {
				require.NoError(t, err, "recovery should succeed")

				// Verify recovered data matches original
				for i := 0; i < coder.DataShards(); i++ {
					assert.Equal(t, originalShards[i], testShards[i],
						"recovered data shard %d should match original", i)
				}
			} else {
				assert.Error(t, err, "recovery should fail with too many missing shards")
			}
		})
	}
}

func TestECManager_EncodeDecodeBasic(t *testing.T) {
	scheme := types.EC4_2
	mgr, _, _ := testSetup(t, scheme)

	ctx := context.Background()
	data := generateTestData(t, 2*1024*1024) // 2MB

	// Encode
	groupID, err := mgr.Encode(ctx, bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, groupID)

	// Decode
	var buf bytes.Buffer
	err = mgr.Decode(ctx, groupID, &buf)
	require.NoError(t, err)

	assert.Equal(t, data, buf.Bytes(), "decoded data should match original")
}

func TestECManager_RecoveryWithMissingShards(t *testing.T) {
	// Note: Recovery at the ECManager level requires backends to still contain
	// the shard data but be temporarily unavailable. The core recovery logic
	// is tested thoroughly in TestReedSolomonCoder_RecoveryWithMissingShards.
	//
	// This test verifies that the decode path works correctly when all shards
	// are available vs when too many are missing.

	scheme := types.EC4_2
	mgr, backendMgr, backends := testSetup(t, scheme)

	ctx := context.Background()
	data := generateTestData(t, 1*1024*1024) // 1MB

	// Encode
	groupID, err := mgr.Encode(ctx, bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	// Test 1: All shards available - should succeed
	t.Run("all shards available", func(t *testing.T) {
		var buf bytes.Buffer
		err := mgr.Decode(ctx, groupID, &buf)
		require.NoError(t, err)
		assert.Equal(t, data, buf.Bytes())
	})

	// Test 2: Too many backends removed - should fail
	t.Run("too many backends removed", func(t *testing.T) {
		// Remove more than parity count (need to remove 3 for 4+2 scheme)
		for i := 0; i < 3; i++ {
			backendMgr.Remove(backends[i].ID)
		}

		var buf bytes.Buffer
		err := mgr.Decode(ctx, groupID, &buf)
		assert.Error(t, err, "should fail with too many backends removed")
		assert.Contains(t, err.Error(), "read quorum not met")
	})
}

func TestECManager_ConvenienceMethods(t *testing.T) {
	scheme := types.EC4_2
	mgr, _, _ := testSetup(t, scheme)

	ctx := context.Background()
	data := generateTestData(t, 512*1024) // 512KB

	// PutChunk
	groupID, err := mgr.PutChunk(ctx, data)
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, groupID)

	// GetChunk
	retrieved, err := mgr.GetChunk(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, data, retrieved)
}

func TestECManager_SmallData(t *testing.T) {
	scheme := types.EC4_2
	mgr, _, _ := testSetup(t, scheme)

	ctx := context.Background()

	testCases := []struct {
		name string
		size int
	}{
		{"1 byte", 1},
		{"100 bytes", 100},
		{"1KB", 1024},
		{"10KB", 10 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := generateTestData(t, tc.size)

			groupID, err := mgr.PutChunk(ctx, data)
			require.NoError(t, err)

			retrieved, err := mgr.GetChunk(ctx, groupID)
			require.NoError(t, err)
			assert.Equal(t, data, retrieved)
		})
	}
}

func TestECManager_DifferentSchemes(t *testing.T) {
	testCases := []struct {
		name   string
		scheme types.ECScheme
	}{
		{"EC4+2", types.EC4_2},
		{"EC6+3", types.EC6_3},
		{"EC8+4", types.EC8_4},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mgr, _, _ := testSetup(t, tc.scheme)

			ctx := context.Background()
			data := generateTestData(t, 1*1024*1024) // 1MB

			groupID, err := mgr.PutChunk(ctx, data)
			require.NoError(t, err)

			retrieved, err := mgr.GetChunk(ctx, groupID)
			require.NoError(t, err)
			assert.Equal(t, data, retrieved)
		})
	}
}

func TestECScheme_ParseECScheme(t *testing.T) {
	testCases := []struct {
		input    string
		expected types.ECScheme
		hasError bool
	}{
		{"4+2", types.EC4_2, false},
		{"6+3", types.EC6_3, false},
		{"8+4", types.EC8_4, false},
		{"10+4", types.EC10_4, false},
		{"", types.EC4_2, false},                                       // Default
		{"3+1", types.ECScheme{DataShards: 3, ParityShards: 1}, false}, // Custom
		{"invalid", types.ECScheme{}, true},
		{"4", types.ECScheme{}, true},
		{"+2", types.ECScheme{}, true},
		{"0+2", types.ECScheme{}, true},
		{"4+0", types.ECScheme{}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result, err := types.ParseECScheme(tc.input)

			if tc.hasError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestECScheme_String(t *testing.T) {
	assert.Equal(t, "4+2", types.EC4_2.String())
	assert.Equal(t, "6+3", types.EC6_3.String())
	assert.Equal(t, "8+4", types.EC8_4.String())
	assert.Equal(t, "10+4", types.EC10_4.String())
}

// BenchmarkEncode benchmarks EC encoding performance
func BenchmarkEncode(b *testing.B) {
	scheme := types.EC4_2
	coder, _ := NewReedSolomonCoder(scheme.DataShards, scheme.ParityShards)
	data := make([]byte, 1*1024*1024) // 1MB
	rand.Read(data)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		_, _ = coder.EncodeData(data)
	}
}

// BenchmarkDecode benchmarks EC reconstruction performance
func BenchmarkDecode(b *testing.B) {
	scheme := types.EC4_2
	coder, _ := NewReedSolomonCoder(scheme.DataShards, scheme.ParityShards)
	data := make([]byte, 1*1024*1024) // 1MB
	rand.Read(data)

	shards, _ := coder.EncodeData(data)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		// Copy shards and remove 2 to force reconstruction
		testShards := make([][]byte, len(shards))
		for j, s := range shards {
			if j < 2 {
				testShards[j] = nil // Simulate missing shards
			} else {
				testShards[j] = make([]byte, len(s))
				copy(testShards[j], s)
			}
		}
		_ = coder.DecodeData(testShards)
	}
}
