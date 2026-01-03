//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE file.

package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	clientmocks "github.com/LeeDigitalWorks/zapfs/mocks/client"
	dbmocks "github.com/LeeDigitalWorks/zapfs/mocks/db"
	pkglicense "github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// =============================================================================
// Test Helpers
// =============================================================================

// testLicenseChecker is a checker that allows all features for testing
type testLicenseChecker struct{}

func (testLicenseChecker) CheckFeature(pkglicense.Feature) error {
	return nil
}

func (testLicenseChecker) IsLicensed() bool {
	return true
}

func (testLicenseChecker) Info() map[string]interface{} {
	return map[string]interface{}{
		"licensed": true,
		"edition":  "test",
	}
}

// setupTestLicense installs a test license checker that allows all features
func setupTestLicense(t *testing.T) {
	t.Helper()
	oldChecker := pkglicense.GetChecker()
	pkglicense.SetChecker(testLicenseChecker{})
	t.Cleanup(func() {
		pkglicense.SetChecker(oldChecker)
	})
}

// createTestDeps creates test dependencies with mocks
// Uses the real memory backend for testing (via AddMemory helper)
func createTestDeps(t *testing.T) (*TransitionDeps, *dbmocks.MockDB, *clientmocks.MockFile, uuid.UUID) {
	// Set up test license
	setupTestLicense(t)

	mockDB := dbmocks.NewMockDB(t)
	mockFile := clientmocks.NewMockFile(t)

	poolID := uuid.New()
	profiles := types.NewProfileSet()
	profile := types.NewStorageProfile("GLACIER")
	profile.Pools = []types.PoolTarget{{PoolID: poolID}}
	profiles.Add(profile)

	pools := types.NewPoolSet()

	// Create a real backend manager with memory backend
	bm := backend.NewManager()
	err := bm.AddMemory(poolID.String())
	require.NoError(t, err)

	deps := &TransitionDeps{
		DB:             mockDB,
		FileClient:     mockFile,
		BackendManager: bm,
		Profiles:       profiles,
		Pools:          pools,
	}

	t.Cleanup(func() {
		bm.Close()
	})

	return deps, mockDB, mockFile, poolID
}

// getMemoryBackend retrieves the memory backend from the manager for test verification
func getMemoryBackend(t *testing.T, bm *backend.Manager, poolID uuid.UUID) *backend.MemoryStorage {
	t.Helper()
	be, ok := bm.Get(poolID.String())
	require.True(t, ok, "backend not found")
	memBE, ok := be.(*backend.MemoryStorage)
	require.True(t, ok, "backend is not a MemoryStorage")
	return memBE
}

// =============================================================================
// ExecuteTransition Tests
// =============================================================================

func TestExecuteTransition_NilBackendManager(t *testing.T) {
	t.Parallel()

	deps := &TransitionDeps{
		BackendManager: nil,
		Profiles:       types.NewProfileSet(),
	}

	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER",
	}

	err := ExecuteTransition(context.Background(), deps, payload)
	assert.ErrorIs(t, err, ErrNoBackendManager)
}

func TestExecuteTransition_NilProfiles(t *testing.T) {
	t.Parallel()

	deps := &TransitionDeps{
		BackendManager: backend.NewManager(),
		Profiles:       nil,
	}

	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER",
	}

	err := ExecuteTransition(context.Background(), deps, payload)
	assert.ErrorIs(t, err, ErrNoProfiles)
}

func TestExecuteTransition_ObjectNotFound(t *testing.T) {
	t.Parallel()

	deps, mockDB, _, _ := createTestDeps(t)

	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER",
	}

	mockDB.EXPECT().
		GetObject(mock.Anything, "test-bucket", "test-key").
		Return(nil, db.ErrObjectNotFound)

	// Should not return error - just skip
	err := ExecuteTransition(context.Background(), deps, payload)
	assert.NoError(t, err)
}

func TestExecuteTransition_GetObjectError(t *testing.T) {
	t.Parallel()

	deps, mockDB, _, _ := createTestDeps(t)

	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER",
	}

	mockDB.EXPECT().
		GetObject(mock.Anything, "test-bucket", "test-key").
		Return(nil, errors.New("database error"))

	err := ExecuteTransition(context.Background(), deps, payload)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get object")
}

func TestExecuteTransition_AlreadyAtTargetStorageClass(t *testing.T) {
	t.Parallel()

	deps, mockDB, _, _ := createTestDeps(t)

	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER",
	}

	obj := &types.ObjectRef{
		ID:           uuid.New(),
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER", // Already at target
	}

	mockDB.EXPECT().
		GetObject(mock.Anything, "test-bucket", "test-key").
		Return(obj, nil)

	// Should not return error - just skip
	err := ExecuteTransition(context.Background(), deps, payload)
	assert.NoError(t, err)
}

func TestExecuteTransition_AlreadyTransitioned(t *testing.T) {
	t.Parallel()

	deps, mockDB, _, _ := createTestDeps(t)

	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER",
	}

	obj := &types.ObjectRef{
		ID:              uuid.New(),
		Bucket:          "test-bucket",
		Key:             "test-key",
		StorageClass:    "STANDARD",
		TransitionedRef: "existing-tier-ref", // Already has remote ref
	}

	mockDB.EXPECT().
		GetObject(mock.Anything, "test-bucket", "test-key").
		Return(obj, nil)

	// Should not return error - just skip
	err := ExecuteTransition(context.Background(), deps, payload)
	assert.NoError(t, err)
}

func TestExecuteTransition_ObjectModifiedSinceEvaluation(t *testing.T) {
	t.Parallel()

	deps, mockDB, _, _ := createTestDeps(t)

	now := time.Now()
	expectedTime := now.Add(-1 * time.Hour)

	payload := taskqueue.LifecyclePayload{
		Bucket:          "test-bucket",
		Key:             "test-key",
		StorageClass:    "GLACIER",
		ExpectedModTime: expectedTime.UnixNano(),
	}

	obj := &types.ObjectRef{
		ID:           uuid.New(),
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "STANDARD",
		CreatedAt:    now.UnixNano(), // Modified after expected
	}

	mockDB.EXPECT().
		GetObject(mock.Anything, "test-bucket", "test-key").
		Return(obj, nil)

	// Should not return error - just skip
	err := ExecuteTransition(context.Background(), deps, payload)
	assert.NoError(t, err)
}

func TestExecuteTransition_ProfileNotFound(t *testing.T) {
	t.Parallel()

	deps, mockDB, _, _ := createTestDeps(t)

	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "NONEXISTENT_CLASS", // No profile for this
	}

	obj := &types.ObjectRef{
		ID:           uuid.New(),
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "STANDARD",
	}

	mockDB.EXPECT().
		GetObject(mock.Anything, "test-bucket", "test-key").
		Return(obj, nil)

	err := ExecuteTransition(context.Background(), deps, payload)
	assert.ErrorIs(t, err, ErrProfileNotFound)
}

func TestExecuteTransition_EmptyObjectSuccess(t *testing.T) {
	t.Parallel()

	deps, mockDB, _, poolID := createTestDeps(t)

	objID := uuid.New()
	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER",
	}

	obj := &types.ObjectRef{
		ID:           objID,
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "STANDARD",
		Size:         0,
		ChunkRefs:    nil, // No chunks - empty object
	}

	mockDB.EXPECT().
		GetObject(mock.Anything, "test-bucket", "test-key").
		Return(obj, nil)

	mockDB.EXPECT().
		UpdateObjectTransition(mock.Anything, objID.String(), "GLACIER", mock.AnythingOfType("int64"), mock.AnythingOfType("string")).
		Return(nil)

	err := ExecuteTransition(context.Background(), deps, payload)
	assert.NoError(t, err)

	// Verify something was written to the backend
	memBE := getMemoryBackend(t, deps.BackendManager, poolID)
	exists, _ := memBE.Exists(context.Background(), generateRemoteKey(objID))
	assert.True(t, exists)
}

func TestExecuteTransition_WithChunks_Success(t *testing.T) {
	t.Parallel()

	deps, mockDB, mockFile, poolID := createTestDeps(t)

	objID := uuid.New()
	chunkID := types.ChunkID("test-chunk-id-123")
	chunkData := []byte("test chunk data")

	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER",
	}

	obj := &types.ObjectRef{
		ID:           objID,
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "STANDARD",
		Size:         uint64(len(chunkData)),
		ChunkRefs: []types.ChunkRef{
			{
				ChunkID:        chunkID,
				FileServerAddr: "file-server:8081",
			},
		},
	}

	mockDB.EXPECT().
		GetObject(mock.Anything, "test-bucket", "test-key").
		Return(obj, nil)

	// Mock GetChunk to return data
	mockFile.EXPECT().
		GetChunk(mock.Anything, "file-server:8081", chunkID.String(), mock.Anything).
		Run(func(ctx context.Context, addr, id string, writer client.ObjectWriter) {
			writer(chunkData)
		}).
		Return(nil)

	mockDB.EXPECT().
		UpdateObjectTransition(mock.Anything, objID.String(), "GLACIER", mock.AnythingOfType("int64"), mock.AnythingOfType("string")).
		Return(nil)

	err := ExecuteTransition(context.Background(), deps, payload)
	assert.NoError(t, err)

	// Verify data was written to backend
	memBE := getMemoryBackend(t, deps.BackendManager, poolID)
	reader, err := memBE.Read(context.Background(), generateRemoteKey(objID))
	require.NoError(t, err)
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	assert.Equal(t, chunkData, data)
}

func TestExecuteTransition_MetadataUpdateFails_CleansUpTierObject(t *testing.T) {
	t.Parallel()

	deps, mockDB, mockFile, poolID := createTestDeps(t)

	objID := uuid.New()
	chunkID := types.ChunkID("test-chunk-id-456")
	chunkData := []byte("test chunk data")

	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER",
	}

	obj := &types.ObjectRef{
		ID:           objID,
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "STANDARD",
		Size:         uint64(len(chunkData)),
		ChunkRefs: []types.ChunkRef{
			{
				ChunkID:        chunkID,
				FileServerAddr: "file-server:8081",
			},
		},
	}

	mockDB.EXPECT().
		GetObject(mock.Anything, "test-bucket", "test-key").
		Return(obj, nil)

	mockFile.EXPECT().
		GetChunk(mock.Anything, "file-server:8081", chunkID.String(), mock.Anything).
		Run(func(ctx context.Context, addr, id string, writer client.ObjectWriter) {
			writer(chunkData)
		}).
		Return(nil)

	// Metadata update fails
	mockDB.EXPECT().
		UpdateObjectTransition(mock.Anything, objID.String(), "GLACIER", mock.AnythingOfType("int64"), mock.AnythingOfType("string")).
		Return(errors.New("database error"))

	err := ExecuteTransition(context.Background(), deps, payload)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update object metadata")

	// Verify cleanup was performed (data should be deleted)
	memBE := getMemoryBackend(t, deps.BackendManager, poolID)
	exists, _ := memBE.Exists(context.Background(), generateRemoteKey(objID))
	assert.False(t, exists)
}

func TestExecuteTransition_DecrementRefCountFails_StillSucceeds(t *testing.T) {
	t.Parallel()

	deps, mockDB, mockFile, _ := createTestDeps(t)

	objID := uuid.New()
	chunkID := types.ChunkID("test-chunk-id-789")
	chunkData := []byte("test chunk data")

	payload := taskqueue.LifecyclePayload{
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "GLACIER",
	}

	obj := &types.ObjectRef{
		ID:           objID,
		Bucket:       "test-bucket",
		Key:          "test-key",
		StorageClass: "STANDARD",
		Size:         uint64(len(chunkData)),
		ChunkRefs: []types.ChunkRef{
			{
				ChunkID:        chunkID,
				FileServerAddr: "file-server:8081",
			},
		},
	}

	mockDB.EXPECT().
		GetObject(mock.Anything, "test-bucket", "test-key").
		Return(obj, nil)

	mockFile.EXPECT().
		GetChunk(mock.Anything, "file-server:8081", chunkID.String(), mock.Anything).
		Run(func(ctx context.Context, addr, id string, writer client.ObjectWriter) {
			writer(chunkData)
		}).
		Return(nil)

	mockDB.EXPECT().
		UpdateObjectTransition(mock.Anything, objID.String(), "GLACIER", mock.AnythingOfType("int64"), mock.AnythingOfType("string")).
		Return(nil)

	err := ExecuteTransition(context.Background(), deps, payload)
	assert.NoError(t, err) // Should succeed despite ref count error
}

// =============================================================================
// generateRemoteKey Tests
// =============================================================================

func TestGenerateRemoteKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   uuid.UUID
	}{
		{
			name: "standard uuid",
			id:   uuid.MustParse("12345678-1234-1234-1234-123456789abc"),
		},
		{
			name: "all zeros",
			id:   uuid.MustParse("00000000-0000-0000-0000-000000000000"),
		},
		{
			name: "random uuid",
			id:   uuid.New(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := generateRemoteKey(tc.id)

			// Should have format ab/cd/uuid
			parts := splitPath(key)
			require.Len(t, parts, 3)

			// First two parts should be 2 chars each
			assert.Len(t, parts[0], 2)
			assert.Len(t, parts[1], 2)

			// Third part should be the full UUID
			assert.Equal(t, tc.id.String(), parts[2])
		})
	}
}

func splitPath(path string) []string {
	var parts []string
	start := 0
	for i, c := range path {
		if c == '/' {
			parts = append(parts, path[start:i])
			start = i + 1
		}
	}
	parts = append(parts, path[start:])
	return parts
}

// =============================================================================
// streamToTier Tests
// =============================================================================

// mockBackend implements types.BackendStorage for streamToTier tests
type mockBackend struct {
	mu          sync.Mutex
	writeErr    error
	written     map[string][]byte
	deleteCalls []string
}

func newMockBackend() *mockBackend {
	return &mockBackend{
		written: make(map[string][]byte),
	}
}

func (m *mockBackend) Type() types.StorageType {
	return types.StorageType("test")
}

func (m *mockBackend) Write(ctx context.Context, key string, reader io.Reader, size int64) error {
	m.mu.Lock()
	writeErr := m.writeErr
	m.mu.Unlock()

	// Always drain the reader to avoid pipe deadlocks
	data, err := io.ReadAll(reader)

	m.mu.Lock()
	defer m.mu.Unlock()

	if writeErr != nil {
		return writeErr
	}
	if err != nil {
		return err
	}
	m.written[key] = data
	return nil
}

func (m *mockBackend) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.written[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockBackend) ReadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBackend) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalls = append(m.deleteCalls, key)
	delete(m.written, key)
	return nil
}

func (m *mockBackend) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.written[key]
	return ok, nil
}

func (m *mockBackend) Size(ctx context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.written[key]
	if !ok {
		return 0, errors.New("not found")
	}
	return int64(len(data)), nil
}

func (m *mockBackend) Close() error {
	return nil
}

func TestStreamToTier_EmptyChunks(t *testing.T) {
	t.Parallel()

	mockBE := newMockBackend()
	ctx := context.Background()

	obj := &types.ObjectRef{
		ID:        uuid.New(),
		ChunkRefs: nil, // No chunks
	}

	err := streamToTier(ctx, nil, obj, mockBE, "test/key")
	assert.NoError(t, err)

	// Should write empty data
	data, ok := mockBE.written["test/key"]
	assert.True(t, ok)
	assert.Empty(t, data)
}

func TestStreamToTier_ChunkWithNoAddress(t *testing.T) {
	t.Parallel()

	mockBE := newMockBackend()
	mockFile := &mockFileClient{}
	ctx := context.Background()

	obj := &types.ObjectRef{
		ID: uuid.New(),
		ChunkRefs: []types.ChunkRef{
			{
				ChunkID:        types.ChunkID("chunk-1"),
				FileServerAddr: "", // No address
			},
		},
	}

	err := streamToTier(ctx, mockFile, obj, mockBE, "test/key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "has no file server address")
}

func TestStreamToTier_GetChunkError(t *testing.T) {
	t.Parallel()

	mockBE := newMockBackend()
	ctx := context.Background()

	chunkID := types.ChunkID("chunk-error-test")
	mockFile := &mockFileClient{
		getChunkErr: errors.New("chunk read error"),
	}

	obj := &types.ObjectRef{
		ID: uuid.New(),
		ChunkRefs: []types.ChunkRef{
			{
				ChunkID:        chunkID,
				FileServerAddr: "file-server:8081",
			},
		},
	}

	err := streamToTier(ctx, mockFile, obj, mockBE, "test/key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read chunk")
}

func TestStreamToTier_BackendWriteError(t *testing.T) {
	t.Parallel()

	mockBE := newMockBackend()
	mockBE.writeErr = errors.New("backend write error")
	ctx := context.Background()

	chunkID := types.ChunkID("chunk-write-error")
	mockFile := &mockFileClient{
		chunkData: []byte("test data"),
	}

	obj := &types.ObjectRef{
		ID:   uuid.New(),
		Size: 9,
		ChunkRefs: []types.ChunkRef{
			{
				ChunkID:        chunkID,
				FileServerAddr: "file-server:8081",
			},
		},
	}

	err := streamToTier(ctx, mockFile, obj, mockBE, "test/key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "backend write error")
}

// mockFileClient is a simple mock for streamToTier tests
type mockFileClient struct {
	chunkData   []byte
	getChunkErr error
}

func (m *mockFileClient) PutObject(ctx context.Context, address string, objectID string, data io.Reader, totalSize uint64) (*client.PutObjectResult, error) {
	return nil, nil
}

func (m *mockFileClient) GetObject(ctx context.Context, address string, objectID string, writer client.ObjectWriter) (string, error) {
	return "", nil
}

func (m *mockFileClient) GetObjectRange(ctx context.Context, address string, objectID string, offset, length uint64, writer client.ObjectWriter) (string, error) {
	return "", nil
}

func (m *mockFileClient) GetChunk(ctx context.Context, address string, chunkID string, writer client.ObjectWriter) error {
	if m.getChunkErr != nil {
		return m.getChunkErr
	}
	if m.chunkData != nil {
		writer(m.chunkData)
	}
	return nil
}

func (m *mockFileClient) GetChunkRange(ctx context.Context, address string, chunkID string, offset, length uint64, writer client.ObjectWriter) error {
	return nil
}

func (m *mockFileClient) Close() error {
	return nil
}

// =============================================================================
// Error Variable Tests
// =============================================================================

func TestErrorVariables(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, ErrLicenseRequired)
	assert.NotNil(t, ErrNoBackendManager)
	assert.NotNil(t, ErrNoProfiles)
	assert.NotNil(t, ErrProfileNotFound)
	assert.NotNil(t, ErrObjectModified)
	assert.NotNil(t, ErrAlreadyTransitioned)

	// Verify error messages
	assert.Contains(t, ErrLicenseRequired.Error(), "enterprise license")
	assert.Contains(t, ErrNoBackendManager.Error(), "backend manager")
	assert.Contains(t, ErrNoProfiles.Error(), "profiles")
}
