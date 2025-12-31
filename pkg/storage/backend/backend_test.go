package backend

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Registry Tests
// ============================================================================

func TestRegister_CustomType(t *testing.T) {
	t.Parallel()

	customType := types.StorageType("test-custom")

	// Register a custom factory
	Register(customType, func(cfg types.BackendConfig) (types.BackendStorage, error) {
		return NewMemoryStorage(), nil
	})

	// Create backend using registered factory
	backend, err := New(types.BackendConfig{Type: customType})
	require.NoError(t, err)
	require.NotNil(t, backend)
	defer backend.Close()

	assert.Equal(t, StorageTypeMemory, backend.Type())
}

func TestNew_UnknownType(t *testing.T) {
	t.Parallel()

	_, err := New(types.BackendConfig{Type: "unknown-type"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown storage type")
}

func TestNew_MemoryType(t *testing.T) {
	t.Parallel()

	backend, err := New(types.BackendConfig{Type: StorageTypeMemory})
	require.NoError(t, err)
	require.NotNil(t, backend)
	defer backend.Close()

	assert.Equal(t, StorageTypeMemory, backend.Type())
}

// ============================================================================
// Manager Tests
// ============================================================================

func TestNewManager(t *testing.T) {
	t.Parallel()

	mgr := NewManager()
	require.NotNil(t, mgr)

	ids := mgr.List()
	assert.Empty(t, ids)
}

func TestManager_Add_Memory(t *testing.T) {
	t.Parallel()

	mgr := NewManager()
	defer mgr.Close()

	err := mgr.AddMemory("test-mem")
	require.NoError(t, err)

	backend, ok := mgr.Get("test-mem")
	assert.True(t, ok)
	require.NotNil(t, backend)
	assert.Equal(t, StorageTypeMemory, backend.Type())
}

func TestManager_Add_UnknownType(t *testing.T) {
	t.Parallel()

	mgr := NewManager()
	defer mgr.Close()

	err := mgr.Add("test", types.BackendConfig{Type: "invalid"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown storage type")
}

func TestManager_Add_ReplacesExisting(t *testing.T) {
	t.Parallel()

	mgr := NewManager()
	defer mgr.Close()

	// Add first backend
	err := mgr.AddMemory("test")
	require.NoError(t, err)

	// Write some data
	backend1, ok := mgr.Get("test")
	require.True(t, ok)
	err = backend1.Write(context.Background(), "key1", strings.NewReader("data1"), 5)
	require.NoError(t, err)

	// Add replacement backend with same ID
	err = mgr.AddMemory("test")
	require.NoError(t, err)

	// New backend should be empty
	backend2, ok := mgr.Get("test")
	require.True(t, ok)
	exists, err := backend2.Exists(context.Background(), "key1")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestManager_Get_NotFound(t *testing.T) {
	t.Parallel()

	mgr := NewManager()
	defer mgr.Close()

	backend, ok := mgr.Get("nonexistent")
	assert.False(t, ok)
	assert.Nil(t, backend)
}

func TestManager_Remove(t *testing.T) {
	t.Parallel()

	mgr := NewManager()
	defer mgr.Close()

	err := mgr.AddMemory("test")
	require.NoError(t, err)

	err = mgr.Remove("test")
	require.NoError(t, err)

	_, ok := mgr.Get("test")
	assert.False(t, ok)
}

func TestManager_Remove_NotFound(t *testing.T) {
	t.Parallel()

	mgr := NewManager()
	defer mgr.Close()

	// Removing non-existent should not error
	err := mgr.Remove("nonexistent")
	assert.NoError(t, err)
}

func TestManager_List(t *testing.T) {
	t.Parallel()

	mgr := NewManager()
	defer mgr.Close()

	err := mgr.AddMemory("backend-a")
	require.NoError(t, err)

	err = mgr.AddMemory("backend-b")
	require.NoError(t, err)

	err = mgr.AddMemory("backend-c")
	require.NoError(t, err)

	ids := mgr.List()
	assert.Len(t, ids, 3)
	assert.Contains(t, ids, "backend-a")
	assert.Contains(t, ids, "backend-b")
	assert.Contains(t, ids, "backend-c")
}

func TestManager_Close(t *testing.T) {
	t.Parallel()

	mgr := NewManager()

	err := mgr.AddMemory("backend1")
	require.NoError(t, err)
	err = mgr.AddMemory("backend2")
	require.NoError(t, err)

	err = mgr.Close()
	require.NoError(t, err)

	// After close, list should be empty
	ids := mgr.List()
	assert.Empty(t, ids)
}

func TestManager_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	mgr := NewManager()
	defer mgr.Close()

	var wg sync.WaitGroup
	ctx := context.Background()

	// Concurrent adds
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = mgr.AddMemory("backend-" + string(rune('a'+id)))
		}(i)
	}

	// Concurrent gets
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.Get("backend-a")
		}()
	}

	// Concurrent list
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.List()
		}()
	}

	wg.Wait()

	// After all operations, verify basic functionality
	_ = mgr.AddMemory("final")
	backend, ok := mgr.Get("final")
	require.True(t, ok)
	err := backend.Write(ctx, "test", strings.NewReader("data"), 4)
	assert.NoError(t, err)
}

// ============================================================================
// MemoryStorage Tests
// ============================================================================

func TestMemoryStorage_Type(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	assert.Equal(t, StorageTypeMemory, ms.Type())
}

func TestMemoryStorage_WriteRead(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	testData := []byte("hello world")

	err := ms.Write(ctx, "key1", bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	reader, err := ms.Read(ctx, "key1")
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestMemoryStorage_Read_NotFound(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	_, err := ms.Read(ctx, "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")
}

func TestMemoryStorage_ReadRange(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	testData := []byte("0123456789")

	err := ms.Write(ctx, "key1", bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	// Read middle portion
	reader, err := ms.ReadRange(ctx, "key1", 3, 4)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("3456"), data)
}

func TestMemoryStorage_ReadRange_PastEnd(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	testData := []byte("short")

	err := ms.Write(ctx, "key1", bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	// Request beyond end
	reader, err := ms.ReadRange(ctx, "key1", 3, 100)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("rt"), data) // Only what's available
}

func TestMemoryStorage_ReadRange_OffsetPastEnd(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	testData := []byte("short")

	err := ms.Write(ctx, "key1", bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	// Offset past end returns empty
	reader, err := ms.ReadRange(ctx, "key1", 100, 10)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Empty(t, data)
}

func TestMemoryStorage_ReadRange_NotFound(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	_, err := ms.ReadRange(ctx, "nonexistent", 0, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")
}

func TestMemoryStorage_Delete(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	err := ms.Write(ctx, "key1", strings.NewReader("data"), 4)
	require.NoError(t, err)

	err = ms.Delete(ctx, "key1")
	require.NoError(t, err)

	exists, err := ms.Exists(ctx, "key1")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestMemoryStorage_Delete_NotFound(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	// Deleting non-existent key should not error
	err := ms.Delete(ctx, "nonexistent")
	assert.NoError(t, err)
}

func TestMemoryStorage_Exists(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	exists, err := ms.Exists(ctx, "key1")
	require.NoError(t, err)
	assert.False(t, exists)

	err = ms.Write(ctx, "key1", strings.NewReader("data"), 4)
	require.NoError(t, err)

	exists, err = ms.Exists(ctx, "key1")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestMemoryStorage_Size(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	testData := []byte("hello world")
	err := ms.Write(ctx, "key1", bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	size, err := ms.Size(ctx, "key1")
	require.NoError(t, err)
	assert.Equal(t, int64(11), size)
}

func TestMemoryStorage_Size_NotFound(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	_, err := ms.Size(ctx, "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")
}

func TestMemoryStorage_Close(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	ctx := context.Background()

	err := ms.Write(ctx, "key1", strings.NewReader("data"), 4)
	require.NoError(t, err)

	err = ms.Close()
	require.NoError(t, err)

	// After close, data should be cleared
	exists, err := ms.Exists(ctx, "key1")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestMemoryStorage_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "key-" + string(rune('a'+id%26))
			data := strings.Repeat("x", id+1)
			_ = ms.Write(ctx, key, strings.NewReader(data), int64(len(data)))
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "key-" + string(rune('a'+id%26))
			reader, err := ms.Read(ctx, key)
			if err == nil {
				reader.Close()
			}
		}(i)
	}

	wg.Wait()
}

func TestMemoryStorage_LargeData(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStorage()
	defer ms.Close()
	ctx := context.Background()

	// 1MB of data
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	err := ms.Write(ctx, "large", bytes.NewReader(largeData), int64(len(largeData)))
	require.NoError(t, err)

	reader, err := ms.Read(ctx, "large")
	require.NoError(t, err)
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, largeData, readData)
}

// ============================================================================
// Local Backend Tests
// ============================================================================

func TestLocal_Type(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	assert.Equal(t, types.StorageTypeLocal, local.Type())
}

func TestLocal_NewLocal_NoPath(t *testing.T) {
	t.Parallel()

	_, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: "",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "path required")
}

func TestLocal_WriteRead(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	testData := []byte("hello local storage")

	err = local.Write(ctx, "test-key", bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	reader, err := local.Read(ctx, "test-key")
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestLocal_WriteRead_NestedPath(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	testData := []byte("nested data")

	err = local.Write(ctx, "a/b/c/nested-file", bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	reader, err := local.Read(ctx, "a/b/c/nested-file")
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestLocal_Read_NotFound(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	_, err = local.Read(ctx, "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")
}

func TestLocal_ReadRange(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	testData := []byte("0123456789ABCDEF")

	err = local.Write(ctx, "range-test", bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	reader, err := local.ReadRange(ctx, "range-test", 4, 8)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("456789AB"), data)
}

func TestLocal_ReadRange_ZeroLength(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	testData := []byte("0123456789")

	err = local.Write(ctx, "test", bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	// Zero length means read to end
	reader, err := local.ReadRange(ctx, "test", 5, 0)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("56789"), data)
}

func TestLocal_Delete(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	err = local.Write(ctx, "to-delete", strings.NewReader("data"), 4)
	require.NoError(t, err)

	exists, _ := local.Exists(ctx, "to-delete")
	assert.True(t, exists)

	err = local.Delete(ctx, "to-delete")
	require.NoError(t, err)

	exists, _ = local.Exists(ctx, "to-delete")
	assert.False(t, exists)
}

func TestLocal_Delete_NotFound(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	// Deleting non-existent should not error
	err = local.Delete(ctx, "nonexistent")
	assert.NoError(t, err)
}

func TestLocal_Exists(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	exists, err := local.Exists(ctx, "test-key")
	require.NoError(t, err)
	assert.False(t, exists)

	err = local.Write(ctx, "test-key", strings.NewReader("data"), 4)
	require.NoError(t, err)

	exists, err = local.Exists(ctx, "test-key")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestLocal_Size(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	testData := []byte("hello world")
	err = local.Write(ctx, "size-test", bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	size, err := local.Size(ctx, "size-test")
	require.NoError(t, err)
	assert.Equal(t, int64(11), size)
}

func TestLocal_Size_NotFound(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	_, err = local.Size(ctx, "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")
}

func TestLocal_WriteOverwrite(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	// Write initial data
	err = local.Write(ctx, "test", strings.NewReader("initial"), 7)
	require.NoError(t, err)

	// Overwrite with new data
	err = local.Write(ctx, "test", strings.NewReader("overwritten data"), 16)
	require.NoError(t, err)

	reader, err := local.Read(ctx, "test")
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "overwritten data", string(data))
}

func TestLocal_FileSync(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	err = local.Write(ctx, "sync-test", strings.NewReader("data to sync"), 12)
	require.NoError(t, err)

	// Verify file exists on disk
	path := filepath.Join(tmpDir, "sync-test")
	_, err = os.Stat(path)
	assert.NoError(t, err)
}

func TestLocal_LargeFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := context.Background()

	local, err := NewLocal(types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)
	defer local.Close()

	// 2MB of data
	largeData := make([]byte, 2*1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	err = local.Write(ctx, "large-file", bytes.NewReader(largeData), int64(len(largeData)))
	require.NoError(t, err)

	reader, err := local.Read(ctx, "large-file")
	require.NoError(t, err)
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, largeData, readData)

	size, err := local.Size(ctx, "large-file")
	require.NoError(t, err)
	assert.Equal(t, int64(2*1024*1024), size)
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestManager_WithLocal(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	mgr := NewManager()
	defer mgr.Close()

	err := mgr.Add("local-backend", types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)

	backend, ok := mgr.Get("local-backend")
	require.True(t, ok)
	require.NotNil(t, backend)

	ctx := context.Background()

	// Write and read through manager
	err = backend.Write(ctx, "mgr-test", strings.NewReader("manager data"), 12)
	require.NoError(t, err)

	reader, err := backend.Read(ctx, "mgr-test")
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "manager data", string(data))
}

func TestManager_MultipleBackends(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	mgr := NewManager()
	defer mgr.Close()

	// Add memory backend
	err := mgr.AddMemory("mem")
	require.NoError(t, err)

	// Add local backend
	err = mgr.Add("local", types.BackendConfig{
		Type: types.StorageTypeLocal,
		Path: tmpDir,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Write to memory backend
	memBackend, _ := mgr.Get("mem")
	err = memBackend.Write(ctx, "key", strings.NewReader("memory data"), 11)
	require.NoError(t, err)

	// Write to local backend
	localBackend, _ := mgr.Get("local")
	err = localBackend.Write(ctx, "key", strings.NewReader("local data"), 10)
	require.NoError(t, err)

	// Read from both and verify they're independent
	memReader, _ := memBackend.Read(ctx, "key")
	memData, _ := io.ReadAll(memReader)
	memReader.Close()
	assert.Equal(t, "memory data", string(memData))

	localReader, _ := localBackend.Read(ctx, "key")
	localData, _ := io.ReadAll(localReader)
	localReader.Close()
	assert.Equal(t, "local data", string(localData))
}
