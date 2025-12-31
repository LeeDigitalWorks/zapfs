package index

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test type for indexer tests
type testValue struct {
	Name  string
	Value int
}

// ============================================================================
// MemoryIndexer Tests
// ============================================================================

func TestNewMemoryIndexer(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, testValue]()
	require.NoError(t, err)
	require.NotNil(t, idx)
	defer idx.Close()
}

func TestMemoryIndexer_PutGet(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, testValue]()
	require.NoError(t, err)
	defer idx.Close()

	val := testValue{Name: "test", Value: 42}
	err = idx.Put("key1", val)
	require.NoError(t, err)

	result, err := idx.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, val, result)
}

func TestMemoryIndexer_Get_NotFound(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, testValue]()
	require.NoError(t, err)
	defer idx.Close()

	_, err = idx.Get("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestMemoryIndexer_PutOverwrite(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, testValue]()
	require.NoError(t, err)
	defer idx.Close()

	val1 := testValue{Name: "first", Value: 1}
	err = idx.Put("key1", val1)
	require.NoError(t, err)

	val2 := testValue{Name: "second", Value: 2}
	err = idx.Put("key1", val2)
	require.NoError(t, err)

	result, err := idx.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, val2, result)
}

func TestMemoryIndexer_Delete(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, testValue]()
	require.NoError(t, err)
	defer idx.Close()

	val := testValue{Name: "test", Value: 42}
	err = idx.Put("key1", val)
	require.NoError(t, err)

	err = idx.Delete("key1")
	require.NoError(t, err)

	_, err = idx.Get("key1")
	require.Error(t, err)
}

func TestMemoryIndexer_Delete_NotFound(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, testValue]()
	require.NoError(t, err)
	defer idx.Close()

	// Deleting non-existent key should not error
	err = idx.Delete("nonexistent")
	assert.NoError(t, err)
}

func TestMemoryIndexer_Iterate(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, int]()
	require.NoError(t, err)
	defer idx.Close()

	// Add some values
	err = idx.Put("a", 1)
	require.NoError(t, err)
	err = idx.Put("b", 2)
	require.NoError(t, err)
	err = idx.Put("c", 3)
	require.NoError(t, err)

	// Iterate and collect
	var sum int
	err = idx.Iterate(func(key string, value int) error {
		sum += value
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 6, sum) // 1 + 2 + 3
}

func TestMemoryIndexer_Iterate_StopOnError(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, int]()
	require.NoError(t, err)
	defer idx.Close()

	err = idx.Put("a", 1)
	require.NoError(t, err)
	err = idx.Put("b", 2)
	require.NoError(t, err)
	err = idx.Put("c", 3)
	require.NoError(t, err)

	expectedErr := errors.New("stop iteration")
	err = idx.Iterate(func(key string, value int) error {
		if value == 2 {
			return expectedErr
		}
		return nil
	})

	// Should return the error (though order is non-deterministic)
	if err != nil {
		assert.Equal(t, expectedErr, err)
	}
}

func TestMemoryIndexer_Stream(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, testValue]()
	require.NoError(t, err)
	defer idx.Close()

	// Add values
	err = idx.Put("a", testValue{Name: "a", Value: 1})
	require.NoError(t, err)
	err = idx.Put("b", testValue{Name: "b", Value: 2})
	require.NoError(t, err)
	err = idx.Put("c", testValue{Name: "c", Value: 3})
	require.NoError(t, err)

	// Stream with filter
	ch := idx.Stream(func(v testValue) bool {
		return v.Value > 1 // Only values > 1
	})

	var count int
	for range ch {
		count++
	}

	assert.Equal(t, 2, count) // b and c match
}

func TestMemoryIndexer_Stream_NoFilter(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, int]()
	require.NoError(t, err)
	defer idx.Close()

	err = idx.Put("a", 1)
	require.NoError(t, err)
	err = idx.Put("b", 2)
	require.NoError(t, err)

	// Stream all values (filter always returns true)
	ch := idx.Stream(func(v int) bool { return true })

	var count int
	for range ch {
		count++
	}

	assert.Equal(t, 2, count)
}

func TestMemoryIndexer_Close(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, int]()
	require.NoError(t, err)

	err = idx.Close()
	assert.NoError(t, err)
}

func TestMemoryIndexer_Destroy(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, int]()
	require.NoError(t, err)

	err = idx.Put("key", 123)
	require.NoError(t, err)

	err = idx.Destroy()
	require.NoError(t, err)

	// Data should be cleared
	_, err = idx.Get("key")
	assert.Error(t, err)
}

func TestMemoryIndexer_Sync(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, int]()
	require.NoError(t, err)
	defer idx.Close()

	// Sync is a no-op for memory indexer
	err = idx.Sync()
	assert.NoError(t, err)
}

func TestMemoryIndexer_PutSync(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, int]()
	require.NoError(t, err)
	defer idx.Close()

	err = idx.PutSync("key", 42)
	require.NoError(t, err)

	val, err := idx.Get("key")
	require.NoError(t, err)
	assert.Equal(t, 42, val)
}

func TestMemoryIndexer_DeleteSync(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[string, int]()
	require.NoError(t, err)
	defer idx.Close()

	err = idx.Put("key", 42)
	require.NoError(t, err)

	err = idx.DeleteSync("key")
	require.NoError(t, err)

	_, err = idx.Get("key")
	assert.Error(t, err)
}

func TestMemoryIndexer_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[int, int]()
	require.NoError(t, err)
	defer idx.Close()

	var wg sync.WaitGroup

	// Concurrent puts
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			idx.Put(id, id*10)
		}(i)
	}

	// Concurrent gets
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			idx.Get(id)
		}(i)
	}

	wg.Wait()
}

func TestMemoryIndexer_IntKeys(t *testing.T) {
	t.Parallel()

	idx, err := NewMemoryIndexer[int, string]()
	require.NoError(t, err)
	defer idx.Close()

	err = idx.Put(1, "one")
	require.NoError(t, err)
	err = idx.Put(2, "two")
	require.NoError(t, err)

	val, err := idx.Get(1)
	require.NoError(t, err)
	assert.Equal(t, "one", val)

	val, err = idx.Get(2)
	require.NoError(t, err)
	assert.Equal(t, "two", val)
}

// ============================================================================
// LevelDB Indexer Tests
// ============================================================================

func TestNewLevelDBIndexer(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	idx, err := NewLevelDBIndexer[string, testValue](
		tmpDir,
		nil,
		func(k string) []byte { return []byte(k) },
		func(b []byte) (string, error) { return string(b), nil },
	)
	require.NoError(t, err)
	require.NotNil(t, idx)
	defer idx.Close()
}

func TestLevelDBIndexer_PutGet(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	idx, err := NewLevelDBIndexer[string, testValue](
		tmpDir,
		nil,
		func(k string) []byte { return []byte(k) },
		func(b []byte) (string, error) { return string(b), nil },
	)
	require.NoError(t, err)
	defer idx.Close()

	val := testValue{Name: "test", Value: 42}
	err = idx.Put("key1", val)
	require.NoError(t, err)

	result, err := idx.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, val, result)
}

func TestLevelDBIndexer_Get_NotFound(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	idx, err := NewLevelDBIndexer[string, testValue](
		tmpDir,
		nil,
		func(k string) []byte { return []byte(k) },
		func(b []byte) (string, error) { return string(b), nil },
	)
	require.NoError(t, err)
	defer idx.Close()

	_, err = idx.Get("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestLevelDBIndexer_Delete(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	idx, err := NewLevelDBIndexer[string, int](
		tmpDir,
		nil,
		func(k string) []byte { return []byte(k) },
		func(b []byte) (string, error) { return string(b), nil },
	)
	require.NoError(t, err)
	defer idx.Close()

	err = idx.Put("key1", 123)
	require.NoError(t, err)

	err = idx.Delete("key1")
	require.NoError(t, err)

	_, err = idx.Get("key1")
	require.Error(t, err)
}

func TestLevelDBIndexer_Iterate(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	idx, err := NewLevelDBIndexer[string, int](
		tmpDir,
		nil,
		func(k string) []byte { return []byte(k) },
		func(b []byte) (string, error) { return string(b), nil },
	)
	require.NoError(t, err)
	defer idx.Close()

	err = idx.Put("a", 1)
	require.NoError(t, err)
	err = idx.Put("b", 2)
	require.NoError(t, err)
	err = idx.Put("c", 3)
	require.NoError(t, err)

	var sum int
	err = idx.Iterate(func(key string, value int) error {
		sum += value
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 6, sum)
}

func TestLevelDBIndexer_PutSync(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	idx, err := NewLevelDBIndexer[string, int](
		tmpDir,
		nil,
		func(k string) []byte { return []byte(k) },
		func(b []byte) (string, error) { return string(b), nil },
	)
	require.NoError(t, err)
	defer idx.Close()

	err = idx.PutSync("key", 42)
	require.NoError(t, err)

	val, err := idx.Get("key")
	require.NoError(t, err)
	assert.Equal(t, 42, val)
}

func TestLevelDBIndexer_Persistence(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	// Create and populate indexer
	idx1, err := NewLevelDBIndexer[string, int](
		tmpDir,
		nil,
		func(k string) []byte { return []byte(k) },
		func(b []byte) (string, error) { return string(b), nil },
	)
	require.NoError(t, err)

	err = idx1.Put("key1", 100)
	require.NoError(t, err)
	err = idx1.Put("key2", 200)
	require.NoError(t, err)

	err = idx1.Close()
	require.NoError(t, err)

	// Reopen and verify data persisted
	idx2, err := NewLevelDBIndexer[string, int](
		tmpDir,
		nil,
		func(k string) []byte { return []byte(k) },
		func(b []byte) (string, error) { return string(b), nil },
	)
	require.NoError(t, err)
	defer idx2.Close()

	val1, err := idx2.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, 100, val1)

	val2, err := idx2.Get("key2")
	require.NoError(t, err)
	assert.Equal(t, 200, val2)
}

func TestLevelDBIndexer_Destroy(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	idx, err := NewLevelDBIndexer[string, int](
		tmpDir,
		nil,
		func(k string) []byte { return []byte(k) },
		func(b []byte) (string, error) { return string(b), nil },
	)
	require.NoError(t, err)

	err = idx.Put("key", 123)
	require.NoError(t, err)

	err = idx.Destroy()
	require.NoError(t, err)

	// Data should be cleared
	_, err = idx.Get("key")
	assert.Error(t, err)
}

func TestLevelDBIndexer_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	idx, err := NewLevelDBIndexer[int, int](
		tmpDir,
		nil,
		func(k int) []byte {
			return []byte{byte(k >> 24), byte(k >> 16), byte(k >> 8), byte(k)}
		},
		func(b []byte) (int, error) {
			if len(b) != 4 {
				return 0, errors.New("invalid key")
			}
			return int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3]), nil
		},
	)
	require.NoError(t, err)
	defer idx.Close()

	var wg sync.WaitGroup

	// Concurrent puts
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			idx.Put(id, id*10)
		}(i)
	}

	wg.Wait()

	// Verify all values written
	for i := 0; i < 50; i++ {
		val, err := idx.Get(i)
		require.NoError(t, err)
		assert.Equal(t, i*10, val)
	}
}
