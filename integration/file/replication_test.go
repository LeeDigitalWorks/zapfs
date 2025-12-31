//go:build integration

package file

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Erasure Coding Tests
// =============================================================================

func TestPutGetObject_ErasureCoding(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-ec")
	data := testutil.GenerateTestData(t, 2*1024*1024) // 2MB

	resp := client.PutObject(objectID, data,
		testutil.WithErasureCoding(4, 2),
	)

	assert.Equal(t, uint64(len(data)), resp.Size)
	assert.Equal(t, testutil.ComputeETag(data), resp.Etag)

	retrieved := client.GetObject(objectID)
	assert.Equal(t, data, retrieved, "EC retrieved data should match original")

	client.DeleteObject(objectID)
}

func TestPutGetObject_ErasureCoding_Large(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-ec-large")
	data := testutil.GenerateTestData(t, 20*1024*1024) // 20MB

	resp := client.PutObject(objectID, data,
		testutil.WithErasureCoding(8, 4),
	)

	assert.Equal(t, uint64(len(data)), resp.Size)

	retrieved := client.GetObject(objectID)
	assert.Equal(t, len(data), len(retrieved))
	assert.Equal(t, testutil.ComputeETag(data), testutil.ComputeETag(retrieved))

	client.DeleteObject(objectID)
}

// =============================================================================
// Both Servers Tests
// =============================================================================

func TestBothServers_IndependentOperations(t *testing.T) {
	t.Parallel()

	client1 := newFileClient(t, fileServer1Addr)
	client2 := newFileClient(t, fileServer2Addr)

	data1 := testutil.GenerateTestData(t, 64*1024)
	data2 := testutil.GenerateTestData(t, 64*1024)

	objectID1 := testutil.UniqueID("test-server1")
	objectID2 := testutil.UniqueID("test-server2")

	// Put to each server
	resp1 := client1.PutObject(objectID1, data1)
	assert.Equal(t, uint64(len(data1)), resp1.Size)

	resp2 := client2.PutObject(objectID2, data2)
	assert.Equal(t, uint64(len(data2)), resp2.Size)

	// Get from each server
	retrieved1 := client1.GetObject(objectID1)
	assert.Equal(t, data1, retrieved1)

	retrieved2 := client2.GetObject(objectID2)
	assert.Equal(t, data2, retrieved2)

	// Cleanup
	client1.DeleteObject(objectID1)
	client2.DeleteObject(objectID2)
}
