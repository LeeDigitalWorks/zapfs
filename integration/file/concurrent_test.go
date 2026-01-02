//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Concurrency Tests
// =============================================================================

func TestConcurrent_PutGet(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	numObjects := 10
	objectSize := 256 * 1024 // 256KB

	var wg sync.WaitGroup
	errors := make(chan error, numObjects*2)

	for i := 0; i < numObjects; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			objectID := testutil.UniqueID(fmt.Sprintf("test-concurrent-%d", idx))
			data := testutil.GenerateTestData(t, objectSize)

			resp := client.PutObject(objectID, data)
			if resp.Size != uint64(len(data)) {
				errors <- fmt.Errorf("object %d: put failed, size mismatch", idx)
				return
			}

			retrieved := client.GetObject(objectID)
			if !bytes.Equal(data, retrieved) {
				errors <- fmt.Errorf("object %d: data mismatch", idx)
				return
			}

			client.DeleteObject(objectID)
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

func TestConcurrent_SameObject(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-same-obj")
	numWriters := 5
	objectSize := 128 * 1024 // 128KB

	var wg sync.WaitGroup
	results := make(chan bool, numWriters)

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := testutil.GenerateTestData(t, objectSize)
			resp := client.PutObject(objectID, data)
			results <- resp.Size == uint64(len(data))
		}()
	}

	wg.Wait()
	close(results)

	successCount := 0
	for success := range results {
		if success {
			successCount++
		}
	}
	assert.Equal(t, numWriters, successCount, "all concurrent writes should succeed")

	// Object should exist and be readable
	retrieved := client.GetObject(objectID)
	assert.NotEmpty(t, retrieved, "object should be retrievable")

	client.DeleteObject(objectID)
}

func TestConcurrent_ReadWhileWriting(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-read-write")
	objectSize := 1 * 1024 * 1024 // 1MB

	// First write
	initialData := testutil.GenerateTestData(t, objectSize)
	client.PutObject(objectID, initialData)

	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	readResults := make(chan []byte, 5)

	wg.Add(2)

	// Writer goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			newData := testutil.GenerateTestData(t, objectSize)
			client.PutObject(objectID, newData)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Reader goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			data := client.GetObject(objectID)
			readResults <- data
			time.Sleep(30 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(readResults)

	readCount := 0
	completeReads := 0
	for data := range readResults {
		readCount++
		if len(data) == objectSize {
			completeReads++
		}
	}

	assert.Equal(t, 5, readCount, "should have 5 read attempts")
	assert.GreaterOrEqual(t, completeReads, 1, "at least one read should complete")
	t.Logf("Complete reads: %d/%d", completeReads, readCount)

	client.DeleteObject(objectID)
}

func TestConnection_MultipleClientsSharedServer(t *testing.T) {
	t.Parallel()

	numClients := 10
	objectsPerClient := 5

	var wg sync.WaitGroup
	errors := make(chan error, numClients*objectsPerClient)

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			client := newFileClient(t, fileServer1Addr)

			for i := 0; i < objectsPerClient; i++ {
				objectID := testutil.UniqueID(fmt.Sprintf("test-multiclient-%d-%d", clientID, i))
				data := testutil.GenerateTestData(t, 32*1024)

				resp := client.PutObject(objectID, data)
				if resp.Size != uint64(len(data)) {
					errors <- fmt.Errorf("client %d object %d: put failed, size mismatch", clientID, i)
					continue
				}

				retrieved := client.GetObject(objectID)
				if !bytes.Equal(data, retrieved) {
					errors <- fmt.Errorf("client %d object %d: data mismatch", clientID, i)
				}

				client.DeleteObject(objectID)
			}
		}(c)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// =============================================================================
// Stress Tests
// =============================================================================

func TestStress_HighConcurrency(t *testing.T) {
	testutil.SkipIfShort(t)
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	numWorkers := 50
	objectsPerWorker := 10
	objectSize := 64 * 1024 // 64KB

	var wg sync.WaitGroup
	errorCount := int32(0)

	start := time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < objectsPerWorker; i++ {
				objectID := testutil.UniqueID(fmt.Sprintf("stress-%d-%d", workerID, i))
				data := testutil.GenerateTestData(t, objectSize)

				resp := client.PutObject(objectID, data)
				if resp.Size != uint64(len(data)) {
					errorCount++
					continue
				}

				retrieved := client.GetObject(objectID)
				if !bytes.Equal(data, retrieved) {
					errorCount++
				}

				client.DeleteObject(objectID)
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := numWorkers * objectsPerWorker * 3
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	t.Logf("Stress test: %d ops in %v (%.2f ops/sec)", totalOps, elapsed, opsPerSec)
	assert.Equal(t, int32(0), errorCount, "should have no errors")
}
