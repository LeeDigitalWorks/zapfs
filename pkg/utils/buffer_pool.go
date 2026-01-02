// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"math/bits"
	"sync"
)

// Buffer pool size classes (powers of 2)
// Index 0 = 1KB, Index 1 = 2KB, ..., Index 12 = 4MB
const (
	minPoolSize   = 1 << 10 // 1KB minimum
	maxPoolSize   = 1 << 22 // 4MB maximum (matches chunk size)
	numPoolLevels = 13      // 1KB to 4MB (13 levels)
)

var bufferPools [numPoolLevels]sync.Pool

func init() {
	for i := range bufferPools {
		size := minPoolSize << i
		bufferPools[i] = sync.Pool{
			New: func() any {
				buf := make([]byte, size)
				return &buf
			},
		}
	}
}

// poolIndex returns the pool index for a given size.
// Returns -1 if size is larger than maxPoolSize.
func poolIndex(size int) int {
	if size <= minPoolSize {
		return 0
	}
	if size > maxPoolSize {
		return -1
	}
	// Find the smallest power of 2 >= size, then convert to index
	// bits.Len returns position of highest bit, we need ceiling
	idx := bits.Len(uint(size-1)) - 10 // -10 because minPoolSize is 1<<10
	if idx < 0 {
		return 0
	}
	if idx >= numPoolLevels {
		return -1
	}
	return idx
}

// GetBuffer returns a byte slice of at least the requested size.
// The returned slice may be larger than requested (rounded up to power of 2).
// Use PutBuffer to return it to the pool when done.
//
// For sizes > 1MB, allocates directly without pooling.
func GetBuffer(size int) []byte {
	idx := poolIndex(size)
	if idx < 0 {
		// Too large for pool, allocate directly
		return make([]byte, size)
	}
	bufPtr := bufferPools[idx].Get().(*[]byte)
	return (*bufPtr)[:size] // Return slice of exact requested size
}

// GetBufferCap returns a byte slice with at least the requested capacity.
// The slice length is 0, but capacity is the pool size.
// Useful when you need to append to a buffer.
func GetBufferCap(capacity int) []byte {
	idx := poolIndex(capacity)
	if idx < 0 {
		return make([]byte, 0, capacity)
	}
	bufPtr := bufferPools[idx].Get().(*[]byte)
	return (*bufPtr)[:0] // Return with length 0, full capacity
}

// PutBuffer returns a buffer to the pool.
// Only buffers obtained from GetBuffer/GetBufferCap should be returned.
// Buffers larger than maxPoolSize are silently discarded.
//
// WARNING: Do not use the buffer after calling PutBuffer.
func PutBuffer(buf []byte) {
	c := cap(buf)
	idx := poolIndex(c)
	if idx < 0 {
		return // Too large, let GC handle it
	}
	// Verify it's a valid pool size (power of 2)
	poolSize := minPoolSize << idx
	if c != poolSize {
		return // Not from our pool, don't corrupt it
	}
	// Re-slice to full capacity and put pointer back
	buf = buf[:c]
	bufferPools[idx].Put(&buf)
}

// BufferPoolStats returns statistics about buffer pool usage.
// Useful for debugging and monitoring.
type BufferPoolStats struct {
	Levels []BufferPoolLevel
}

type BufferPoolLevel struct {
	Size int // Buffer size at this level
}

func GetBufferPoolStats() BufferPoolStats {
	stats := BufferPoolStats{
		Levels: make([]BufferPoolLevel, numPoolLevels),
	}
	for i := range stats.Levels {
		stats.Levels[i].Size = minPoolSize << i
	}
	return stats
}
