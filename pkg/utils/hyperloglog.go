// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"math"
	"math/bits"
	"sync"
)

// HyperLogLog is a probabilistic data structure for cardinality estimation.
// It can estimate the number of unique elements in a set using minimal memory.
//
// Thread-safe for concurrent use.
//
// Memory usage: 2^precision bytes (e.g., precision=14 uses 16KB)
// Error rate: approximately 1.04 / sqrt(2^precision)
//
// Usage:
//
//	hll := NewHyperLogLog(14) // ~0.8% error, 16KB memory
//	hll.Add("user1")
//	hll.Add("user2")
//	hll.Add("user1") // duplicate
//	count := hll.Count() // ~2
type HyperLogLog struct {
	mu        sync.RWMutex
	registers []uint8 // One register per bucket
	precision uint8   // Number of bits for bucket index (4-18)
	m         uint32  // Number of buckets (2^precision)
	alpha     float64 // Correction factor
}

// NewHyperLogLog creates a new HyperLogLog with the given precision.
// Precision must be between 4 and 18 (inclusive).
//
// Common precision values:
//   - 10: ~1024 bytes, ~3.25% error
//   - 12: ~4KB, ~1.625% error
//   - 14: ~16KB, ~0.8% error (recommended)
//   - 16: ~64KB, ~0.4% error
func NewHyperLogLog(precision uint8) *HyperLogLog {
	if precision < 4 {
		precision = 4
	}
	if precision > 18 {
		precision = 18
	}

	m := uint32(1) << precision
	alpha := getAlpha(m)

	return &HyperLogLog{
		registers: make([]uint8, m),
		precision: precision,
		m:         m,
		alpha:     alpha,
	}
}

// Add adds an element to the HyperLogLog.
func (hll *HyperLogLog) Add(item string) {
	hll.AddBytes([]byte(item))
}

// AddBytes adds a byte slice to the HyperLogLog.
func (hll *HyperLogLog) AddBytes(item []byte) {
	hash := hll.hash(item)
	hll.addHash(hash)
}

// AddHash adds a pre-computed 64-bit hash to the HyperLogLog.
// Useful when you already have a hash value.
func (hll *HyperLogLog) AddHash(hash uint64) {
	hll.addHash(hash)
}

func (hll *HyperLogLog) addHash(hash uint64) {
	// Use the lower 'precision' bits as bucket index
	idx := hash & uint64(hll.m-1)

	// Use upper bits to compute rho (position of leftmost 1, 1-indexed)
	// Shift right by precision bits to get the remaining bits
	w := hash >> hll.precision

	// rho = number of leading zeros + 1
	// For a 64-bit hash with precision bits used for index,
	// we have 64-precision bits left
	var rho uint8
	if w == 0 {
		rho = uint8(64-hll.precision) + 1
	} else {
		// bits.LeadingZeros64 counts leading zeros in the full 64 bits
		// We need to adjust for the bits we've shifted out
		lz := bits.LeadingZeros64(w)
		rho = uint8(lz - int(hll.precision) + 1)
		if rho < 1 {
			rho = 1
		}
	}

	hll.mu.Lock()
	if rho > hll.registers[idx] {
		hll.registers[idx] = rho
	}
	hll.mu.Unlock()
}

// Count returns the estimated cardinality.
func (hll *HyperLogLog) Count() uint64 {
	hll.mu.RLock()
	defer hll.mu.RUnlock()

	// Calculate harmonic mean of 2^(-register[i])
	sum := 0.0
	zeros := uint32(0)

	for _, val := range hll.registers {
		sum += math.Pow(2, -float64(val))
		if val == 0 {
			zeros++
		}
	}

	// Raw estimate
	estimate := hll.alpha * float64(hll.m) * float64(hll.m) / sum

	// Apply corrections
	if estimate <= 2.5*float64(hll.m) {
		// Small range correction using linear counting
		if zeros > 0 {
			estimate = float64(hll.m) * math.Log(float64(hll.m)/float64(zeros))
		}
	} else if estimate > (1.0/30.0)*math.Pow(2, 32) {
		// Large range correction (for 32-bit hash space)
		estimate = -math.Pow(2, 32) * math.Log(1-estimate/math.Pow(2, 32))
	}

	return uint64(math.Round(estimate))
}

// Merge combines another HyperLogLog into this one.
// Both must have the same precision.
func (hll *HyperLogLog) Merge(other *HyperLogLog) {
	if hll.precision != other.precision {
		return
	}

	hll.mu.Lock()
	other.mu.RLock()

	for i := range hll.registers {
		if other.registers[i] > hll.registers[i] {
			hll.registers[i] = other.registers[i]
		}
	}

	other.mu.RUnlock()
	hll.mu.Unlock()
}

// Clear resets the HyperLogLog to empty state.
func (hll *HyperLogLog) Clear() {
	hll.mu.Lock()
	defer hll.mu.Unlock()

	for i := range hll.registers {
		hll.registers[i] = 0
	}
}

// SizeBytes returns the memory usage in bytes.
func (hll *HyperLogLog) SizeBytes() int {
	return int(hll.m)
}

// ErrorRate returns the theoretical standard error rate.
func (hll *HyperLogLog) ErrorRate() float64 {
	return 1.04 / math.Sqrt(float64(hll.m))
}

// FNV-1a constants for inline hashing (avoids allocation)
const (
	hllFnvOffset64 = 14695981039346656037
	hllFnvPrime64  = 1099511628211
)

// hash computes a 64-bit hash of the data using inline FNV-1a.
// Inline implementation avoids allocations from fnv.New64a().
func (hll *HyperLogLog) hash(data []byte) uint64 {
	h := uint64(hllFnvOffset64)
	for _, b := range data {
		h ^= uint64(b)
		h *= hllFnvPrime64
	}
	return h
}

// getAlpha returns the alpha correction factor for m buckets.
func getAlpha(m uint32) float64 {
	switch m {
	case 16:
		return 0.673
	case 32:
		return 0.697
	case 64:
		return 0.709
	default:
		return 0.7213 / (1 + 1.079/float64(m))
	}
}

// HyperLogLogSet manages multiple HyperLogLogs for different keys.
// Useful for tracking cardinality per bucket, user, etc.
type HyperLogLogSet struct {
	mu        sync.RWMutex
	hlls      map[string]*HyperLogLog
	precision uint8
}

// NewHyperLogLogSet creates a new set of HyperLogLogs.
func NewHyperLogLogSet(precision uint8) *HyperLogLogSet {
	return &HyperLogLogSet{
		hlls:      make(map[string]*HyperLogLog),
		precision: precision,
	}
}

// Add adds an item to the HyperLogLog for the given key.
func (hlls *HyperLogLogSet) Add(key, item string) {
	hlls.mu.Lock()
	hll, exists := hlls.hlls[key]
	if !exists {
		hll = NewHyperLogLog(hlls.precision)
		hlls.hlls[key] = hll
	}
	hlls.mu.Unlock()

	hll.Add(item)
}

// Count returns the estimated cardinality for a key.
func (hlls *HyperLogLogSet) Count(key string) uint64 {
	hlls.mu.RLock()
	hll, exists := hlls.hlls[key]
	hlls.mu.RUnlock()

	if !exists {
		return 0
	}
	return hll.Count()
}

// Keys returns all tracked keys.
func (hlls *HyperLogLogSet) Keys() []string {
	hlls.mu.RLock()
	defer hlls.mu.RUnlock()

	keys := make([]string, 0, len(hlls.hlls))
	for k := range hlls.hlls {
		keys = append(keys, k)
	}
	return keys
}

// Delete removes a key's HyperLogLog.
func (hlls *HyperLogLogSet) Delete(key string) {
	hlls.mu.Lock()
	delete(hlls.hlls, key)
	hlls.mu.Unlock()
}

// Len returns the number of tracked keys.
func (hlls *HyperLogLogSet) Len() int {
	hlls.mu.RLock()
	defer hlls.mu.RUnlock()
	return len(hlls.hlls)
}

// SizeBytes returns the total memory usage across all HyperLogLogs.
func (hlls *HyperLogLogSet) SizeBytes() int {
	hlls.mu.RLock()
	defer hlls.mu.RUnlock()

	total := 0
	for _, hll := range hlls.hlls {
		total += hll.SizeBytes()
	}
	return total
}

// MergeInto merges all HyperLogLogs from one key into another.
func (hlls *HyperLogLogSet) MergeInto(destKey, srcKey string) {
	hlls.mu.Lock()
	defer hlls.mu.Unlock()

	src, srcExists := hlls.hlls[srcKey]
	if !srcExists {
		return
	}

	dest, destExists := hlls.hlls[destKey]
	if !destExists {
		dest = NewHyperLogLog(hlls.precision)
		hlls.hlls[destKey] = dest
	}

	dest.Merge(src)
}
