package utils

import (
	"hash"
	"hash/fnv"
	"math"
	"math/bits"
	"sync"
)

// FNV-1a constants for inline hashing (avoids allocation)
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// BloomFilter is a space-efficient probabilistic data structure for set membership tests.
// It can have false positives but never false negatives.
//
// Thread-safe for concurrent use.
//
// Usage:
//
//	// Create a filter for ~10000 items with 1% false positive rate
//	bf := NewBloomFilter(10000, 0.01)
//	bf.Add("key1")
//	bf.Add("key2")
//
//	bf.Contains("key1") // true
//	bf.Contains("key3") // false (probably)
type BloomFilter struct {
	mu      sync.RWMutex
	bits    []uint64 // bit array stored as uint64s
	numBits uint64   // total number of bits
	numHash uint64   // number of hash functions
	count   uint64   // approximate number of items added
}

// NewBloomFilter creates a new Bloom filter optimized for the expected number of items
// and desired false positive rate.
//
// Parameters:
//   - expectedItems: expected number of items to be added
//   - falsePositiveRate: desired false positive rate (e.g., 0.01 for 1%)
//
// Memory usage is approximately: -1.44 * expectedItems * ln(falsePositiveRate) / 8 bytes
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	if expectedItems < 1 {
		expectedItems = 1
	}
	if falsePositiveRate <= 0 {
		falsePositiveRate = 0.01
	}
	if falsePositiveRate >= 1 {
		falsePositiveRate = 0.99
	}

	// Calculate optimal number of bits: m = -n * ln(p) / (ln(2)^2)
	n := float64(expectedItems)
	p := falsePositiveRate
	m := -n * math.Log(p) / (math.Ln2 * math.Ln2)

	// Calculate optimal number of hash functions: k = (m/n) * ln(2)
	k := (m / n) * math.Ln2

	numBits := uint64(math.Ceil(m))
	numHash := uint64(math.Ceil(k))

	// Ensure minimum values
	if numBits < 64 {
		numBits = 64
	}
	if numHash < 1 {
		numHash = 1
	}

	// Round up to nearest 64 bits for efficient storage
	numWords := (numBits + 63) / 64
	numBits = numWords * 64

	return &BloomFilter{
		bits:    make([]uint64, numWords),
		numBits: numBits,
		numHash: numHash,
	}
}

// NewBloomFilterFromSize creates a Bloom filter with explicit bit size and hash count.
// Use this when you want precise control over memory usage.
func NewBloomFilterFromSize(numBits, numHash uint64) *BloomFilter {
	if numBits < 64 {
		numBits = 64
	}
	if numHash < 1 {
		numHash = 1
	}

	numWords := (numBits + 63) / 64
	numBits = numWords * 64

	return &BloomFilter{
		bits:    make([]uint64, numWords),
		numBits: numBits,
		numHash: numHash,
	}
}

// Add adds an item to the filter.
func (bf *BloomFilter) Add(item string) {
	bf.AddBytes([]byte(item))
}

// AddBytes adds a byte slice to the filter.
func (bf *BloomFilter) AddBytes(item []byte) {
	h1, h2 := bf.hash(item)

	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := uint64(0); i < bf.numHash; i++ {
		// Double hashing: h(i) = h1 + i*h2
		pos := (h1 + i*h2) % bf.numBits
		bf.setBit(pos)
	}
	bf.count++
}

// Contains tests whether an item might be in the filter.
// Returns true if the item is possibly in the set (may be false positive).
// Returns false if the item is definitely not in the set.
func (bf *BloomFilter) Contains(item string) bool {
	return bf.ContainsBytes([]byte(item))
}

// ContainsBytes tests whether a byte slice might be in the filter.
func (bf *BloomFilter) ContainsBytes(item []byte) bool {
	h1, h2 := bf.hash(item)

	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for i := uint64(0); i < bf.numHash; i++ {
		pos := (h1 + i*h2) % bf.numBits
		if !bf.getBit(pos) {
			return false
		}
	}
	return true
}

// Count returns the approximate number of items added to the filter.
func (bf *BloomFilter) Count() uint64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.count
}

// Clear resets the filter to empty state.
func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.count = 0
}

// FillRatio returns the ratio of set bits to total bits.
// A high fill ratio (>0.5) indicates the filter may have too many false positives.
func (bf *BloomFilter) FillRatio() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	setBits := uint64(0)
	for _, word := range bf.bits {
		setBits += uint64(popcount(word))
	}
	return float64(setBits) / float64(bf.numBits)
}

// EstimatedFalsePositiveRate returns the current estimated false positive rate
// based on the number of items added.
func (bf *BloomFilter) EstimatedFalsePositiveRate() float64 {
	bf.mu.RLock()
	n := float64(bf.count)
	m := float64(bf.numBits)
	k := float64(bf.numHash)
	bf.mu.RUnlock()

	if n == 0 {
		return 0
	}

	// FPR = (1 - e^(-kn/m))^k
	return math.Pow(1-math.Exp(-k*n/m), k)
}

// SizeBytes returns the memory usage of the filter in bytes.
func (bf *BloomFilter) SizeBytes() int {
	return len(bf.bits) * 8
}

// hash computes two independent hash values using inline FNV-1a.
// Uses the technique from "Less Hashing, Same Performance" paper.
// Inline implementation avoids allocations from fnv.New64a().
func (bf *BloomFilter) hash(data []byte) (uint64, uint64) {
	// Inline FNV-1a for first hash (zero allocations)
	h1 := uint64(fnvOffset64)
	for _, b := range data {
		h1 ^= uint64(b)
		h1 *= fnvPrime64
	}

	// Second hash: continue from h1 with an extra byte
	h2 := h1
	h2 ^= 0x01
	h2 *= fnvPrime64

	return h1, h2
}

// setBit sets the bit at position pos.
// Caller must hold write lock.
func (bf *BloomFilter) setBit(pos uint64) {
	word := pos / 64
	bit := pos % 64
	bf.bits[word] |= 1 << bit
}

// getBit returns true if the bit at position pos is set.
// Caller must hold read lock.
func (bf *BloomFilter) getBit(pos uint64) bool {
	word := pos / 64
	bit := pos % 64
	return (bf.bits[word] & (1 << bit)) != 0
}

// popcount returns the number of set bits in a uint64.
// Uses hardware-accelerated POPCNT instruction when available.
func popcount(x uint64) int {
	return bits.OnesCount64(x)
}

// BloomFilterSet wraps a BloomFilter with a backing store for rebuilding.
// Useful when you need to periodically rebuild the filter from source data.
type BloomFilterSet struct {
	mu       sync.RWMutex
	filter   *BloomFilter
	items    map[string]struct{}
	capacity int
	fpRate   float64
}

// NewBloomFilterSet creates a new BloomFilterSet with backing store.
func NewBloomFilterSet(capacity int, falsePositiveRate float64) *BloomFilterSet {
	return &BloomFilterSet{
		filter:   NewBloomFilter(capacity, falsePositiveRate),
		items:    make(map[string]struct{}),
		capacity: capacity,
		fpRate:   falsePositiveRate,
	}
}

// Add adds an item to both the filter and backing store.
func (bfs *BloomFilterSet) Add(item string) {
	bfs.mu.Lock()
	defer bfs.mu.Unlock()

	bfs.items[item] = struct{}{}
	bfs.filter.Add(item)
}

// Remove removes an item from the backing store.
// Note: Item will still be in the Bloom filter until Rebuild() is called.
func (bfs *BloomFilterSet) Remove(item string) {
	bfs.mu.Lock()
	defer bfs.mu.Unlock()

	delete(bfs.items, item)
}

// Contains checks if an item might be in the set using the Bloom filter.
func (bfs *BloomFilterSet) Contains(item string) bool {
	return bfs.filter.Contains(item)
}

// ContainsExact checks if an item is definitely in the set using the backing store.
func (bfs *BloomFilterSet) ContainsExact(item string) bool {
	bfs.mu.RLock()
	defer bfs.mu.RUnlock()

	_, exists := bfs.items[item]
	return exists
}

// Rebuild reconstructs the Bloom filter from the backing store.
// Call this periodically if items are removed frequently.
func (bfs *BloomFilterSet) Rebuild() {
	bfs.mu.Lock()
	defer bfs.mu.Unlock()

	// Create new filter sized for current items
	size := len(bfs.items)
	if size < bfs.capacity {
		size = bfs.capacity
	}
	bfs.filter = NewBloomFilter(size, bfs.fpRate)

	for item := range bfs.items {
		bfs.filter.Add(item)
	}
}

// Len returns the number of items in the backing store.
func (bfs *BloomFilterSet) Len() int {
	bfs.mu.RLock()
	defer bfs.mu.RUnlock()
	return len(bfs.items)
}

// HasherPool provides pooled hashers for high-throughput scenarios.
type HasherPool struct {
	pool sync.Pool
}

// NewHasherPool creates a new pool of FNV hashers.
func NewHasherPool() *HasherPool {
	return &HasherPool{
		pool: sync.Pool{
			New: func() any {
				return fnv.New64a()
			},
		},
	}
}

// Get returns a hasher from the pool.
func (hp *HasherPool) Get() hash.Hash64 {
	return hp.pool.Get().(hash.Hash64)
}

// Put returns a hasher to the pool after resetting it.
func (hp *HasherPool) Put(h hash.Hash64) {
	h.Reset()
	hp.pool.Put(h)
}
