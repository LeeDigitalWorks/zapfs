// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"hash/maphash"
	"sync"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// BucketStore wraps a Cache to provide PolicyStore and ACLStore interfaces
// for the authorization filter. This allows reusing the bucket cache for policy
// and ACL lookups without a separate cache.
//
// It also maintains an owner index for efficient ListBuckets operations.
// The owner index uses lock striping to reduce contention when multiple owners
// are accessed concurrently.
//
// BucketStore is cache-only and does not perform lazy loading. The cache must be
// populated by background refresh goroutines (similar to GlobalBucketCache).
// If a bucket is not in the cache, GetBucket returns nil (not found).
//
// Usage:
//
//	bucketsCache := cache.New[string, s3types.Bucket](ctx, cache.WithMaxSize[string, s3types.Bucket](10000))
//	bucketStore := cache.NewBucketStore(bucketsCache)
//
//	// Use in authorization filter
//	authzFilter := filter.NewAuthorizationFilter(filter.AuthorizationConfig{
//	    PolicyStore: bucketStore,
//	    ACLStore:    bucketStore,
//	})

const ownerIndexShardCount = 64

// ownerIndexShard is a single shard of the owner index with its own lock
type ownerIndexShard struct {
	mu   sync.RWMutex
	data map[string]map[string]struct{} // owner ID -> set of bucket names
}

type BucketStore struct {
	cache *Cache[string, s3types.Bucket]

	// Owner index shards: owner ID -> set of bucket names
	// Uses lock striping to allow concurrent access to different owners
	ownerIndexShards []*ownerIndexShard
	ownerIndexSeed   maphash.Seed
}

// NewBucketStore creates a new BucketStore wrapping the given cache.
// The cache should be populated by background refresh goroutines.
func NewBucketStore(cache *Cache[string, s3types.Bucket]) *BucketStore {
	shards := make([]*ownerIndexShard, ownerIndexShardCount)
	for i := range shards {
		shards[i] = &ownerIndexShard{
			data: make(map[string]map[string]struct{}),
		}
	}
	return &BucketStore{
		cache:            cache,
		ownerIndexShards: shards,
		ownerIndexSeed:   maphash.MakeSeed(),
	}
}

// getOwnerIndexShard returns the shard for a given owner ID
func (bs *BucketStore) getOwnerIndexShard(ownerID string) *ownerIndexShard {
	if ownerID == "" {
		// Empty owner ID always goes to shard 0
		return bs.ownerIndexShards[0]
	}
	var h maphash.Hash
	h.SetSeed(bs.ownerIndexSeed)
	h.WriteString(ownerID)
	return bs.ownerIndexShards[h.Sum64()%ownerIndexShardCount]
}

// GetBucket returns the bucket from cache, or nil if not found.
// This is cache-only - no lazy loading is performed.
func (bs *BucketStore) GetBucket(bucket string) (s3types.Bucket, bool) {
	return bs.cache.Get(bucket)
}

// --- PolicyStore interface implementation ---

// GetBucketPolicy returns the bucket policy, or nil if none exists
func (bs *BucketStore) GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, bool) {
	b, exists := bs.GetBucket(bucket)
	if !exists {
		return nil, false
	}
	return b.Policy, true
}

// --- ACLStore interface implementation ---

// GetBucketACL returns the ACL for a bucket
func (bs *BucketStore) GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, bool) {
	b, exists := bs.GetBucket(bucket)
	if !exists {
		return nil, false
	}
	return b.ACL, true
}

// GetObjectACL returns the ACL for an object.
// Object ACLs are stored per-object in the metadata DB, not in the bucket cache.
// This is a stub that returns nil - object ACLs should be fetched from the
// object metadata during request handling.
func (bs *BucketStore) GetObjectACL(ctx context.Context, bucket, key string) (*s3types.AccessControlList, bool) {
	// Object ACLs are not stored in the bucket cache.
	// The handler should fetch object metadata which includes the ACL.
	// For authorization, if object ACL is needed, it should be passed in the request context.
	return nil, false
}

// --- Cache management methods ---

// SetBucket updates or adds a bucket in the cache and updates the owner index
func (bs *BucketStore) SetBucket(bucket string, b s3types.Bucket) {
	// Check if bucket already exists with a different owner
	if existing, exists := bs.cache.Get(bucket); exists && existing.OwnerID != b.OwnerID {
		// Remove from old owner's index
		bs.removeFromOwnerIndex(existing.OwnerID, bucket)
	}

	bs.cache.Set(bucket, b)

	// Add to new owner's index
	bs.addToOwnerIndex(b.OwnerID, bucket)
}

// DeleteBucket removes a bucket from the cache and owner index
func (bs *BucketStore) DeleteBucket(bucket string) {
	// Get bucket to find owner before deleting
	if b, exists := bs.cache.Get(bucket); exists {
		bs.removeFromOwnerIndex(b.OwnerID, bucket)
	}
	bs.cache.Delete(bucket)
}

// ListBucketsByOwner returns all buckets for a given owner from cache
func (bs *BucketStore) ListBucketsByOwner(ownerID string) []s3types.Bucket {
	shard := bs.getOwnerIndexShard(ownerID)
	shard.mu.RLock()
	bucketNames, exists := shard.data[ownerID]
	shard.mu.RUnlock()

	if !exists {
		return nil
	}

	var buckets []s3types.Bucket
	for name := range bucketNames {
		if b, exists := bs.cache.Get(name); exists {
			buckets = append(buckets, b)
		}
	}
	return buckets
}

// addToOwnerIndex adds a bucket to the owner's index
func (bs *BucketStore) addToOwnerIndex(owner, bucket string) {
	if owner == "" {
		return
	}
	shard := bs.getOwnerIndexShard(owner)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.data[owner] == nil {
		shard.data[owner] = make(map[string]struct{})
	}
	shard.data[owner][bucket] = struct{}{}
}

// removeFromOwnerIndex removes a bucket from the owner's index
func (bs *BucketStore) removeFromOwnerIndex(owner, bucket string) {
	if owner == "" {
		return
	}
	shard := bs.getOwnerIndexShard(owner)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if buckets, exists := shard.data[owner]; exists {
		delete(buckets, bucket)
		if len(buckets) == 0 {
			delete(shard.data, owner)
		}
	}
}

// UpdateBucketPolicy updates just the policy field for a cached bucket
func (bs *BucketStore) UpdateBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error {
	b, exists := bs.GetBucket(bucket)
	if !exists {
		return nil // Bucket doesn't exist
	}
	b.Policy = policy
	bs.cache.Set(bucket, b)
	return nil
}

// UpdateBucketACL updates just the ACL field for a cached bucket
func (bs *BucketStore) UpdateBucketACL(ctx context.Context, bucket string, acl *s3types.AccessControlList) error {
	b, exists := bs.GetBucket(bucket)
	if !exists {
		return nil // Bucket doesn't exist
	}
	b.ACL = acl
	bs.cache.Set(bucket, b)
	return nil
}

// IsReady returns true if the cache has completed initial load
func (bs *BucketStore) IsReady() bool {
	return bs.cache.HasLoaded()
}

// Cache returns the underlying Cache for direct access if needed
func (bs *BucketStore) Cache() *Cache[string, s3types.Bucket] {
	return bs.cache
}

// BucketCacheEntry represents a bucket entry for cache dump
type BucketCacheEntry struct {
	Name       string `json:"name"`
	OwnerID    string `json:"owner_id"`
	Location   string `json:"location"`
	CreateTime string `json:"create_time"`
	HasPolicy  bool   `json:"has_policy"`
	HasACL     bool   `json:"has_acl"`
}

// DumpCache returns all cached buckets for debugging
func (bs *BucketStore) DumpCache() []BucketCacheEntry {
	var entries []BucketCacheEntry
	for name, bucket := range bs.cache.Iter() {
		entries = append(entries, BucketCacheEntry{
			Name:       name,
			OwnerID:    bucket.OwnerID,
			Location:   bucket.Location,
			CreateTime: bucket.CreateTime.Format("2006-01-02T15:04:05Z"),
			HasPolicy:  bucket.Policy != nil,
			HasACL:     bucket.ACL != nil,
		})
	}
	return entries
}
