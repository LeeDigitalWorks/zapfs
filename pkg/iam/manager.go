package iam

import (
	"context"
	"time"

	"zapfs/pkg/cache"
)

// Manager provides fast credential lookups with in-memory caching.
// Uses the generic pkg/cache for high-performance concurrent access.
type Manager struct {
	store          CredentialStore
	accessKeyCache *cache.Cache[string, *cacheEntry]
}

// cacheEntry holds cached credential lookup results
type cacheEntry struct {
	identity   *Identity
	credential *Credential
}

const (
	defaultCacheMaxItems = 10000
	defaultCacheTTL      = 5 * time.Minute
)

// NewManager creates a new IAM manager with the given credential store
func NewManager(store CredentialStore) *Manager {
	return NewManagerWithCache(store, defaultCacheMaxItems, defaultCacheTTL)
}

// NewManagerWithCache creates a new IAM manager with custom cache settings
func NewManagerWithCache(store CredentialStore, maxItems int, ttl time.Duration) *Manager {
	return &Manager{
		store: store,
		accessKeyCache: cache.New[string, *cacheEntry](context.Background(),
			cache.WithMaxSize[string, *cacheEntry](maxItems),
			cache.WithExpiry[string, *cacheEntry](ttl),
		),
	}
}

// LookupByAccessKey retrieves identity and credential by access key (with caching)
func (m *Manager) LookupByAccessKey(ctx context.Context, accessKey string) (*Identity, *Credential, bool) {
	// Fast path: check cache
	if entry, exists := m.accessKeyCache.Get(accessKey); exists && entry != nil {
		// Verify credential is still active and user not disabled
		if !entry.credential.IsActive() || entry.identity.Disabled {
			return nil, nil, false
		}
		return entry.identity, entry.credential, true
	}

	// Slow path: query store and cache result
	identity, cred, err := m.store.GetUserByAccessKey(ctx, accessKey)
	if err != nil {
		return nil, nil, false
	}

	// Check status before caching
	if !cred.IsActive() || identity.Disabled {
		return nil, nil, false
	}

	// Cache the result
	m.accessKeyCache.Set(accessKey, &cacheEntry{
		identity:   identity,
		credential: cred,
	})

	return identity, cred, true
}

// InvalidateCache clears the entire access key lookup cache
func (m *Manager) InvalidateCache() {
	m.accessKeyCache.Clear()
}

// InvalidateAccessKey removes a specific access key from cache
func (m *Manager) InvalidateAccessKey(accessKey string) {
	m.accessKeyCache.Delete(accessKey)
}

// GetStore returns the underlying credential store
func (m *Manager) GetStore() CredentialStore {
	return m.store
}

// CacheSize returns the current number of cached entries
func (m *Manager) CacheSize() int {
	return m.accessKeyCache.Size()
}

// CacheDump represents a single cached credential entry for debugging
type CacheDump struct {
	AccessKey    string `json:"access_key"`
	IdentityName string `json:"identity_name"`
	AccountID    string `json:"account_id,omitempty"`
	Disabled     bool   `json:"disabled"`
	Status       string `json:"status"`
}

// DumpCache returns all cached entries for debugging.
// Secret keys are NOT included for security.
func (m *Manager) DumpCache() []CacheDump {
	var entries []CacheDump
	for accessKey, entry := range m.accessKeyCache.Iter() {
		dump := CacheDump{
			AccessKey:    accessKey,
			IdentityName: entry.identity.Name,
			Disabled:     entry.identity.Disabled,
			Status:       entry.credential.Status,
		}
		if entry.identity.Account != nil {
			dump.AccountID = entry.identity.Account.ID
		}
		entries = append(entries, dump)
	}
	return entries
}

// Stop stops the cache cleanup goroutine. Call when manager is no longer needed.
func (m *Manager) Stop() {
	m.accessKeyCache.Stop()
}
