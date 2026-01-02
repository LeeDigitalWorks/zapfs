// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/cache"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/iam_pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// RemoteCredentialStore implements CredentialStore, PolicyStore, and GroupStore
// by querying the manager cluster. This is the recommended store for metadata
// services in production.
//
// Features:
//   - Queries manager for credential lookups
//   - Streams credential changes for real-time sync
//   - Caches credentials AND policies for authorization
//   - Local cache with TTL for performance
//   - Automatic reconnection on stream failure
type RemoteCredentialStore struct {
	mu           sync.RWMutex
	managerAddrs []string
	conn         *grpc.ClientConn
	client       iam_pb.IAMServiceClient

	// Local cache for fast lookups
	cache *cache.Cache[string, *cachedCredential]

	// Policy and group caches (synced alongside credentials)
	policyMu     sync.RWMutex
	policies     map[string]*Policy  // policy name -> policy
	userPolicies map[string][]string // username -> policy names
	userGroups   map[string][]string // username -> group names

	// Current IAM version (for sync)
	version atomic.Uint64

	// Stream management
	streamCtx    context.Context
	streamCancel context.CancelFunc
	streaming    atomic.Bool

	// Configuration
	config RemoteStoreConfig
}

type cachedCredential struct {
	identity   *Identity
	credential *Credential
}

// RemoteStoreConfig configures the remote credential store
type RemoteStoreConfig struct {
	// Manager addresses to connect to
	ManagerAddrs []string

	// Cache settings
	CacheMaxItems int
	CacheTTL      time.Duration

	// Connection settings
	DialTimeout    time.Duration
	RequestTimeout time.Duration

	// Retry settings
	StreamReconnectDelay time.Duration
}

// DefaultRemoteStoreConfig returns sensible defaults
func DefaultRemoteStoreConfig() RemoteStoreConfig {
	return RemoteStoreConfig{
		CacheMaxItems:        1000000, // 1 million items
		CacheTTL:             5 * time.Minute,
		DialTimeout:          5 * time.Second,
		RequestTimeout:       10 * time.Second,
		StreamReconnectDelay: 5 * time.Second,
	}
}

// NewRemoteCredentialStore creates a new remote credential store
func NewRemoteCredentialStore(ctx context.Context, cfg RemoteStoreConfig) (*RemoteCredentialStore, error) {
	if len(cfg.ManagerAddrs) == 0 {
		return nil, fmt.Errorf("at least one manager address required")
	}

	// Apply defaults
	if cfg.CacheMaxItems <= 0 {
		cfg.CacheMaxItems = 1000000
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 5 * time.Minute
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	if cfg.StreamReconnectDelay <= 0 {
		cfg.StreamReconnectDelay = 5 * time.Second
	}

	store := &RemoteCredentialStore{
		managerAddrs: cfg.ManagerAddrs,
		cache: cache.New[string, *cachedCredential](ctx,
			cache.WithMaxSize[string, *cachedCredential](cfg.CacheMaxItems),
			cache.WithExpiry[string, *cachedCredential](cfg.CacheTTL),
		),
		policies:     make(map[string]*Policy),
		userPolicies: make(map[string][]string),
		userGroups:   make(map[string][]string),
		config:       cfg,
	}

	// Connect to manager
	if err := store.connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to manager: %w", err)
	}

	return store, nil
}

// connect establishes gRPC connection to manager
func (r *RemoteCredentialStore) connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.conn != nil {
		r.conn.Close()
	}

	// Try each manager address
	var lastErr error
	for _, addr := range r.managerAddrs {
		// Use passthrough resolver to bypass DNS issues with Docker hostnames
		// gRPC's default DNS resolver doesn't work well in all Docker network configs
		target := addr
		if !strings.Contains(addr, "://") {
			target = "passthrough:///" + addr
		}
		conn, err := grpc.NewClient(target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			lastErr = err
			logger.Warn().Err(err).Str("addr", addr).Msg("failed to create client for manager")
			continue
		}

		// Trigger connection and wait for it to be ready
		conn.Connect()
		dialCtx, cancel := context.WithTimeout(ctx, r.config.DialTimeout)
		if !waitForReady(dialCtx, conn) {
			cancel()
			conn.Close()
			lastErr = fmt.Errorf("connection timeout")
			logger.Warn().Str("addr", addr).Msg("failed to connect to manager: timeout")
			continue
		}
		cancel()

		r.conn = conn
		r.client = iam_pb.NewIAMServiceClient(conn)
		logger.Info().Str("addr", addr).Msg("connected to IAM manager")
		return nil
	}

	return fmt.Errorf("failed to connect to any manager: %w", lastErr)
}

// waitForReady waits for the connection to become ready or the context to be done
func waitForReady(ctx context.Context, conn *grpc.ClientConn) bool {
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return true
		}
		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			return false
		}
		if !conn.WaitForStateChange(ctx, state) {
			return false // context done
		}
	}
}

// GetUserByAccessKey implements CredentialStore
func (r *RemoteCredentialStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*Identity, *Credential, error) {
	// Check cache first
	if cached, ok := r.cache.Get(accessKey); ok {
		return cached.identity, cached.credential, nil
	}

	// Query manager
	r.mu.RLock()
	client := r.client
	r.mu.RUnlock()

	if client == nil {
		return nil, nil, fmt.Errorf("not connected to manager")
	}

	reqCtx, cancel := context.WithTimeout(ctx, r.config.RequestTimeout)
	defer cancel()

	resp, err := client.GetCredential(reqCtx, &iam_pb.GetCredentialRequest{
		AccessKey:     accessKey,
		IncludeSecret: true, // Request decrypted secret for signature verification
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get credential from manager: %w", err)
	}

	if !resp.Found {
		return nil, nil, ErrAccessKeyNotFound
	}

	// Convert proto to domain types
	identity, cred := protoToCredential(resp.Credential)

	// Cache the result
	r.cache.Set(accessKey, &cachedCredential{
		identity:   identity,
		credential: cred,
	})

	return identity, cred, nil
}

// StartStreaming starts the credential change stream
func (r *RemoteCredentialStore) StartStreaming(ctx context.Context) error {
	if r.streaming.Load() {
		return nil // Already streaming
	}

	r.streamCtx, r.streamCancel = context.WithCancel(ctx)
	r.streaming.Store(true)

	go r.streamLoop()
	return nil
}

// streamLoop handles credential streaming with reconnection
func (r *RemoteCredentialStore) streamLoop() {
	defer r.streaming.Store(false)

	for {
		select {
		case <-r.streamCtx.Done():
			return
		default:
		}

		if err := r.doStream(); err != nil {
			logger.Warn().Err(err).Msg("credential stream disconnected, reconnecting...")
			time.Sleep(r.config.StreamReconnectDelay)

			// Try to reconnect
			if err := r.connect(r.streamCtx); err != nil {
				logger.Error().Err(err).Msg("failed to reconnect to manager")
			}
		}
	}
}

func (r *RemoteCredentialStore) doStream() error {
	r.mu.RLock()
	client := r.client
	r.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("not connected")
	}

	stream, err := client.StreamCredentials(r.streamCtx, &iam_pb.StreamCredentialsRequest{
		SinceVersion: r.version.Load(),
	})
	if err != nil {
		return err
	}

	logger.Info().Uint64("since_version", r.version.Load()).Msg("started credential stream")

	for {
		event, err := stream.Recv()
		if err != nil {
			return err
		}

		r.handleCredentialEvent(event)
	}
}

func (r *RemoteCredentialStore) handleCredentialEvent(event *iam_pb.CredentialEvent) {
	identity, cred := protoToCredential(event.Credential)

	switch event.Type {
	case iam_pb.CredentialEvent_EVENT_TYPE_CREATED:
		// New credential - add to cache
		r.cache.Set(event.Credential.AccessKey, &cachedCredential{
			identity:   identity,
			credential: cred,
		})
		// Store policy and group info
		r.storePoliciesAndGroups(event.Credential)
		logger.Debug().
			Str("access_key", event.Credential.AccessKey).
			Str("type", event.Type.String()).
			Uint64("version", event.Version).
			Int("policies", len(event.Credential.Policies)).
			Msg("credential created")

	case iam_pb.CredentialEvent_EVENT_TYPE_UPDATED:
		// Update - invalidate first then set (bypass TTL)
		r.cache.Delete(event.Credential.AccessKey)
		r.cache.Set(event.Credential.AccessKey, &cachedCredential{
			identity:   identity,
			credential: cred,
		})
		// Update policy and group info
		r.storePoliciesAndGroups(event.Credential)
		logger.Debug().
			Str("access_key", event.Credential.AccessKey).
			Str("type", event.Type.String()).
			Uint64("version", event.Version).
			Msg("credential updated (cache invalidated)")

	case iam_pb.CredentialEvent_EVENT_TYPE_DELETED:
		// Delete - remove from cache immediately
		r.cache.Delete(event.Credential.AccessKey)
		// Remove user's policy/group mappings
		r.removePoliciesAndGroups(event.Credential.Username)
		logger.Debug().
			Str("access_key", event.Credential.AccessKey).
			Uint64("version", event.Version).
			Msg("credential deleted")
	}

	r.version.Store(event.Version)
}

// storePoliciesAndGroups caches the user's policies and groups from a credential event
func (r *RemoteCredentialStore) storePoliciesAndGroups(pb *iam_pb.Credential) {
	r.policyMu.Lock()
	defer r.policyMu.Unlock()

	username := pb.Username

	// Store user's policy names and groups
	r.userPolicies[username] = pb.PolicyNames
	r.userGroups[username] = pb.GroupNames

	// Store full policy documents
	for _, p := range pb.Policies {
		if p == nil {
			continue
		}
		policy, err := PolicyFromJSON(p.Document)
		if err != nil {
			logger.Warn().Err(err).Str("policy", p.Name).Msg("failed to parse policy document")
			continue
		}
		policy.ID = p.Name
		r.policies[p.Name] = policy
	}
}

// removePoliciesAndGroups removes cached policy/group info for a user
func (r *RemoteCredentialStore) removePoliciesAndGroups(username string) {
	r.policyMu.Lock()
	defer r.policyMu.Unlock()

	delete(r.userPolicies, username)
	delete(r.userGroups, username)
	// Note: We don't remove policies as they may be used by other users
}

// InitialSync performs a full sync of all credentials
func (r *RemoteCredentialStore) InitialSync(ctx context.Context) error {
	r.mu.RLock()
	client := r.client
	r.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("not connected to manager")
	}

	stream, err := client.ListCredentials(ctx, &iam_pb.ListCredentialsRequest{})
	if err != nil {
		return fmt.Errorf("failed to list credentials: %w", err)
	}

	count := 0
	for {
		cred, err := stream.Recv()
		if err != nil {
			break // End of stream
		}

		identity, credential := protoToCredential(cred)
		r.cache.Set(cred.AccessKey, &cachedCredential{
			identity:   identity,
			credential: credential,
		})
		count++
	}

	logger.Info().Int("count", count).Msg("initial credential sync complete")
	return nil
}

// StopStreaming stops the credential change stream
func (r *RemoteCredentialStore) StopStreaming() {
	if r.streamCancel != nil {
		r.streamCancel()
	}
}

// Close closes the connection
func (r *RemoteCredentialStore) Close() error {
	r.StopStreaming()
	r.cache.Stop()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// CacheSize returns the current cache size
func (r *RemoteCredentialStore) CacheSize() int {
	return r.cache.Size()
}

// Version returns the current IAM version
func (r *RemoteCredentialStore) Version() uint64 {
	return r.version.Load()
}

// === PolicyStore interface implementation ===

// GetUserPolicies returns all policies attached to a user
func (r *RemoteCredentialStore) GetUserPolicies(ctx context.Context, username string) ([]*Policy, error) {
	r.policyMu.RLock()
	defer r.policyMu.RUnlock()

	policyNames := r.userPolicies[username]
	var policies []*Policy

	for _, name := range policyNames {
		if p, ok := r.policies[name]; ok {
			policies = append(policies, p)
		}
	}

	return policies, nil
}

// GetGroupPolicies returns all policies attached to a group
func (r *RemoteCredentialStore) GetGroupPolicies(ctx context.Context, groupName string) ([]*Policy, error) {
	// Group policies are embedded in user credentials when synced
	// For now, return empty - group policies come through user sync
	return nil, nil
}

// GetRolePolicies returns all policies attached to a role
func (r *RemoteCredentialStore) GetRolePolicies(ctx context.Context, roleName string) ([]*Policy, error) {
	// Roles not yet implemented
	return nil, nil
}

// === GroupStore interface implementation ===

// GetUserGroups returns the groups a user belongs to
func (r *RemoteCredentialStore) GetUserGroups(ctx context.Context, username string) ([]string, error) {
	r.policyMu.RLock()
	defer r.policyMu.RUnlock()

	groups := r.userGroups[username]
	if groups == nil {
		return []string{}, nil
	}
	return groups, nil
}

// === CredentialStore interface methods (pass-through or unsupported) ===

func (r *RemoteCredentialStore) CreateUser(ctx context.Context, identity *Identity) error {
	return fmt.Errorf("CreateUser not supported on remote store - use manager admin API")
}

func (r *RemoteCredentialStore) GetUser(ctx context.Context, username string) (*Identity, error) {
	return nil, fmt.Errorf("GetUser by username not supported on remote store")
}

func (r *RemoteCredentialStore) UpdateUser(ctx context.Context, identity *Identity) error {
	return fmt.Errorf("UpdateUser not supported on remote store - use manager admin API")
}

func (r *RemoteCredentialStore) DeleteUser(ctx context.Context, username string) error {
	return fmt.Errorf("DeleteUser not supported on remote store - use manager admin API")
}

func (r *RemoteCredentialStore) ListUsers(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("ListUsers not supported on remote store - use manager admin API")
}

func (r *RemoteCredentialStore) CreateAccessKey(ctx context.Context, username string, cred *Credential) error {
	return fmt.Errorf("CreateAccessKey not supported on remote store - use manager admin API")
}

func (r *RemoteCredentialStore) DeleteAccessKey(ctx context.Context, username, accessKey string) error {
	return fmt.Errorf("DeleteAccessKey not supported on remote store - use manager admin API")
}

// === Helper functions ===

func protoToCredential(pb *iam_pb.Credential) (*Identity, *Credential) {
	if pb == nil {
		return nil, nil
	}

	identity := &Identity{
		Name:     pb.Username,
		Disabled: pb.Disabled,
		Account: &Account{
			ID:           pb.AccountId,
			DisplayName:  pb.DisplayName,
			EmailAddress: pb.Email,
		},
	}

	var createdAt time.Time
	if pb.CreatedAt != nil {
		createdAt = pb.CreatedAt.AsTime()
	}

	var expiresAt *time.Time
	if pb.ExpiresAt != nil {
		t := pb.ExpiresAt.AsTime()
		expiresAt = &t
	}

	cred := &Credential{
		AccessKey: pb.AccessKey,
		SecretKey: pb.SecretKey, // Decrypted secret for local signature verification
		Status:    pb.Status,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	}

	return identity, cred
}
