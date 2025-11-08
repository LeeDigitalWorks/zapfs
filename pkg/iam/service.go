package iam

import (
	"context"
	"time"
)

// Service provides a unified interface for all IAM operations.
// It combines credential management, policy evaluation, and optional
// AWS-compatible services (STS, KMS) into a single entry point.
//
// Usage:
//
//	// Option 1: Load from TOML config (recommended)
//	svc, err := iam.LoadFromConfig(myIAMConfig)
//
//	// Option 2: Use defaults for development
//	svc, err := iam.NewServiceWithDefaults()
//
//	// Use in metadata filters
//	authFilter := filter.NewAuthenticationFilter(svc.Manager())
//	authzFilter := filter.NewAuthorizationFilter(filter.AuthorizationConfig{
//	    IAMEvaluator: svc.Evaluator(),
//	})
type Service struct {
	// Credential management
	credStore CredentialStore
	manager   *Manager

	// Policy management
	policyStore PolicyStore
	groupStore  GroupStore
	evaluator   *PolicyEvaluator

	// Optional AWS-compatible services
	sts *STSService
	kms *KMSService

	config ServiceConfig
}

// ServiceConfig holds configuration for the IAM service
type ServiceConfig struct {
	// Required: credential store for user/access key management
	CredentialStore CredentialStore

	// Optional: policy and group stores for authorization
	PolicyStore PolicyStore
	GroupStore  GroupStore

	// Cache settings for credential lookups
	CacheMaxItems int
	CacheTTL      time.Duration

	// Enable AWS-compatible services
	EnableSTS bool
	EnableKMS bool

	// STS configuration
	STSConfig STSConfig
}

// DefaultServiceConfig returns a ServiceConfig with sensible defaults
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		CacheMaxItems: 10000,
		CacheTTL:      5 * time.Minute,
		EnableSTS:     false,
		EnableKMS:     false,
		STSConfig:     DefaultSTSConfig(),
	}
}

// NewService creates a new IAM service with the given configuration
func NewService(cfg ServiceConfig) (*Service, error) {
	if cfg.CredentialStore == nil {
		return nil, ErrUserNotFound // Should have a better error
	}

	// Apply defaults
	if cfg.CacheMaxItems <= 0 {
		cfg.CacheMaxItems = 10000
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 5 * time.Minute
	}

	svc := &Service{
		credStore:   cfg.CredentialStore,
		manager:     NewManagerWithCache(cfg.CredentialStore, cfg.CacheMaxItems, cfg.CacheTTL),
		policyStore: cfg.PolicyStore,
		groupStore:  cfg.GroupStore,
		config:      cfg,
	}

	// Create policy evaluator if stores are provided
	if cfg.PolicyStore != nil {
		svc.evaluator = NewPolicyEvaluator(cfg.PolicyStore, cfg.GroupStore)
	}

	// Initialize optional services
	if cfg.EnableSTS {
		svc.sts = NewSTSService(svc.manager, cfg.STSConfig)
	}

	if cfg.EnableKMS {
		svc.kms = NewKMSService()
	}

	return svc, nil
}

// NewServiceWithDefaults creates an IAM service with default development configuration.
// This loads the default IAM config which includes:
//   - admin user with test-access-key/test-secret-key (full access)
//   - developer user with dev-access-key/dev-secret-key (read only)
//
// For production, use LoadFromConfig with your own IAMConfig.
func NewServiceWithDefaults() (*Service, error) {
	return LoadFromConfig(DefaultIAMConfig())
}

// --- Accessors for components ---

// Manager returns the credential manager for authentication
func (s *Service) Manager() *Manager {
	return s.manager
}

// Evaluator returns the policy evaluator for authorization
func (s *Service) Evaluator() *PolicyEvaluator {
	return s.evaluator
}

// CredentialStore returns the underlying credential store
func (s *Service) CredentialStore() CredentialStore {
	return s.credStore
}

// PolicyStore returns the policy store (may be nil)
func (s *Service) PolicyStore() PolicyStore {
	return s.policyStore
}

// GroupStore returns the group store (may be nil)
func (s *Service) GroupStore() GroupStore {
	return s.groupStore
}

// STS returns the STS service (may be nil if not enabled)
func (s *Service) STS() *STSService {
	return s.sts
}

// KMS returns the KMS service (may be nil if not enabled)
func (s *Service) KMS() *KMSService {
	return s.kms
}

// --- Credential operations (delegated to store) ---

// LookupByAccessKey retrieves identity and credential by access key (with caching)
func (s *Service) LookupByAccessKey(ctx context.Context, accessKey string) (*Identity, *Credential, bool) {
	return s.manager.LookupByAccessKey(ctx, accessKey)
}

// CreateUser creates a new user
func (s *Service) CreateUser(ctx context.Context, identity *Identity) error {
	err := s.credStore.CreateUser(ctx, identity)
	if err != nil {
		return err
	}
	// Invalidate cache for new access keys
	for _, cred := range identity.Credentials {
		s.manager.InvalidateAccessKey(cred.AccessKey)
	}
	return nil
}

// GetUser retrieves a user by name
func (s *Service) GetUser(ctx context.Context, username string) (*Identity, error) {
	return s.credStore.GetUser(ctx, username)
}

// UpdateUser updates an existing user
func (s *Service) UpdateUser(ctx context.Context, identity *Identity) error {
	// Get old identity to invalidate old access keys
	oldIdentity, _ := s.credStore.GetUser(ctx, identity.Name)

	err := s.credStore.UpdateUser(ctx, identity)
	if err != nil {
		return err
	}

	// Invalidate cache for old and new access keys
	if oldIdentity != nil {
		for _, cred := range oldIdentity.Credentials {
			s.manager.InvalidateAccessKey(cred.AccessKey)
		}
	}
	for _, cred := range identity.Credentials {
		s.manager.InvalidateAccessKey(cred.AccessKey)
	}

	return nil
}

// DeleteUser deletes a user
func (s *Service) DeleteUser(ctx context.Context, username string) error {
	// Get identity to invalidate access keys
	identity, _ := s.credStore.GetUser(ctx, username)

	err := s.credStore.DeleteUser(ctx, username)
	if err != nil {
		return err
	}

	// Invalidate cache
	if identity != nil {
		for _, cred := range identity.Credentials {
			s.manager.InvalidateAccessKey(cred.AccessKey)
		}
	}

	return nil
}

// ListUsers returns all usernames
func (s *Service) ListUsers(ctx context.Context) ([]string, error) {
	return s.credStore.ListUsers(ctx)
}

// CreateAccessKey creates a new access key for a user.
// Returns the generated credential (secret key is only available at creation time).
func (s *Service) CreateAccessKey(ctx context.Context, username string) (*Credential, error) {
	cred := &Credential{
		AccessKey:   GenerateAccessKey(),
		SecretKey:   GenerateSecretKey(),
		Status:      "Active",
		CreatedAt:   time.Now(),
		Description: "Generated by IAM service",
	}

	if err := s.credStore.CreateAccessKey(ctx, username, cred); err != nil {
		return nil, err
	}

	return cred, nil
}

// DeleteAccessKey deletes an access key
func (s *Service) DeleteAccessKey(ctx context.Context, username, accessKey string) error {
	err := s.credStore.DeleteAccessKey(ctx, username, accessKey)
	if err != nil {
		return err
	}

	// Invalidate cache
	s.manager.InvalidateAccessKey(accessKey)
	return nil
}

// --- Policy operations (delegated to evaluator) ---

// Evaluate checks if the identity is allowed to perform the action
func (s *Service) Evaluate(ctx context.Context, identity *Identity, evalCtx *PolicyEvaluationContext) PolicyDecision {
	if s.evaluator == nil {
		// No policy store configured - default to allow (for development)
		return DecisionAllow
	}
	return s.evaluator.Evaluate(ctx, identity, evalCtx)
}

// IsAllowed is a convenience method that returns true only for explicit allow
func (s *Service) IsAllowed(ctx context.Context, identity *Identity, action, bucket, key string) bool {
	evalCtx := BuildEvaluationContext(action, bucket, key)
	return s.Evaluate(ctx, identity, evalCtx) == DecisionAllow
}

// --- Policy management (requires MemoryPolicyStore or similar) ---

// AttachUserPolicy attaches a policy to a user (requires MemoryPolicyStore)
func (s *Service) AttachUserPolicy(username string, policy *Policy) {
	if mps, ok := s.policyStore.(*MemoryPolicyStore); ok {
		mps.AttachUserPolicy(username, policy)
	}
}

// AttachGroupPolicy attaches a policy to a group (requires MemoryPolicyStore)
func (s *Service) AttachGroupPolicy(groupName string, policy *Policy) {
	if mps, ok := s.policyStore.(*MemoryPolicyStore); ok {
		mps.AttachGroupPolicy(groupName, policy)
	}
}

// AddUserToGroup adds a user to a group (requires MemoryPolicyStore)
func (s *Service) AddUserToGroup(username, groupName string) {
	if mps, ok := s.policyStore.(*MemoryPolicyStore); ok {
		mps.AddUserToGroup(username, groupName)
	}
}

// RemoveUserFromGroup removes a user from a group (requires MemoryPolicyStore)
func (s *Service) RemoveUserFromGroup(username, groupName string) {
	if mps, ok := s.policyStore.(*MemoryPolicyStore); ok {
		mps.RemoveUserFromGroup(username, groupName)
	}
}

// --- STS operations (delegated if enabled) ---

// AssumeRole generates temporary credentials for assuming a role
func (s *Service) AssumeRole(ctx context.Context, callerIdentity *Identity, input AssumeRoleInput) (*SessionCredential, error) {
	if s.sts == nil {
		return nil, ErrSessionNotFound // STS not enabled
	}
	return s.sts.AssumeRole(ctx, callerIdentity, input)
}

// GetSessionToken generates temporary session credentials
func (s *Service) GetSessionToken(ctx context.Context, callerIdentity *Identity, input GetSessionCredentialsInput) (*SessionCredential, error) {
	if s.sts == nil {
		return nil, ErrSessionNotFound // STS not enabled
	}
	return s.sts.GetSessionToken(ctx, callerIdentity, input)
}

// ValidateSessionToken validates a session token
func (s *Service) ValidateSessionToken(ctx context.Context, sessionToken string) (*SessionCredential, error) {
	if s.sts == nil {
		return nil, ErrSessionNotFound // STS not enabled
	}
	return s.sts.ValidateSessionToken(ctx, sessionToken)
}

// --- KMS operations (delegated if enabled) ---

// KMSCreateKey creates a new KMS key
func (s *Service) KMSCreateKey(ctx context.Context, input CreateKeyInput) (*KeyMetadata, error) {
	if s.kms == nil {
		return nil, ErrKeyNotFound // KMS not enabled
	}
	return s.kms.CreateKey(ctx, input)
}

// KMSEncrypt encrypts data using a KMS key
func (s *Service) KMSEncrypt(ctx context.Context, keyID string, plaintext []byte) ([]byte, error) {
	if s.kms == nil {
		return nil, ErrKeyNotFound // KMS not enabled
	}
	return s.kms.Encrypt(ctx, keyID, plaintext)
}

// KMSDecrypt decrypts data using a KMS key
func (s *Service) KMSDecrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error) {
	if s.kms == nil {
		return nil, ErrKeyNotFound // KMS not enabled
	}
	return s.kms.Decrypt(ctx, keyID, ciphertext)
}

// KMSGenerateDataKey generates a data encryption key
func (s *Service) KMSGenerateDataKey(ctx context.Context, keyID, keySpec string) (plaintext, ciphertext []byte, err error) {
	if s.kms == nil {
		return nil, nil, ErrKeyNotFound // KMS not enabled
	}
	return s.kms.GenerateDataKey(ctx, keyID, keySpec)
}

// InvalidateCache clears all cached credentials
func (s *Service) InvalidateCache() {
	s.manager.InvalidateCache()
}
