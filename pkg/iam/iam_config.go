package iam

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// IAMConfig holds the complete IAM configuration loaded from TOML/JSON.
// This is the source of truth for users, groups, and policies in local development.
//
// Example TOML:
//
//	[iam]
//	cache_max_items = 10000
//	cache_ttl = "5m"
//	enable_sts = true
//	enable_kms = true
//
//	[[iam.users]]
//	name = "admin"
//	access_key = "AKIAEXAMPLE123456"
//	secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
//	policies = ["FullAccess"]
//
//	[[iam.users]]
//	name = "developer"
//	access_key = "AKIADEVKEY789012"
//	secret_key = "devSecretKey1234567890123456789012345"
//	groups = ["developers"]
//
//	[[iam.groups]]
//	name = "developers"
//	policies = ["ReadOnly", "DevBucketAccess"]
//
//	[[iam.policies]]
//	name = "FullAccess"
//	document = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}'
//
//	[[iam.policies]]
//	name = "ReadOnly"
//	document = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:Get*","s3:List*"],"Resource":"*"}]}'
type IAMConfig struct {
	// Cache settings
	CacheMaxItems int    `mapstructure:"cache_max_items"`
	CacheTTL      string `mapstructure:"cache_ttl"`

	// AWS-compatible services
	EnableSTS bool `mapstructure:"enable_sts"`
	EnableKMS bool `mapstructure:"enable_kms"`

	// STS settings
	STSDefaultDuration string `mapstructure:"sts_default_duration"`
	STSMaxDuration     string `mapstructure:"sts_max_duration"`

	// Users, groups, and policies
	Users    []UserConfig   `mapstructure:"users"`
	Groups   []GroupConfig  `mapstructure:"groups"`
	Policies []PolicyConfig `mapstructure:"policies"`
}

// UserConfig represents a user in the configuration
type UserConfig struct {
	Name        string   `mapstructure:"name"`
	AccessKey   string   `mapstructure:"access_key"`
	SecretKey   string   `mapstructure:"secret_key"`
	AccountID   string   `mapstructure:"account_id"`
	DisplayName string   `mapstructure:"display_name"`
	Email       string   `mapstructure:"email"`
	Disabled    bool     `mapstructure:"disabled"`
	Policies    []string `mapstructure:"policies"` // Policy names attached directly
	Groups      []string `mapstructure:"groups"`   // Groups this user belongs to
}

// GroupConfig represents a group in the configuration
type GroupConfig struct {
	Name     string   `mapstructure:"name"`
	Policies []string `mapstructure:"policies"` // Policy names attached to this group
}

// PolicyConfig represents a policy in the configuration
type PolicyConfig struct {
	Name     string `mapstructure:"name"`
	Document string `mapstructure:"document"` // JSON policy document
}

// DefaultIAMConfig returns default configuration for development
func DefaultIAMConfig() IAMConfig {
	return IAMConfig{
		CacheMaxItems: 10000,
		CacheTTL:      "5m",
		EnableSTS:     false,
		EnableKMS:     false,
		Users: []UserConfig{
			{
				Name:        "admin",
				AccessKey:   "test-access-key",
				SecretKey:   "test-secret-key",
				AccountID:   "000000000001",
				DisplayName: "Admin User",
				Email:       "admin@localhost",
				Policies:    []string{"FullAccess"},
			},
			{
				Name:        "developer",
				AccessKey:   "dev-access-key",
				SecretKey:   "dev-secret-key",
				AccountID:   "000000000002",
				DisplayName: "Developer User",
				Email:       "developer@localhost",
				Policies:    []string{"ReadOnly"},
			},
		},
		Policies: []PolicyConfig{
			{
				Name:     "FullAccess",
				Document: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
			},
			{
				Name:     "ReadOnly",
				Document: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:Get*","s3:List*","s3:Head*"],"Resource":"*"}]}`,
			},
		},
	}
}

// LoadFromConfig creates an IAM Service from configuration.
// This is the primary way to initialize IAM in a ZapFS deployment.
func LoadFromConfig(cfg IAMConfig) (*Service, error) {
	// Parse cache TTL
	cacheTTL := 5 * time.Minute
	if cfg.CacheTTL != "" {
		var err error
		cacheTTL, err = time.ParseDuration(cfg.CacheTTL)
		if err != nil {
			return nil, fmt.Errorf("invalid cache_ttl %q: %w", cfg.CacheTTL, err)
		}
	}

	cacheMaxItems := cfg.CacheMaxItems
	if cacheMaxItems <= 0 {
		cacheMaxItems = 10000
	}

	// Parse policies first (needed for user/group attachment)
	policyMap := make(map[string]*Policy)
	for _, pc := range cfg.Policies {
		policy, err := parsePolicyDocument(pc.Name, pc.Document)
		if err != nil {
			return nil, fmt.Errorf("invalid policy %q: %w", pc.Name, err)
		}
		policyMap[pc.Name] = policy
	}

	// Create credential store with users
	credStore := NewMemoryStore()
	for _, u := range cfg.Users {
		identity := &Identity{
			Name:     u.Name,
			Disabled: u.Disabled,
			Account: &Account{
				ID:           u.AccountID,
				DisplayName:  u.DisplayName,
				EmailAddress: u.Email,
			},
			Credentials: []*Credential{{
				AccessKey:   u.AccessKey,
				SecretKey:   u.SecretKey,
				Status:      "Active",
				CreatedAt:   time.Now(),
				Description: "Loaded from configuration",
			}},
		}

		if identity.Account.ID == "" {
			identity.Account.ID = generateID()
		}
		if identity.Account.DisplayName == "" {
			identity.Account.DisplayName = u.Name
		}

		if err := credStore.CreateUser(context.Background(), identity); err != nil {
			return nil, fmt.Errorf("failed to create user %q: %w", u.Name, err)
		}
	}

	// Create policy store with groups
	policyStore := NewMemoryPolicyStore()

	// Create groups
	for _, g := range cfg.Groups {
		policyStore.CreateGroup(g.Name)
		for _, policyName := range g.Policies {
			if policy, ok := policyMap[policyName]; ok {
				policyStore.AttachGroupPolicy(g.Name, policy)
			}
		}
	}

	// Attach policies to users and add users to groups
	for _, u := range cfg.Users {
		// Attach user policies
		for _, policyName := range u.Policies {
			if policy, ok := policyMap[policyName]; ok {
				policyStore.AttachUserPolicy(u.Name, policy)
			}
		}
		// Add to groups
		for _, groupName := range u.Groups {
			policyStore.AddUserToGroup(u.Name, groupName)
		}
	}

	// Parse STS config
	stsConfig := DefaultSTSConfig()
	if cfg.STSDefaultDuration != "" {
		if d, err := time.ParseDuration(cfg.STSDefaultDuration); err == nil {
			stsConfig.DefaultSessionDuration = d
		}
	}
	if cfg.STSMaxDuration != "" {
		if d, err := time.ParseDuration(cfg.STSMaxDuration); err == nil {
			stsConfig.MaxSessionDuration = d
		}
	}

	// Create service
	return NewService(ServiceConfig{
		CredentialStore: credStore,
		PolicyStore:     policyStore,
		GroupStore:      policyStore,
		CacheMaxItems:   cacheMaxItems,
		CacheTTL:        cacheTTL,
		EnableSTS:       cfg.EnableSTS,
		EnableKMS:       cfg.EnableKMS,
		STSConfig:       stsConfig,
	})
}

// parsePolicyDocument parses a JSON policy document
func parsePolicyDocument(name, document string) (*Policy, error) {
	if document == "" {
		return nil, fmt.Errorf("empty policy document")
	}

	var policy Policy
	if err := json.Unmarshal([]byte(document), &policy); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	policy.ID = name
	return &policy, nil
}
