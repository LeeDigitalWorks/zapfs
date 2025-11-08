package iam

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"
	// "github.com/go-ldap/ldap/v3" // Uncomment when implementing
)

// LDAPStore integrates with an LDAP/Active Directory server for authentication
// while maintaining a local mapping of access keys to LDAP users.
//
// LDAP is used for:
// - Verifying user exists and is active
// - Getting user attributes (email, groups)
//
// Local store (FileStore/DBStore) is used for:
// - Storing S3 access keys (LDAP doesn't have AWS-style credentials)
type LDAPStore struct {
	config LDAPConfig

	// Local storage for access key <-> LDAP user mapping
	localStore CredentialStore
}

type LDAPConfig struct {
	// Server settings
	ServerURL  string        // ldap://localhost:389 or ldaps://localhost:636
	BindDN     string        // cn=admin,dc=example,dc=com
	BindPass   string        // admin password for binding
	BaseDN     string        // dc=example,dc=com
	UserFilter string        // (uid=%s) or (sAMAccountName=%s)
	TLS        *tls.Config   // Optional TLS config
	Timeout    time.Duration // Connection timeout

	// Attribute mapping
	UsernameAttr string // uid, sAMAccountName, cn
	EmailAttr    string // mail
	GroupAttr    string // memberOf

	// Group-based access control
	RequiredGroup string // Optional: cn=s3-users,ou=groups,dc=example,dc=com
}

// NewLDAPStore creates a new LDAP-backed credential store
func NewLDAPStore(config LDAPConfig, localStore CredentialStore) (*LDAPStore, error) {
	if config.ServerURL == "" {
		return nil, errors.New("LDAP server URL is required")
	}
	if config.BaseDN == "" {
		return nil, errors.New("LDAP base DN is required")
	}
	if config.UserFilter == "" {
		config.UserFilter = "(uid=%s)"
	}
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}

	store := &LDAPStore{
		config:     config,
		localStore: localStore,
	}

	if err := store.testConnection(); err != nil {
		return nil, fmt.Errorf("LDAP connection test failed: %w", err)
	}

	return store, nil
}

func (s *LDAPStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*Identity, *Credential, error) {
	// Get from local store
	identity, cred, err := s.localStore.GetUserByAccessKey(ctx, accessKey)
	if err != nil {
		return nil, nil, err
	}

	// Verify user still exists and is active in LDAP
	ldapUser, err := s.lookupLDAPUser(identity.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("LDAP user lookup failed: %w", err)
	}

	// Merge LDAP attributes
	if identity.Account == nil {
		identity.Account = &Account{}
	}
	identity.Account.EmailAddress = ldapUser.Email
	identity.Disabled = !ldapUser.Active

	return identity, cred, nil
}

func (s *LDAPStore) CreateUser(ctx context.Context, identity *Identity) error {
	// Verify user exists in LDAP first
	ldapUser, err := s.lookupLDAPUser(identity.Name)
	if err != nil {
		return fmt.Errorf("user not found in LDAP: %w", err)
	}

	// Merge LDAP attributes
	if identity.Account == nil {
		identity.Account = &Account{}
	}
	identity.Account.EmailAddress = ldapUser.Email

	return s.localStore.CreateUser(ctx, identity)
}

func (s *LDAPStore) GetUser(ctx context.Context, username string) (*Identity, error) {
	identity, err := s.localStore.GetUser(ctx, username)
	if err != nil {
		// Try to create from LDAP
		ldapUser, ldapErr := s.lookupLDAPUser(username)
		if ldapErr != nil {
			return nil, ErrUserNotFound
		}

		return &Identity{
			Name: username,
			Account: &Account{
				EmailAddress: ldapUser.Email,
			},
			Disabled: !ldapUser.Active,
		}, nil
	}

	// Refresh from LDAP
	ldapUser, ldapErr := s.lookupLDAPUser(username)
	if ldapErr == nil {
		if identity.Account == nil {
			identity.Account = &Account{}
		}
		identity.Account.EmailAddress = ldapUser.Email
		identity.Disabled = !ldapUser.Active
	}

	return identity, nil
}

func (s *LDAPStore) UpdateUser(ctx context.Context, identity *Identity) error {
	return s.localStore.UpdateUser(ctx, identity)
}

func (s *LDAPStore) DeleteUser(ctx context.Context, username string) error {
	return s.localStore.DeleteUser(ctx, username)
}

func (s *LDAPStore) ListUsers(ctx context.Context) ([]string, error) {
	return s.localStore.ListUsers(ctx)
}

func (s *LDAPStore) CreateAccessKey(ctx context.Context, username string, cred *Credential) error {
	// Verify user exists in LDAP
	if _, err := s.lookupLDAPUser(username); err != nil {
		return fmt.Errorf("user not found in LDAP: %w", err)
	}
	return s.localStore.CreateAccessKey(ctx, username, cred)
}

func (s *LDAPStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	return s.localStore.DeleteAccessKey(ctx, username, accessKey)
}

// LDAP-specific types and methods

type ldapUser struct {
	Username string
	Email    string
	Groups   []string // LDAP group memberships
	Active   bool
}

func (s *LDAPStore) testConnection() error {
	// TODO: Implement LDAP connection test
	return nil
}

func (s *LDAPStore) lookupLDAPUser(username string) (*ldapUser, error) {
	// TODO: Implement actual LDAP lookup
	// Placeholder for testing
	return &ldapUser{
		Username: username,
		Email:    username + "@example.com",
		Active:   true,
		Groups:   []string{},
	}, nil
}

// AuthenticateLDAP verifies username/password against LDAP
// Can be used for admin UI authentication
func (s *LDAPStore) AuthenticateLDAP(username, password string) (*Identity, error) {
	// TODO: Implement LDAP bind authentication
	return nil, errors.New("not implemented")
}
