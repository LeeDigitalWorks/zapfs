//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package ldap provides LDAP and Active Directory integration for enterprise IAM.
// This package is only available in the enterprise edition of ZapFS.
package ldap

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/go-ldap/ldap/v3"
)

// ErrInvalidCredentials is returned when LDAP authentication fails
var ErrInvalidCredentials = errors.New("invalid credentials")

// Store integrates with an LDAP/Active Directory server for authentication
// while maintaining a local mapping of access keys to LDAP users.
//
// LDAP is used for:
// - Verifying user exists and is active
// - Getting user attributes (email, groups)
//
// Local store (FileStore/DBStore) is used for:
// - Storing S3 access keys (LDAP doesn't have AWS-style credentials)
type Store struct {
	config Config

	// Local storage for access key <-> LDAP user mapping
	localStore iam.CredentialStore

	// Connection pool for LDAP
	pool     chan *ldap.Conn
	poolSize int
}

// Config holds LDAP connection and attribute configuration
type Config struct {
	// Server settings
	ServerURL  string        // ldap://localhost:389 or ldaps://localhost:636
	BindDN     string        // cn=admin,dc=example,dc=com
	BindPass   string        // admin password for binding
	BaseDN     string        // dc=example,dc=com
	UserFilter string        // (uid=%s) or (sAMAccountName=%s)
	TLS        *tls.Config   // Optional TLS config
	Timeout    time.Duration // Connection timeout
	StartTLS   bool          // Use StartTLS for connection upgrade

	// Attribute mapping
	UsernameAttr string // uid, sAMAccountName, cn
	EmailAttr    string // mail
	GroupAttr    string // memberOf

	// Group-based access control
	RequiredGroup  string              // Optional: cn=s3-users,ou=groups,dc=example,dc=com
	GroupPolicyMap map[string][]string // LDAP group DN -> policy names

	// Connection pool settings
	PoolSize int // Number of connections to keep (default: 5)
}

// NewStore creates a new LDAP-backed credential store
func NewStore(config Config, localStore iam.CredentialStore) (*Store, error) {
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
	if config.UsernameAttr == "" {
		config.UsernameAttr = "uid"
	}
	if config.EmailAttr == "" {
		config.EmailAttr = "mail"
	}
	if config.GroupAttr == "" {
		config.GroupAttr = "memberOf"
	}
	if config.PoolSize == 0 {
		config.PoolSize = 5
	}

	store := &Store{
		config:     config,
		localStore: localStore,
		pool:       make(chan *ldap.Conn, config.PoolSize),
		poolSize:   config.PoolSize,
	}

	if err := store.testConnection(); err != nil {
		return nil, fmt.Errorf("LDAP connection test failed: %w", err)
	}

	return store, nil
}

func (s *Store) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam.Identity, *iam.Credential, error) {
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
		identity.Account = &iam.Account{}
	}
	identity.Account.EmailAddress = ldapUser.Email
	identity.Disabled = !ldapUser.Active

	return identity, cred, nil
}

func (s *Store) CreateUser(ctx context.Context, identity *iam.Identity) error {
	// Verify user exists in LDAP first
	ldapUser, err := s.lookupLDAPUser(identity.Name)
	if err != nil {
		return fmt.Errorf("user not found in LDAP: %w", err)
	}

	// Merge LDAP attributes
	if identity.Account == nil {
		identity.Account = &iam.Account{}
	}
	identity.Account.EmailAddress = ldapUser.Email

	return s.localStore.CreateUser(ctx, identity)
}

func (s *Store) GetUser(ctx context.Context, username string) (*iam.Identity, error) {
	identity, err := s.localStore.GetUser(ctx, username)
	if err != nil {
		// Try to create from LDAP
		ldapUser, ldapErr := s.lookupLDAPUser(username)
		if ldapErr != nil {
			return nil, iam.ErrUserNotFound
		}

		return &iam.Identity{
			Name: username,
			Account: &iam.Account{
				EmailAddress: ldapUser.Email,
			},
			Disabled: !ldapUser.Active,
		}, nil
	}

	// Refresh from LDAP
	ldapUser, ldapErr := s.lookupLDAPUser(username)
	if ldapErr == nil {
		if identity.Account == nil {
			identity.Account = &iam.Account{}
		}
		identity.Account.EmailAddress = ldapUser.Email
		identity.Disabled = !ldapUser.Active
	}

	return identity, nil
}

func (s *Store) UpdateUser(ctx context.Context, identity *iam.Identity) error {
	return s.localStore.UpdateUser(ctx, identity)
}

func (s *Store) DeleteUser(ctx context.Context, username string) error {
	return s.localStore.DeleteUser(ctx, username)
}

func (s *Store) ListUsers(ctx context.Context) ([]string, error) {
	return s.localStore.ListUsers(ctx)
}

func (s *Store) CreateAccessKey(ctx context.Context, username string, cred *iam.Credential) error {
	// Verify user exists in LDAP
	if _, err := s.lookupLDAPUser(username); err != nil {
		return fmt.Errorf("user not found in LDAP: %w", err)
	}
	return s.localStore.CreateAccessKey(ctx, username, cred)
}

func (s *Store) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	return s.localStore.DeleteAccessKey(ctx, username, accessKey)
}

// LDAP-specific types and methods

type ldapUser struct {
	Username string
	Email    string
	Groups   []string // LDAP group memberships
	Active   bool
	DN       string // Distinguished Name
}

// dial creates a new LDAP connection with proper TLS/StartTLS handling
func (s *Store) dial() (*ldap.Conn, error) {
	conn, err := ldap.DialURL(s.config.ServerURL, ldap.DialWithDialer(&net.Dialer{Timeout: s.config.Timeout}))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to LDAP server: %w", err)
	}

	// Apply StartTLS if configured and not already using ldaps://
	if s.config.StartTLS && !strings.HasPrefix(s.config.ServerURL, "ldaps://") {
		tlsConfig := s.config.TLS
		if tlsConfig == nil {
			tlsConfig = &tls.Config{InsecureSkipVerify: false}
		}
		if err := conn.StartTLS(tlsConfig); err != nil {
			conn.Close()
			return nil, fmt.Errorf("StartTLS failed: %w", err)
		}
	}

	// Bind with service account if configured
	if s.config.BindDN != "" {
		if err := conn.Bind(s.config.BindDN, s.config.BindPass); err != nil {
			conn.Close()
			return nil, fmt.Errorf("LDAP bind failed: %w", err)
		}
	}

	return conn, nil
}

// getConnection gets a connection from pool or creates a new one
func (s *Store) getConnection() (*ldap.Conn, error) {
	select {
	case conn := <-s.pool:
		// Test if connection is still valid
		if conn.IsClosing() {
			return s.dial()
		}
		return conn, nil
	default:
		return s.dial()
	}
}

// returnConnection returns a connection to the pool
func (s *Store) returnConnection(conn *ldap.Conn) {
	if conn == nil || conn.IsClosing() {
		return
	}
	select {
	case s.pool <- conn:
		// Returned to pool
	default:
		// Pool is full, close connection
		conn.Close()
	}
}

func (s *Store) testConnection() error {
	conn, err := s.dial()
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}

func (s *Store) lookupLDAPUser(username string) (*ldapUser, error) {
	conn, err := s.getConnection()
	if err != nil {
		return nil, err
	}
	defer s.returnConnection(conn)

	// Build search filter with proper escaping
	filter := fmt.Sprintf(s.config.UserFilter, ldap.EscapeFilter(username))

	// Build attribute list
	attributes := []string{s.config.UsernameAttr, s.config.EmailAttr, s.config.GroupAttr, "dn"}

	req := ldap.NewSearchRequest(
		s.config.BaseDN,
		ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases,
		1,                                 // SizeLimit: only need 1 result
		int(s.config.Timeout/time.Second), // TimeLimit
		false,                             // TypesOnly
		filter,
		attributes,
		nil,
	)

	result, err := conn.Search(req)
	if err != nil {
		return nil, fmt.Errorf("LDAP search failed: %w", err)
	}

	if len(result.Entries) == 0 {
		return nil, iam.ErrUserNotFound
	}

	entry := result.Entries[0]
	user := &ldapUser{
		Username: entry.GetAttributeValue(s.config.UsernameAttr),
		Email:    entry.GetAttributeValue(s.config.EmailAttr),
		Groups:   entry.GetAttributeValues(s.config.GroupAttr),
		DN:       entry.DN,
		Active:   true, // User exists in LDAP = active
	}

	// Check required group membership if configured
	if s.config.RequiredGroup != "" {
		hasGroup := false
		for _, group := range user.Groups {
			if strings.EqualFold(group, s.config.RequiredGroup) {
				hasGroup = true
				break
			}
		}
		if !hasGroup {
			return nil, fmt.Errorf("user not member of required group: %s", s.config.RequiredGroup)
		}
	}

	return user, nil
}

// AuthenticateLDAP verifies username/password against LDAP using bind authentication.
// Can be used for admin UI authentication.
func (s *Store) AuthenticateLDAP(username, password string) (*iam.Identity, error) {
	if password == "" {
		return nil, errors.New("password cannot be empty")
	}

	// First lookup user to get their DN
	user, err := s.lookupLDAPUser(username)
	if err != nil {
		return nil, fmt.Errorf("user lookup failed: %w", err)
	}

	// Create a new connection for user bind (don't use pooled connection)
	conn, err := ldap.DialURL(s.config.ServerURL, ldap.DialWithDialer(&net.Dialer{Timeout: s.config.Timeout}))
	if err != nil {
		return nil, fmt.Errorf("failed to connect for auth: %w", err)
	}
	defer conn.Close()

	// Apply StartTLS if configured
	if s.config.StartTLS && !strings.HasPrefix(s.config.ServerURL, "ldaps://") {
		tlsConfig := s.config.TLS
		if tlsConfig == nil {
			tlsConfig = &tls.Config{InsecureSkipVerify: false}
		}
		if err := conn.StartTLS(tlsConfig); err != nil {
			return nil, fmt.Errorf("StartTLS failed: %w", err)
		}
	}

	// Try to bind as the user
	if err := conn.Bind(user.DN, password); err != nil {
		return nil, ErrInvalidCredentials
	}

	// Authentication successful - build identity
	identity := &iam.Identity{
		Name:     username,
		Disabled: !user.Active,
		Account: &iam.Account{
			EmailAddress: user.Email,
		},
	}

	return identity, nil
}

// Close closes all pooled connections
func (s *Store) Close() error {
	close(s.pool)
	for conn := range s.pool {
		conn.Close()
	}
	return nil
}

// GetPoliciesForGroups returns policy names for the given LDAP groups
func (s *Store) GetPoliciesForGroups(groups []string) []string {
	if s.config.GroupPolicyMap == nil {
		return nil
	}

	var policies []string
	seen := make(map[string]bool)

	for _, group := range groups {
		if policyNames, ok := s.config.GroupPolicyMap[group]; ok {
			for _, p := range policyNames {
				if !seen[p] {
					seen[p] = true
					policies = append(policies, p)
				}
			}
		}
	}

	return policies
}
