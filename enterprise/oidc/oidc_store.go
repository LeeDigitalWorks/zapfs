//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package oidc provides OpenID Connect authentication for enterprise IAM.
// This package is only available in the enterprise edition of ZapFS.
package oidc

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

var (
	// ErrInvalidToken is returned when token validation fails
	ErrInvalidToken = errors.New("invalid token")
	// ErrUnauthorized is returned when user is not authorized
	ErrUnauthorized = errors.New("unauthorized")
)

// Config holds OIDC provider configuration
type Config struct {
	// Provider settings
	Issuer       string   // OIDC issuer URL (e.g., https://accounts.google.com)
	ClientID     string   // OAuth2 client ID
	ClientSecret string   // OAuth2 client secret (optional for public clients)
	RedirectURL  string   // Callback URL (e.g., http://localhost:8060/v1/oidc/callback)
	Scopes       []string // Scopes to request (default: openid, email, profile)

	// Claims mapping
	UsernameClaim string // Claim to use as username (default: email)
	GroupsClaim   string // Claim containing groups (optional)

	// Access control
	RequiredGroups []string // User must be in at least one of these groups
	AllowedDomains []string // Allowed email domains (e.g., ["example.com"])

	// Policy mapping
	GroupPolicyMap map[string][]string // OIDC group -> policy names
}

// Store implements OIDC authentication backed by a local credential store.
// OIDC is used for user authentication, while S3 credentials are stored locally.
type Store struct {
	config Config

	// OIDC provider and OAuth2 config
	provider    *oidc.Provider
	oauth2      oauth2.Config
	verifier    *oidc.IDTokenVerifier
	httpClient  *http.Client
	initialized bool

	// Local storage for S3 access keys
	localStore iam.CredentialStore

	// State management for OAuth2 flow
	statesMu sync.RWMutex
	states   map[string]stateEntry
}

type stateEntry struct {
	createdAt time.Time
	nonce     string
}

// NewStore creates a new OIDC-backed credential store
func NewStore(ctx context.Context, config Config, localStore iam.CredentialStore) (*Store, error) {
	if config.Issuer == "" {
		return nil, errors.New("OIDC issuer URL is required")
	}
	if config.ClientID == "" {
		return nil, errors.New("OIDC client ID is required")
	}
	if config.RedirectURL == "" {
		return nil, errors.New("OIDC redirect URL is required")
	}
	if len(config.Scopes) == 0 {
		config.Scopes = []string{oidc.ScopeOpenID, "email", "profile"}
	}
	if config.UsernameClaim == "" {
		config.UsernameClaim = "email"
	}

	store := &Store{
		config:     config,
		localStore: localStore,
		httpClient: http.DefaultClient,
		states:     make(map[string]stateEntry),
	}

	// Initialize provider (performs OIDC discovery)
	if err := store.initProvider(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize OIDC provider: %w", err)
	}

	// Start state cleanup goroutine
	go store.cleanupStates()

	return store, nil
}

func (s *Store) initProvider(ctx context.Context) error {
	provider, err := oidc.NewProvider(ctx, s.config.Issuer)
	if err != nil {
		return fmt.Errorf("failed to get OIDC provider: %w", err)
	}

	s.provider = provider
	s.oauth2 = oauth2.Config{
		ClientID:     s.config.ClientID,
		ClientSecret: s.config.ClientSecret,
		RedirectURL:  s.config.RedirectURL,
		Endpoint:     provider.Endpoint(),
		Scopes:       s.config.Scopes,
	}
	s.verifier = provider.Verifier(&oidc.Config{ClientID: s.config.ClientID})
	s.initialized = true

	return nil
}

// GetUserByAccessKey retrieves user by S3 access key from local store
func (s *Store) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam.Identity, *iam.Credential, error) {
	return s.localStore.GetUserByAccessKey(ctx, accessKey)
}

// CreateUser creates a user in the local store (must be authenticated via OIDC first)
func (s *Store) CreateUser(ctx context.Context, identity *iam.Identity) error {
	return s.localStore.CreateUser(ctx, identity)
}

// GetUser retrieves user from local store
func (s *Store) GetUser(ctx context.Context, username string) (*iam.Identity, error) {
	return s.localStore.GetUser(ctx, username)
}

// UpdateUser updates user in local store
func (s *Store) UpdateUser(ctx context.Context, identity *iam.Identity) error {
	return s.localStore.UpdateUser(ctx, identity)
}

// DeleteUser deletes user from local store
func (s *Store) DeleteUser(ctx context.Context, username string) error {
	return s.localStore.DeleteUser(ctx, username)
}

// ListUsers lists all users in local store
func (s *Store) ListUsers(ctx context.Context) ([]string, error) {
	return s.localStore.ListUsers(ctx)
}

// CreateAccessKey creates an access key for a user in local store
func (s *Store) CreateAccessKey(ctx context.Context, username string, cred *iam.Credential) error {
	return s.localStore.CreateAccessKey(ctx, username, cred)
}

// DeleteAccessKey deletes an access key from local store
func (s *Store) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	return s.localStore.DeleteAccessKey(ctx, username, accessKey)
}

// OIDC-specific methods

// GenerateAuthURL creates an OAuth2 authorization URL with state
func (s *Store) GenerateAuthURL() (string, string, error) {
	if !s.initialized {
		return "", "", errors.New("OIDC provider not initialized")
	}

	state, err := generateRandomString(32)
	if err != nil {
		return "", "", fmt.Errorf("failed to generate state: %w", err)
	}

	nonce, err := generateRandomString(32)
	if err != nil {
		return "", "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	s.statesMu.Lock()
	s.states[state] = stateEntry{
		createdAt: time.Now(),
		nonce:     nonce,
	}
	s.statesMu.Unlock()

	url := s.oauth2.AuthCodeURL(state,
		oauth2.SetAuthURLParam("nonce", nonce),
	)

	return url, state, nil
}

// HandleCallback processes the OAuth2 callback and returns user info
func (s *Store) HandleCallback(ctx context.Context, code, state string) (*OIDCUser, error) {
	if !s.initialized {
		return nil, errors.New("OIDC provider not initialized")
	}

	// Verify state
	s.statesMu.Lock()
	entry, ok := s.states[state]
	if ok {
		delete(s.states, state)
	}
	s.statesMu.Unlock()

	if !ok {
		return nil, errors.New("invalid state parameter")
	}

	// State expires after 10 minutes
	if time.Since(entry.createdAt) > 10*time.Minute {
		return nil, errors.New("state expired")
	}

	// Exchange code for tokens
	token, err := s.oauth2.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange code: %w", err)
	}

	// Extract ID token
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		return nil, errors.New("no id_token in response")
	}

	// Verify ID token
	idToken, err := s.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return nil, fmt.Errorf("failed to verify ID token: %w", err)
	}

	// Verify nonce
	var claims struct {
		Nonce string `json:"nonce"`
	}
	if err := idToken.Claims(&claims); err != nil {
		return nil, fmt.Errorf("failed to parse nonce: %w", err)
	}
	if claims.Nonce != entry.nonce {
		return nil, errors.New("nonce mismatch")
	}

	// Extract user info from token
	user, err := s.extractUserInfo(idToken)
	if err != nil {
		return nil, err
	}

	// Validate user against access controls
	if err := s.validateUser(user); err != nil {
		return nil, err
	}

	return user, nil
}

// OIDCUser represents a user authenticated via OIDC
type OIDCUser struct {
	Subject  string   // OIDC subject (unique identifier)
	Username string   // Extracted username (from configured claim)
	Email    string   // Email address
	Name     string   // Display name
	Groups   []string // Group memberships
	Policies []string // Mapped policies from groups
}

func (s *Store) extractUserInfo(idToken *oidc.IDToken) (*OIDCUser, error) {
	var claims map[string]interface{}
	if err := idToken.Claims(&claims); err != nil {
		return nil, fmt.Errorf("failed to extract claims: %w", err)
	}

	user := &OIDCUser{
		Subject: idToken.Subject,
	}

	// Extract username from configured claim
	if val, ok := claims[s.config.UsernameClaim]; ok {
		user.Username = fmt.Sprintf("%v", val)
	}
	if user.Username == "" {
		user.Username = idToken.Subject
	}

	// Extract email
	if email, ok := claims["email"].(string); ok {
		user.Email = email
	}

	// Extract name
	if name, ok := claims["name"].(string); ok {
		user.Name = name
	}

	// Extract groups from configured claim
	if s.config.GroupsClaim != "" {
		if groupsVal, ok := claims[s.config.GroupsClaim]; ok {
			switch g := groupsVal.(type) {
			case []interface{}:
				for _, v := range g {
					if str, ok := v.(string); ok {
						user.Groups = append(user.Groups, str)
					}
				}
			case []string:
				user.Groups = g
			case string:
				user.Groups = []string{g}
			}
		}
	}

	// Map groups to policies
	user.Policies = s.mapGroupsToPolicies(user.Groups)

	return user, nil
}

func (s *Store) validateUser(user *OIDCUser) error {
	// Check allowed domains
	if len(s.config.AllowedDomains) > 0 && user.Email != "" {
		parts := strings.Split(user.Email, "@")
		if len(parts) != 2 {
			return fmt.Errorf("invalid email format")
		}
		domain := strings.ToLower(parts[1])
		allowed := false
		for _, d := range s.config.AllowedDomains {
			if strings.ToLower(d) == domain {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("email domain not allowed: %s", domain)
		}
	}

	// Check required groups
	if len(s.config.RequiredGroups) > 0 {
		hasGroup := false
		for _, required := range s.config.RequiredGroups {
			for _, userGroup := range user.Groups {
				if strings.EqualFold(required, userGroup) {
					hasGroup = true
					break
				}
			}
			if hasGroup {
				break
			}
		}
		if !hasGroup {
			return fmt.Errorf("user not in required groups")
		}
	}

	return nil
}

func (s *Store) mapGroupsToPolicies(groups []string) []string {
	if s.config.GroupPolicyMap == nil {
		return nil
	}

	seen := make(map[string]bool)
	var policies []string

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

// CreateOrUpdateUser creates or updates a user from OIDC authentication
func (s *Store) CreateOrUpdateUser(ctx context.Context, oidcUser *OIDCUser) (*iam.Identity, error) {
	identity, err := s.localStore.GetUser(ctx, oidcUser.Username)
	if err != nil {
		// User doesn't exist, create new
		identity = &iam.Identity{
			Name: oidcUser.Username,
			Account: &iam.Account{
				ID:           generateAccountID(),
				DisplayName:  oidcUser.Name,
				EmailAddress: oidcUser.Email,
			},
			Disabled: false,
		}
		if err := s.localStore.CreateUser(ctx, identity); err != nil {
			return nil, fmt.Errorf("failed to create user: %w", err)
		}
		logger.Info().Str("username", oidcUser.Username).Msg("created new OIDC user")
	} else {
		// Update existing user
		if identity.Account == nil {
			identity.Account = &iam.Account{}
		}
		identity.Account.DisplayName = oidcUser.Name
		identity.Account.EmailAddress = oidcUser.Email
		if err := s.localStore.UpdateUser(ctx, identity); err != nil {
			return nil, fmt.Errorf("failed to update user: %w", err)
		}
	}

	return identity, nil
}

// GenerateAccessKey creates a new S3 access key for the user
func (s *Store) GenerateAccessKey(ctx context.Context, username string) (*iam.Credential, error) {
	accessKey, err := generateAccessKey()
	if err != nil {
		return nil, err
	}

	secretKey, err := generateSecretKey()
	if err != nil {
		return nil, err
	}

	cred := &iam.Credential{
		AccessKey: accessKey,
		SecretKey: secretKey,
		CreatedAt: time.Now(),
		Status:    "Active",
	}

	if err := s.localStore.CreateAccessKey(ctx, username, cred); err != nil {
		return nil, err
	}

	return cred, nil
}

// cleanupStates removes expired state entries
func (s *Store) cleanupStates() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		s.statesMu.Lock()
		now := time.Now()
		for state, entry := range s.states {
			if now.Sub(entry.createdAt) > 15*time.Minute {
				delete(s.states, state)
			}
		}
		s.statesMu.Unlock()
	}
}

// Close cleans up resources
func (s *Store) Close() error {
	s.statesMu.Lock()
	s.states = nil
	s.statesMu.Unlock()
	return nil
}

// Helper functions

func generateRandomString(length int) (string, error) {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b)[:length], nil
}

func generateAccessKey() (string, error) {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "AKIA" + strings.ToUpper(hex.EncodeToString(b))[:16], nil
}

func generateSecretKey() (string, error) {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func generateAccountID() string {
	b := make([]byte, 6)
	rand.Read(b)
	return fmt.Sprintf("%012d", (int64(b[0])<<40)|(int64(b[1])<<32)|(int64(b[2])<<24)|(int64(b[3])<<16)|(int64(b[4])<<8)|int64(b[5])%1000000000000)
}

// HTTPHandler returns HTTP handlers for OIDC endpoints
type HTTPHandler struct {
	store *Store
}

// NewHTTPHandler creates a new OIDC HTTP handler
func NewHTTPHandler(store *Store) *HTTPHandler {
	return &HTTPHandler{store: store}
}

// ServeHTTP implements http.Handler
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/oidc")

	switch path {
	case "/login":
		h.handleLogin(w, r)
	case "/callback":
		h.handleCallback(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *HTTPHandler) handleLogin(w http.ResponseWriter, r *http.Request) {
	authURL, _, err := h.store.GenerateAuthURL()
	if err != nil {
		http.Error(w, "Failed to generate auth URL: "+err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, authURL, http.StatusFound)
}

func (h *HTTPHandler) handleCallback(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Check for errors from provider
	if errParam := r.URL.Query().Get("error"); errParam != "" {
		errDesc := r.URL.Query().Get("error_description")
		logger.Warn().Str("error", errParam).Str("description", errDesc).Msg("OIDC callback error")
		http.Error(w, "Authentication failed: "+errDesc, http.StatusUnauthorized)
		return
	}

	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")

	if code == "" || state == "" {
		http.Error(w, "Missing code or state", http.StatusBadRequest)
		return
	}

	// Process callback
	oidcUser, err := h.store.HandleCallback(ctx, code, state)
	if err != nil {
		logger.Warn().Err(err).Msg("OIDC callback failed")
		http.Error(w, "Authentication failed: "+err.Error(), http.StatusUnauthorized)
		return
	}

	// Create or update user
	identity, err := h.store.CreateOrUpdateUser(ctx, oidcUser)
	if err != nil {
		logger.Error().Err(err).Str("username", oidcUser.Username).Msg("failed to create/update OIDC user")
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	// Generate new access key
	cred, err := h.store.GenerateAccessKey(ctx, oidcUser.Username)
	if err != nil {
		logger.Error().Err(err).Str("username", oidcUser.Username).Msg("failed to generate access key")
		http.Error(w, "Failed to generate credentials", http.StatusInternalServerError)
		return
	}

	// Return credentials
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"username":   identity.Name,
		"email":      oidcUser.Email,
		"access_key": cred.AccessKey,
		"secret_key": cred.SecretKey,
		"policies":   oidcUser.Policies,
		"message":    "Save your secret key - it will not be shown again!",
	})

	logger.Info().
		Str("username", identity.Name).
		Str("access_key", cred.AccessKey).
		Strs("policies", oidcUser.Policies).
		Msg("OIDC user authenticated, credentials issued")
}
