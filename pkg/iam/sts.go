package iam

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

// STS errors
var (
	ErrSessionExpired   = errors.New("session has expired")
	ErrSessionNotFound  = errors.New("session not found")
	ErrInvalidToken     = errors.New("invalid session token")
	ErrAssumeRoleDenied = errors.New("assume role denied")
)

// SessionCredential represents temporary credentials from STS
type SessionCredential struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time

	// The assumed role or federated user
	AssumedRoleARN  string
	SourceIdentity  string // Original identity that assumed the role
	RoleSessionName string
}

// IsExpired checks if the session has expired
func (s *SessionCredential) IsExpired() bool {
	return time.Now().After(s.Expiration)
}

// STSConfig holds configuration for the STS service
type STSConfig struct {
	// DefaultSessionDuration is the default duration for session credentials
	DefaultSessionDuration time.Duration
	// MaxSessionDuration is the maximum allowed session duration
	MaxSessionDuration time.Duration
	// MinSessionDuration is the minimum allowed session duration
	MinSessionDuration time.Duration
}

// DefaultSTSConfig returns default STS configuration
func DefaultSTSConfig() STSConfig {
	return STSConfig{
		DefaultSessionDuration: 1 * time.Hour,
		MaxSessionDuration:     12 * time.Hour,
		MinSessionDuration:     15 * time.Minute,
	}
}

// STSService provides Security Token Service functionality
// This is a simplified implementation for local development
type STSService struct {
	config   STSConfig
	iamMgr   *Manager
	sessions *utils.ShardedMap[*SessionCredential] // sessionToken -> *SessionCredential
}

// NewSTSService creates a new STS service
func NewSTSService(iamMgr *Manager, config STSConfig) *STSService {
	sts := &STSService{
		config:   config,
		iamMgr:   iamMgr,
		sessions: utils.NewShardedMap[*SessionCredential](),
	}

	// Start cleanup goroutine
	go sts.cleanupExpiredSessions()

	return sts
}

// AssumeRoleInput contains parameters for AssumeRole
type AssumeRoleInput struct {
	RoleARN         string
	RoleSessionName string
	DurationSeconds int64
	ExternalID      string
	Policy          string // Optional inline policy
	PolicyARNs      []string
}

// AssumeRole returns temporary credentials for assuming a role
// Note: This is a simplified implementation - in production, this would
// validate the role exists, check trust policies, and apply permission boundaries
func (s *STSService) AssumeRole(ctx context.Context, callerIdentity *Identity, input AssumeRoleInput) (*SessionCredential, error) {
	// Calculate duration
	duration := s.config.DefaultSessionDuration
	if input.DurationSeconds > 0 {
		duration = time.Duration(input.DurationSeconds) * time.Second
		if duration < s.config.MinSessionDuration {
			duration = s.config.MinSessionDuration
		}
		if duration > s.config.MaxSessionDuration {
			duration = s.config.MaxSessionDuration
		}
	}

	// Generate temporary credentials
	cred := &SessionCredential{
		AccessKeyID:     generateSessionAccessKey(),
		SecretAccessKey: GenerateSecretKey(),
		SessionToken:    generateSessionToken(),
		Expiration:      time.Now().Add(duration),
		AssumedRoleARN:  input.RoleARN,
		SourceIdentity:  callerIdentity.Name,
		RoleSessionName: input.RoleSessionName,
	}

	// Store session
	s.sessions.Store(cred.SessionToken, cred)

	return cred, nil
}

// GetSessionCredentialsInput contains parameters for GetSessionToken
type GetSessionCredentialsInput struct {
	DurationSeconds int64
	SerialNumber    string // For MFA
	TokenCode       string // For MFA
}

// GetSessionToken returns temporary credentials for the calling user
func (s *STSService) GetSessionToken(ctx context.Context, callerIdentity *Identity, input GetSessionCredentialsInput) (*SessionCredential, error) {
	// Calculate duration
	duration := s.config.DefaultSessionDuration
	if input.DurationSeconds > 0 {
		duration = time.Duration(input.DurationSeconds) * time.Second
		if duration < s.config.MinSessionDuration {
			duration = s.config.MinSessionDuration
		}
		if duration > s.config.MaxSessionDuration {
			duration = s.config.MaxSessionDuration
		}
	}

	// Generate temporary credentials
	cred := &SessionCredential{
		AccessKeyID:     generateSessionAccessKey(),
		SecretAccessKey: GenerateSecretKey(),
		SessionToken:    generateSessionToken(),
		Expiration:      time.Now().Add(duration),
		SourceIdentity:  callerIdentity.Name,
	}

	// Store session
	s.sessions.Store(cred.SessionToken, cred)

	return cred, nil
}

// ValidateSessionToken validates a session token and returns the session credentials
func (s *STSService) ValidateSessionToken(ctx context.Context, sessionToken string) (*SessionCredential, error) {
	cred, exists := s.sessions.Load(sessionToken)
	if !exists {
		return nil, ErrSessionNotFound
	}

	if cred.IsExpired() {
		s.sessions.Delete(sessionToken)
		return nil, ErrSessionExpired
	}

	return cred, nil
}

// RevokeSession revokes a session token
func (s *STSService) RevokeSession(sessionToken string) {
	s.sessions.Delete(sessionToken)
}

// cleanupExpiredSessions periodically removes expired sessions
func (s *STSService) cleanupExpiredSessions() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		s.sessions.DeleteIf(func(_ string, cred *SessionCredential) bool {
			return cred.IsExpired()
		})
	}
}

// generateSessionAccessKey generates a temporary access key ID
func generateSessionAccessKey() string {
	b := make([]byte, 10)
	rand.Read(b)
	return "ASIA" + hex.EncodeToString(b)[:16] // ASIA prefix for session credentials
}

// generateSessionToken generates a session token
func generateSessionToken() string {
	b := make([]byte, 128)
	rand.Read(b)
	return hex.EncodeToString(b)
}
