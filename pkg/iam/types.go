package iam

import (
	"time"
)

// Credential represents an access key and secret key pair for S3 authentication
type Credential struct {
	AccessKey   string     `json:"access_key"`
	SecretKey   string     `json:"secret_key,omitempty"` // Plaintext (only returned on creation)
	Status      string     `json:"status"`               // "Active" or "Inactive"
	CreatedAt   time.Time  `json:"created_at"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"` // Optional expiration
	Description string     `json:"description,omitempty"`

	// For secure storage (never store SecretKey in plaintext)
	EncryptedSecret []byte `json:"encrypted_secret,omitempty"` // AES-GCM encrypted secret
	SigningKey      []byte `json:"-"`                          // Derived HMAC key for signature verification (transient)
}

// IsActive returns true if the credential is active and not expired
func (c *Credential) IsActive() bool {
	if c.Status != "" && c.Status != "Active" {
		return false
	}
	if c.ExpiresAt != nil && time.Now().After(*c.ExpiresAt) {
		return false
	}
	return true
}

// Account represents an S3 account with billing/ownership identity
type Account struct {
	DisplayName  string `json:"display_name"`
	EmailAddress string `json:"email_address"`
	ID           string `json:"id"` // Canonical user ID (used in ACLs)
}

// Identity represents an S3 user/principal with credentials
// This is for S3 API authentication only - admin access is handled separately via gRPC
type Identity struct {
	Name        string        `json:"name"`
	Account     *Account      `json:"account,omitempty"`
	Credentials []*Credential `json:"credentials,omitempty"`
	Disabled    bool          `json:"disabled"`
}

// Predefined accounts
var (
	AccountAnonymous = &Account{
		DisplayName:  "anonymous",
		EmailAddress: "",
		ID:           "anonymous",
	}
)
