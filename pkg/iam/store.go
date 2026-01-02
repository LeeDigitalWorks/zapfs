// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"errors"
)

var (
	ErrUserNotFound      = errors.New("user not found")
	ErrUserAlreadyExists = errors.New("user already exists")
	ErrAccessKeyNotFound = errors.New("access key not found")
)

// CredentialStore defines the interface for credential storage and retrieval
type CredentialStore interface {
	// GetUserByAccessKey retrieves a user identity by access key
	GetUserByAccessKey(ctx context.Context, accessKey string) (*Identity, *Credential, error)

	// CreateUser creates a new user with identity
	CreateUser(ctx context.Context, identity *Identity) error

	// GetUser retrieves a user by name
	GetUser(ctx context.Context, username string) (*Identity, error)

	// UpdateUser updates an existing user
	UpdateUser(ctx context.Context, identity *Identity) error

	// DeleteUser removes a user
	DeleteUser(ctx context.Context, username string) error

	// ListUsers returns all usernames
	ListUsers(ctx context.Context) ([]string, error)

	// CreateAccessKey creates a new access key for a user
	CreateAccessKey(ctx context.Context, username string, cred *Credential) error

	// DeleteAccessKey removes an access key
	DeleteAccessKey(ctx context.Context, username string, accessKey string) error
}
