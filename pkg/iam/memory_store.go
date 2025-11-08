package iam

import (
	"context"
	"sync"
)

// MemoryStore is an in-memory implementation of CredentialStore
type MemoryStore struct {
	mu         sync.RWMutex
	users      map[string]*Identity // username -> Identity
	accessKeys map[string]string    // accessKey -> username
}

// NewMemoryStore creates a new in-memory credential store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		users:      make(map[string]*Identity),
		accessKeys: make(map[string]string),
	}
}

func (s *MemoryStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*Identity, *Credential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	username, exists := s.accessKeys[accessKey]
	if !exists {
		return nil, nil, ErrAccessKeyNotFound
	}

	identity, exists := s.users[username]
	if !exists {
		return nil, nil, ErrUserNotFound
	}

	// Find the specific credential
	for _, cred := range identity.Credentials {
		if cred.AccessKey == accessKey {
			return identity, cred, nil
		}
	}

	return nil, nil, ErrAccessKeyNotFound
}

func (s *MemoryStore) CreateUser(ctx context.Context, identity *Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[identity.Name]; exists {
		return ErrUserAlreadyExists
	}

	s.users[identity.Name] = identity

	// Index all access keys
	for _, cred := range identity.Credentials {
		s.accessKeys[cred.AccessKey] = identity.Name
	}

	return nil
}

func (s *MemoryStore) GetUser(ctx context.Context, username string) (*Identity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	identity, exists := s.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}

	return identity, nil
}

func (s *MemoryStore) UpdateUser(ctx context.Context, identity *Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[identity.Name]; !exists {
		return ErrUserNotFound
	}

	// Remove old access keys from index
	if oldIdentity, exists := s.users[identity.Name]; exists {
		for _, cred := range oldIdentity.Credentials {
			delete(s.accessKeys, cred.AccessKey)
		}
	}

	s.users[identity.Name] = identity

	// Re-index new access keys
	for _, cred := range identity.Credentials {
		s.accessKeys[cred.AccessKey] = identity.Name
	}

	return nil
}

func (s *MemoryStore) DeleteUser(ctx context.Context, username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	identity, exists := s.users[username]
	if !exists {
		return ErrUserNotFound
	}

	// Remove access keys from index
	for _, cred := range identity.Credentials {
		delete(s.accessKeys, cred.AccessKey)
	}

	delete(s.users, username)
	return nil
}

func (s *MemoryStore) ListUsers(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	usernames := make([]string, 0, len(s.users))
	for username := range s.users {
		usernames = append(usernames, username)
	}

	return usernames, nil
}

func (s *MemoryStore) CreateAccessKey(ctx context.Context, username string, cred *Credential) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	identity, exists := s.users[username]
	if !exists {
		return ErrUserNotFound
	}

	// Check if access key already exists
	if _, exists := s.accessKeys[cred.AccessKey]; exists {
		return ErrUserAlreadyExists
	}

	identity.Credentials = append(identity.Credentials, cred)
	s.accessKeys[cred.AccessKey] = username

	return nil
}

func (s *MemoryStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	identity, exists := s.users[username]
	if !exists {
		return ErrUserNotFound
	}

	// Find and remove the credential
	newCreds := make([]*Credential, 0, len(identity.Credentials))
	found := false
	for _, cred := range identity.Credentials {
		if cred.AccessKey == accessKey {
			found = true
			delete(s.accessKeys, accessKey)
		} else {
			newCreds = append(newCreds, cred)
		}
	}

	if !found {
		return ErrAccessKeyNotFound
	}

	identity.Credentials = newCreds
	return nil
}
