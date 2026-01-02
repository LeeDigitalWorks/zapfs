// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// FileStore stores IAM credentials in JSON files on disk
// Layout:
//
//	<basePath>/
//	  users/
//	    alice.json
//	    bob.json
//	  indexes/
//	    access_keys.json  # map[accessKey]username for fast lookup
type FileStore struct {
	basePath string
	mu       sync.RWMutex

	// In-memory indexes for performance
	accessKeyIndex map[string]string // accessKey -> username
}

type userFile struct {
	Identity *Identity `json:"identity"`
}

type accessKeyIndexFile struct {
	Index map[string]string `json:"access_key_index"`
}

// NewFileStore creates a new file-based credential store
func NewFileStore(basePath string) (*FileStore, error) {
	store := &FileStore{
		basePath:       basePath,
		accessKeyIndex: make(map[string]string),
	}

	// Create directory structure
	if err := os.MkdirAll(filepath.Join(basePath, "users"), 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(basePath, "indexes"), 0700); err != nil {
		return nil, err
	}

	// Load access key index
	if err := store.loadAccessKeyIndex(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return store, nil
}

func (s *FileStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*Identity, *Credential, error) {
	s.mu.RLock()
	username, exists := s.accessKeyIndex[accessKey]
	s.mu.RUnlock()

	if !exists {
		return nil, nil, ErrAccessKeyNotFound
	}

	identity, err := s.GetUser(ctx, username)
	if err != nil {
		return nil, nil, err
	}

	// Find the specific credential
	for _, cred := range identity.Credentials {
		if cred.AccessKey == accessKey {
			return identity, cred, nil
		}
	}

	return nil, nil, ErrAccessKeyNotFound
}

func (s *FileStore) CreateUser(ctx context.Context, identity *Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	userPath := s.userFilePath(identity.Name)

	// Check if user already exists
	if _, err := os.Stat(userPath); err == nil {
		return ErrUserAlreadyExists
	}

	// Write user file
	if err := s.writeUserFile(identity); err != nil {
		return err
	}

	// Update access key index
	for _, cred := range identity.Credentials {
		s.accessKeyIndex[cred.AccessKey] = identity.Name
	}

	// Persist index
	return s.saveAccessKeyIndex()
}

func (s *FileStore) GetUser(ctx context.Context, username string) (*Identity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.readUserFile(username)
}

func (s *FileStore) UpdateUser(ctx context.Context, identity *Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Read old identity to clean up access keys
	oldIdentity, err := s.readUserFile(identity.Name)
	if err != nil {
		return ErrUserNotFound
	}

	// Remove old access keys from index
	for _, cred := range oldIdentity.Credentials {
		delete(s.accessKeyIndex, cred.AccessKey)
	}

	// Write updated user
	if err := s.writeUserFile(identity); err != nil {
		return err
	}

	// Update access key index
	for _, cred := range identity.Credentials {
		s.accessKeyIndex[cred.AccessKey] = identity.Name
	}

	return s.saveAccessKeyIndex()
}

func (s *FileStore) DeleteUser(ctx context.Context, username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	identity, err := s.readUserFile(username)
	if err != nil {
		return ErrUserNotFound
	}

	// Remove access keys from index
	for _, cred := range identity.Credentials {
		delete(s.accessKeyIndex, cred.AccessKey)
	}

	// Delete user file
	userPath := s.userFilePath(username)
	if err := os.Remove(userPath); err != nil {
		return err
	}

	return s.saveAccessKeyIndex()
}

func (s *FileStore) ListUsers(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	usersDir := filepath.Join(s.basePath, "users")
	entries, err := os.ReadDir(usersDir)
	if err != nil {
		return nil, err
	}

	usernames := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			username := entry.Name()[:len(entry.Name())-5] // Remove .json
			usernames = append(usernames, username)
		}
	}

	return usernames, nil
}

func (s *FileStore) CreateAccessKey(ctx context.Context, username string, cred *Credential) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	identity, err := s.readUserFile(username)
	if err != nil {
		return ErrUserNotFound
	}

	// Check if access key already exists
	if _, exists := s.accessKeyIndex[cred.AccessKey]; exists {
		return ErrUserAlreadyExists
	}

	identity.Credentials = append(identity.Credentials, cred)
	s.accessKeyIndex[cred.AccessKey] = username

	if err := s.writeUserFile(identity); err != nil {
		return err
	}

	return s.saveAccessKeyIndex()
}

func (s *FileStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	identity, err := s.readUserFile(username)
	if err != nil {
		return ErrUserNotFound
	}

	// Find and remove the credential
	newCreds := make([]*Credential, 0, len(identity.Credentials))
	found := false
	for _, cred := range identity.Credentials {
		if cred.AccessKey == accessKey {
			found = true
			delete(s.accessKeyIndex, accessKey)
		} else {
			newCreds = append(newCreds, cred)
		}
	}

	if !found {
		return ErrAccessKeyNotFound
	}

	identity.Credentials = newCreds

	if err := s.writeUserFile(identity); err != nil {
		return err
	}

	return s.saveAccessKeyIndex()
}

// Helper methods

func (s *FileStore) userFilePath(username string) string {
	return filepath.Join(s.basePath, "users", username+".json")
}

func (s *FileStore) accessKeyIndexPath() string {
	return filepath.Join(s.basePath, "indexes", "access_keys.json")
}

func (s *FileStore) readUserFile(username string) (*Identity, error) {
	data, err := os.ReadFile(s.userFilePath(username))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}

	var uf userFile
	if err := json.Unmarshal(data, &uf); err != nil {
		return nil, err
	}

	return uf.Identity, nil
}

func (s *FileStore) writeUserFile(identity *Identity) error {
	uf := userFile{Identity: identity}
	data, err := json.MarshalIndent(uf, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(s.userFilePath(identity.Name), data, 0600)
}

func (s *FileStore) loadAccessKeyIndex() error {
	data, err := os.ReadFile(s.accessKeyIndexPath())
	if err != nil {
		return err
	}

	var indexFile accessKeyIndexFile
	if err := json.Unmarshal(data, &indexFile); err != nil {
		return err
	}

	s.accessKeyIndex = indexFile.Index
	return nil
}

func (s *FileStore) saveAccessKeyIndex() error {
	indexFile := accessKeyIndexFile{Index: s.accessKeyIndex}
	data, err := json.MarshalIndent(indexFile, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(s.accessKeyIndexPath(), data, 0600)
}
