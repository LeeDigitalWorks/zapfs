// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/google/btree"
	"google.golang.org/protobuf/proto"
)

// FSMState holds all state that is replicated through Raft consensus.
// This struct isolates replicated state from server concerns, making
// snapshots cleaner and state management more explicit.
//
// All fields in this struct are persisted in Raft snapshots and must
// be consistent across all nodes in the cluster.
type FSMState struct {
	mu sync.RWMutex

	// Service registry - tracks registered file and metadata services
	FileServices     map[string]*ServiceRegistration
	MetadataServices map[string]*ServiceRegistration
	TopologyVersion  uint64

	// Collections (storage namespaces / buckets)
	Collections        map[string]*manager_pb.Collection
	CollectionsVersion uint64

	// Derived indexes - rebuilt from Collections on restore
	// These are not persisted in snapshots but reconstructed
	CollectionsByOwner   map[string][]string
	OwnerCollectionCount map[string]int
	CollectionsByTier    map[string][]string
	CollectionsByTime    *btree.BTree

	// Configuration replicated via Raft
	PlacementPolicy *manager_pb.PlacementPolicy
	RegionID        string

	// IAM state - users, groups, policies replicated via Raft
	// Credentials are stored with encrypted secrets (EncryptedSecret field)
	IAMUsers    map[string]*iam.Identity // name -> identity
	IAMPolicies map[string]*iam.Policy   // name -> policy
	IAMVersion  uint64                   // Monotonic version counter for IAM changes

	// Derived IAM indexes - rebuilt on restore
	IAMCredentialIndex map[string]string // accessKey -> userName (for fast lookup)
}

// collectionTimeItem implements btree.Item for time-ordered collection indexing.
// This is the btree item type used in CollectionsByTime.
type collectionTimeItem struct {
	createdAt  time.Time
	name       string // for uniqueness when timestamps collide
	collection *manager_pb.Collection
}

// Less implements btree.Item interface.
// Orders by created_at first, then by name for deterministic ordering.
func (a *collectionTimeItem) Less(b btree.Item) bool {
	other := b.(*collectionTimeItem)
	if a.createdAt.Equal(other.createdAt) {
		return a.name < other.name
	}
	return a.createdAt.Before(other.createdAt)
}

// NewFSMState creates a new FSMState with initialized maps.
func NewFSMState(regionID string, defaultNumReplicas uint32) *FSMState {
	if defaultNumReplicas == 0 {
		defaultNumReplicas = 3
	}

	return &FSMState{
		RegionID:             regionID,
		FileServices:         make(map[string]*ServiceRegistration),
		MetadataServices:     make(map[string]*ServiceRegistration),
		Collections:          make(map[string]*manager_pb.Collection),
		CollectionsVersion:   1,
		TopologyVersion:      1,
		CollectionsByOwner:   make(map[string][]string),
		OwnerCollectionCount: make(map[string]int),
		CollectionsByTier:    make(map[string][]string),
		CollectionsByTime:    btree.New(2),
		PlacementPolicy: &manager_pb.PlacementPolicy{
			NumReplicas: defaultNumReplicas,
		},
		// IAM state
		IAMUsers:           make(map[string]*iam.Identity),
		IAMPolicies:        make(map[string]*iam.Policy),
		IAMVersion:         1,
		IAMCredentialIndex: make(map[string]string),
	}
}

// Lock acquires the write lock on the state.
func (s *FSMState) Lock() {
	s.mu.Lock()
}

// Unlock releases the write lock on the state.
func (s *FSMState) Unlock() {
	s.mu.Unlock()
}

// RLock acquires the read lock on the state.
func (s *FSMState) RLock() {
	s.mu.RLock()
}

// RUnlock releases the read lock on the state.
func (s *FSMState) RUnlock() {
	s.mu.RUnlock()
}

// GetRegistry returns the appropriate service registry based on service type.
func (s *FSMState) GetRegistry(serviceType manager_pb.ServiceType) map[string]*ServiceRegistration {
	if serviceType == manager_pb.ServiceType_FILE_SERVICE {
		return s.FileServices
	}
	return s.MetadataServices
}

// Snapshot creates a point-in-time snapshot of the Raft state.
// This is called by the FSM Snapshot method.
// Must be called with at least a read lock held.
func (s *FSMState) Snapshot() *fsmSnapshot {
	snapshot := &fsmSnapshot{
		FileServices:       make(map[string]*ServiceRegistration),
		MetadataServices:   make(map[string]*ServiceRegistration),
		Collections:        make(map[string]*manager_pb.Collection),
		TopologyVersion:    s.TopologyVersion,
		CollectionsVersion: s.CollectionsVersion,
		PlacementPolicy:    s.PlacementPolicy,
		RegionID:           s.RegionID,
		// IAM state
		IAMUsers:    make(map[string]*iam.Identity),
		IAMPolicies: make(map[string]*iam.Policy),
		IAMVersion:  s.IAMVersion,
	}

	// Deep copy file services
	for k, v := range s.FileServices {
		regCopy := *v
		snapshot.FileServices[k] = &regCopy
	}

	// Deep copy metadata services
	for k, v := range s.MetadataServices {
		regCopy := *v
		snapshot.MetadataServices[k] = &regCopy
	}

	// Deep copy collections
	for k, v := range s.Collections {
		colCopy := proto.Clone(v).(*manager_pb.Collection)
		snapshot.Collections[k] = colCopy
	}

	// Deep copy IAM users
	for k, v := range s.IAMUsers {
		snapshot.IAMUsers[k] = copyIdentity(v)
	}

	// Deep copy IAM policies
	for k, v := range s.IAMPolicies {
		snapshot.IAMPolicies[k] = copyPolicy(v)
	}

	return snapshot
}

// copyIdentity creates a deep copy of an Identity.
func copyIdentity(id *iam.Identity) *iam.Identity {
	if id == nil {
		return nil
	}
	copy := &iam.Identity{
		Name:     id.Name,
		Disabled: id.Disabled,
	}
	if id.Account != nil {
		copy.Account = &iam.Account{
			DisplayName:  id.Account.DisplayName,
			EmailAddress: id.Account.EmailAddress,
			ID:           id.Account.ID,
		}
	}
	for _, cred := range id.Credentials {
		credCopy := &iam.Credential{
			AccessKey:       cred.AccessKey,
			Status:          cred.Status,
			CreatedAt:       cred.CreatedAt,
			Description:     cred.Description,
			EncryptedSecret: append([]byte(nil), cred.EncryptedSecret...),
		}
		if cred.ExpiresAt != nil {
			exp := *cred.ExpiresAt
			credCopy.ExpiresAt = &exp
		}
		copy.Credentials = append(copy.Credentials, credCopy)
	}
	return copy
}

// copyPolicy creates a deep copy of a Policy.
func copyPolicy(p *iam.Policy) *iam.Policy {
	if p == nil {
		return nil
	}
	policyCopy := &iam.Policy{
		Version: p.Version,
		ID:      p.ID,
	}
	for _, stmt := range p.Statements {
		stmtCopy := iam.PolicyStatement{
			Sid:          stmt.Sid,
			Effect:       stmt.Effect,
			Actions:      copyStringOrSlice(stmt.Actions),
			NotActions:   copyStringOrSlice(stmt.NotActions),
			Resources:    copyStringOrSlice(stmt.Resources),
			NotResources: copyStringOrSlice(stmt.NotResources),
		}
		if stmt.Condition != nil {
			stmtCopy.Condition = make(map[string]iam.Condition)
			for k, v := range stmt.Condition {
				stmtCopy.Condition[k] = copyCondition(v)
			}
		}
		policyCopy.Statements = append(policyCopy.Statements, stmtCopy)
	}
	return policyCopy
}

// copyStringOrSlice creates a deep copy of a StringOrSlice.
func copyStringOrSlice(s iam.StringOrSlice) iam.StringOrSlice {
	if s == nil {
		return nil
	}
	return append(iam.StringOrSlice(nil), s...)
}

// copyCondition creates a deep copy of a Condition.
func copyCondition(c iam.Condition) iam.Condition {
	if c == nil {
		return nil
	}
	condCopy := make(iam.Condition)
	for k, v := range c {
		condCopy[k] = copyStringOrSlice(v)
	}
	return condCopy
}

// Restore restores state from a snapshot.
// Must be called with the write lock held.
func (s *FSMState) Restore(snapshot *fsmSnapshot) {
	s.FileServices = snapshot.FileServices
	s.MetadataServices = snapshot.MetadataServices
	s.Collections = snapshot.Collections
	s.TopologyVersion = snapshot.TopologyVersion
	s.CollectionsVersion = snapshot.CollectionsVersion
	s.PlacementPolicy = snapshot.PlacementPolicy
	s.RegionID = snapshot.RegionID

	// Restore IAM state
	s.IAMUsers = snapshot.IAMUsers
	if s.IAMUsers == nil {
		s.IAMUsers = make(map[string]*iam.Identity)
	}
	s.IAMPolicies = snapshot.IAMPolicies
	if s.IAMPolicies == nil {
		s.IAMPolicies = make(map[string]*iam.Policy)
	}
	s.IAMVersion = snapshot.IAMVersion

	// Rebuild derived indexes from restored collections and IAM
	s.rebuildIndexes()
	s.rebuildIAMIndexes()
}

// rebuildIndexes rebuilds all derived indexes from the collections map.
// Must be called with the write lock held.
func (s *FSMState) rebuildIndexes() {
	s.CollectionsByOwner = make(map[string][]string)
	s.OwnerCollectionCount = make(map[string]int)
	s.CollectionsByTier = make(map[string][]string)
	s.CollectionsByTime = btree.New(2)

	for _, col := range s.Collections {
		s.AddCollectionToIndexes(col)
	}
}

// AddCollectionToIndexes adds a collection to the derived indexes.
// Must be called with the write lock held.
func (s *FSMState) AddCollectionToIndexes(col *manager_pb.Collection) {
	// Owner index (sorted)
	ownerColls := s.CollectionsByOwner[col.Owner]
	ownerColls = append(ownerColls, col.Name)
	insertSorted(&ownerColls, col.Name)
	s.CollectionsByOwner[col.Owner] = ownerColls

	// Increment owner count
	s.OwnerCollectionCount[col.Owner]++

	// Tier index
	tierColls := s.CollectionsByTier[col.Tier]
	tierColls = append(tierColls, col.Name)
	s.CollectionsByTier[col.Tier] = tierColls

	// Time index
	if col.CreatedAt != nil {
		s.CollectionsByTime.ReplaceOrInsert(&collectionTimeItem{
			createdAt:  col.CreatedAt.AsTime(),
			name:       col.Name,
			collection: col,
		})
	}
}

// RemoveCollectionFromIndexes removes a collection from the derived indexes.
// Must be called with the write lock held.
func (s *FSMState) RemoveCollectionFromIndexes(col *manager_pb.Collection) {
	// Remove from owner index
	if ownerColls, ok := s.CollectionsByOwner[col.Owner]; ok {
		s.CollectionsByOwner[col.Owner] = removeFromSlice(ownerColls, col.Name)
		if len(s.CollectionsByOwner[col.Owner]) == 0 {
			delete(s.CollectionsByOwner, col.Owner)
		}
	}

	// Decrement owner count
	s.OwnerCollectionCount[col.Owner]--
	if s.OwnerCollectionCount[col.Owner] == 0 {
		delete(s.OwnerCollectionCount, col.Owner)
	}

	// Remove from tier index
	if tierColls, ok := s.CollectionsByTier[col.Tier]; ok {
		s.CollectionsByTier[col.Tier] = removeFromSlice(tierColls, col.Name)
		if len(s.CollectionsByTier[col.Tier]) == 0 {
			delete(s.CollectionsByTier, col.Tier)
		}
	}

	// Remove from time index
	if col.CreatedAt != nil {
		s.CollectionsByTime.Delete(&collectionTimeItem{
			createdAt: col.CreatedAt.AsTime(),
			name:      col.Name,
		})
	}
}

// GetCollectionsByOwner returns collection names for a given owner.
// Must be called with at least a read lock held.
func (s *FSMState) GetCollectionsByOwner(owner string) []string {
	return s.CollectionsByOwner[owner]
}

// GetOwnerCollectionCount returns the number of collections for a given owner.
// Must be called with at least a read lock held.
func (s *FSMState) GetOwnerCollectionCount(owner string) int {
	return s.OwnerCollectionCount[owner]
}

// GetCollectionsByTier returns collection names for a given tier.
// Must be called with at least a read lock held.
func (s *FSMState) GetCollectionsByTier(tier string) []string {
	return s.CollectionsByTier[tier]
}

// GetCollectionsByTime returns the btree for time-based lookups.
// Must be called with at least a read lock held.
func (s *FSMState) GetCollectionsByTime() *btree.BTree {
	return s.CollectionsByTime
}

// ============================================================================
// IAM State Methods
// ============================================================================

// rebuildIAMIndexes rebuilds all derived IAM indexes from the users map.
// Must be called with the write lock held.
func (s *FSMState) rebuildIAMIndexes() {
	s.IAMCredentialIndex = make(map[string]string)

	for userName, identity := range s.IAMUsers {
		for _, cred := range identity.Credentials {
			s.IAMCredentialIndex[cred.AccessKey] = userName
		}
	}
}

// AddIAMUser adds a user to the IAM state.
// Must be called with the write lock held.
func (s *FSMState) AddIAMUser(identity *iam.Identity) {
	s.IAMUsers[identity.Name] = identity
	s.IAMVersion++

	// Update credential index
	for _, cred := range identity.Credentials {
		s.IAMCredentialIndex[cred.AccessKey] = identity.Name
	}
}

// UpdateIAMUser updates an existing user in the IAM state.
// Must be called with the write lock held.
func (s *FSMState) UpdateIAMUser(identity *iam.Identity) {
	// Remove old credential index entries if user exists
	if old, exists := s.IAMUsers[identity.Name]; exists {
		for _, cred := range old.Credentials {
			delete(s.IAMCredentialIndex, cred.AccessKey)
		}
	}

	// Add updated user
	s.IAMUsers[identity.Name] = identity
	s.IAMVersion++

	// Update credential index
	for _, cred := range identity.Credentials {
		s.IAMCredentialIndex[cred.AccessKey] = identity.Name
	}
}

// RemoveIAMUser removes a user from the IAM state.
// Must be called with the write lock held.
func (s *FSMState) RemoveIAMUser(userName string) {
	if identity, exists := s.IAMUsers[userName]; exists {
		// Remove credential index entries
		for _, cred := range identity.Credentials {
			delete(s.IAMCredentialIndex, cred.AccessKey)
		}
		delete(s.IAMUsers, userName)
		s.IAMVersion++
	}
}

// GetIAMUser returns a user by name.
// Must be called with at least a read lock held.
func (s *FSMState) GetIAMUser(name string) (*iam.Identity, bool) {
	identity, exists := s.IAMUsers[name]
	return identity, exists
}

// GetIAMUserByAccessKey returns the user name for an access key.
// Must be called with at least a read lock held.
func (s *FSMState) GetIAMUserByAccessKey(accessKey string) (string, bool) {
	userName, exists := s.IAMCredentialIndex[accessKey]
	return userName, exists
}

// AddIAMPolicy adds a policy to the IAM state.
// Must be called with the write lock held.
func (s *FSMState) AddIAMPolicy(policy *iam.Policy) {
	s.IAMPolicies[policy.ID] = policy
	s.IAMVersion++
}

// RemoveIAMPolicy removes a policy from the IAM state.
// Must be called with the write lock held.
func (s *FSMState) RemoveIAMPolicy(policyID string) {
	delete(s.IAMPolicies, policyID)
	s.IAMVersion++
}

// GetIAMPolicy returns a policy by ID.
// Must be called with at least a read lock held.
func (s *FSMState) GetIAMPolicy(id string) (*iam.Policy, bool) {
	policy, exists := s.IAMPolicies[id]
	return policy, exists
}

// GetIAMVersion returns the current IAM version counter.
// Must be called with at least a read lock held.
func (s *FSMState) GetIAMVersion() uint64 {
	return s.IAMVersion
}

// HasIAMUsers returns true if there are any IAM users in the state.
// Must be called with at least a read lock held.
func (s *FSMState) HasIAMUsers() bool {
	return len(s.IAMUsers) > 0
}
