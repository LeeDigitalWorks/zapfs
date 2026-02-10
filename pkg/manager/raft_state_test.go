// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAddCollectionToIndexes_NoDuplicates(t *testing.T) {
	t.Parallel()

	state := NewFSMState("test-region", 3)

	col := &manager_pb.Collection{
		Name:      "my-bucket",
		Owner:     "user-1",
		Tier:      "standard",
		CreatedAt: timestamppb.Now(),
	}

	// Add the same collection twice
	state.AddCollectionToIndexes(col)
	state.AddCollectionToIndexes(col)

	// Owner index should contain exactly 1 entry
	ownerColls := state.GetCollectionsByOwner("user-1")
	require.Len(t, ownerColls, 1, "CollectionsByOwner should have exactly 1 entry, got duplicates")
	assert.Equal(t, "my-bucket", ownerColls[0])

	// Owner count should be 1
	assert.Equal(t, 1, state.GetOwnerCollectionCount("user-1"), "OwnerCollectionCount should be 1")

	// Tier index should contain exactly 1 entry
	tierColls := state.GetCollectionsByTier("standard")
	require.Len(t, tierColls, 1, "CollectionsByTier should have exactly 1 entry, got duplicates")
	assert.Equal(t, "my-bucket", tierColls[0])
}

func TestAddCollectionToIndexes_MultipleDistinct(t *testing.T) {
	t.Parallel()

	state := NewFSMState("test-region", 3)

	col1 := &manager_pb.Collection{
		Name:      "bucket-a",
		Owner:     "user-1",
		Tier:      "standard",
		CreatedAt: timestamppb.Now(),
	}
	col2 := &manager_pb.Collection{
		Name:      "bucket-b",
		Owner:     "user-1",
		Tier:      "standard",
		CreatedAt: timestamppb.Now(),
	}
	col3 := &manager_pb.Collection{
		Name:      "bucket-c",
		Owner:     "user-2",
		Tier:      "archive",
		CreatedAt: timestamppb.Now(),
	}

	state.AddCollectionToIndexes(col1)
	state.AddCollectionToIndexes(col2)
	state.AddCollectionToIndexes(col3)

	// user-1 should have 2 collections, sorted
	ownerColls := state.GetCollectionsByOwner("user-1")
	assert.Equal(t, []string{"bucket-a", "bucket-b"}, ownerColls)
	assert.Equal(t, 2, state.GetOwnerCollectionCount("user-1"))

	// user-2 should have 1 collection
	ownerColls2 := state.GetCollectionsByOwner("user-2")
	assert.Equal(t, []string{"bucket-c"}, ownerColls2)
	assert.Equal(t, 1, state.GetOwnerCollectionCount("user-2"))

	// standard tier should have 2 collections, sorted
	tierColls := state.GetCollectionsByTier("standard")
	assert.Equal(t, []string{"bucket-a", "bucket-b"}, tierColls)

	// archive tier should have 1 collection
	archiveColls := state.GetCollectionsByTier("archive")
	assert.Equal(t, []string{"bucket-c"}, archiveColls)
}
