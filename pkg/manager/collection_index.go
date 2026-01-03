// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
)

// ===== COLLECTION INDEX MANAGEMENT =====
// These methods delegate to RaftState for secondary index management.

// addCollectionToIndexes adds collection to secondary indexes (must hold state lock)
func (ms *ManagerServer) addCollectionToIndexes(col *manager_pb.Collection) {
	ms.state.AddCollectionToIndexes(col)
}

// removeCollectionFromIndexes removes collection from secondary indexes (must hold state lock)
func (ms *ManagerServer) removeCollectionFromIndexes(col *manager_pb.Collection) {
	ms.state.RemoveCollectionFromIndexes(col)
}

// GetOwnerCollectionCount returns number of collections owned by an owner (must hold state lock)
func (ms *ManagerServer) GetOwnerCollectionCount(owner string) int {
	return ms.state.GetOwnerCollectionCount(owner)
}

// GetCollectionsByOwner returns collection names for an owner (must hold state lock)
// Results are already sorted
func (ms *ManagerServer) GetCollectionsByOwner(owner string) []string {
	return ms.state.GetCollectionsByOwner(owner)
}

// Helper: insert into sorted slice (maintains sort order)
func insertSorted(slice *[]string, value string) {
	// Binary search for insertion point
	i := 0
	j := len(*slice)
	for i < j {
		h := i + (j-i)/2
		if (*slice)[h] < value {
			i = h + 1
		} else {
			j = h
		}
	}

	// Insert at position i
	*slice = append(*slice, "")
	copy((*slice)[i+1:], (*slice)[i:])
	(*slice)[i] = value
}

// Helper: remove value from slice
func removeFromSlice(slice []string, value string) []string {
	for i, v := range slice {
		if v == value {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}
