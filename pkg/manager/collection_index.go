// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
)

// ===== COLLECTION INDEX MANAGEMENT =====
// These methods manage secondary indexes for efficient collection lookups.

// addCollectionToIndexes adds collection to secondary indexes (must hold lock)
func (ms *ManagerServer) addCollectionToIndexes(col *manager_pb.Collection) {
	// Add to owner index (sorted)
	ownerColls := ms.collectionsByOwner[col.Owner]
	ownerColls = append(ownerColls, col.Name)
	// Keep sorted for efficient prefix scans
	insertSorted(&ownerColls, col.Name)
	ms.collectionsByOwner[col.Owner] = ownerColls

	// Increment owner count
	ms.ownerCollectionCount[col.Owner]++

	// Add to tier index
	tierColls := ms.collectionsByTier[col.Tier]
	tierColls = append(tierColls, col.Name)
	ms.collectionsByTier[col.Tier] = tierColls

	// Add to time-ordered index
	ms.collectionsByTime.ReplaceOrInsert(&collectionTimeItem{
		createdAt:  col.CreatedAt.AsTime(),
		name:       col.Name,
		collection: col,
	})
}

// removeCollectionFromIndexes removes collection from secondary indexes (must hold lock)
func (ms *ManagerServer) removeCollectionFromIndexes(col *manager_pb.Collection) {
	// Remove from owner index
	if ownerColls, ok := ms.collectionsByOwner[col.Owner]; ok {
		ms.collectionsByOwner[col.Owner] = removeFromSlice(ownerColls, col.Name)
		if len(ms.collectionsByOwner[col.Owner]) == 0 {
			delete(ms.collectionsByOwner, col.Owner)
		}
	}

	// Decrement owner count
	ms.ownerCollectionCount[col.Owner]--
	if ms.ownerCollectionCount[col.Owner] == 0 {
		delete(ms.ownerCollectionCount, col.Owner)
	}

	// Remove from tier index
	if tierColls, ok := ms.collectionsByTier[col.Tier]; ok {
		ms.collectionsByTier[col.Tier] = removeFromSlice(tierColls, col.Name)
		if len(ms.collectionsByTier[col.Tier]) == 0 {
			delete(ms.collectionsByTier, col.Tier)
		}
	}

	// Remove from time-ordered index
	ms.collectionsByTime.Delete(&collectionTimeItem{
		createdAt: col.CreatedAt.AsTime(),
		name:      col.Name,
	})
}

// GetOwnerCollectionCount returns number of collections owned by an owner (must hold lock)
func (ms *ManagerServer) GetOwnerCollectionCount(owner string) int {
	return ms.ownerCollectionCount[owner]
}

// GetCollectionsByOwner returns collection names for an owner (must hold lock)
// Results are already sorted
func (ms *ManagerServer) GetCollectionsByOwner(owner string) []string {
	return ms.collectionsByOwner[owner]
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
