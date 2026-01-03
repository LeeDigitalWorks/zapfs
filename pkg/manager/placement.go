// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"sort"

	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
)

// placementHeadroomPercent is the minimum free space percentage to leave on a backend
// after placing a file. This helps mitigate stale capacity info from heartbeats.
const placementHeadroomPercent = 5

// backendCandidate holds a backend and its free space for sorting
type backendCandidate struct {
	location    *common_pb.Location
	backendID   string
	backendType string
	freeBytes   uint64
}

// selectReplicationTargets selects file services for storing replicas
// This is called with ms.state.RLock() held
func (ms *ManagerServer) selectReplicationTargets(fileSize uint64, numReplicas uint32, tier string) []*manager_pb.ReplicationTarget {
	// Collect all suitable candidates
	var candidates []backendCandidate

	// TODO: Implement advanced placement:
	// - Rack-aware placement
	// - Zone-aware placement
	// - Datacenter-aware placement
	// - Tier-based placement (SSD vs HDD)

	for _, reg := range ms.state.FileServices {
		if reg.Status != ServiceActive {
			continue
		}

		// Iterate through storage backends
		for _, storageBackend := range reg.StorageBackends {
			if len(storageBackend.Backends) == 0 {
				continue
			}

			// Find suitable backend
			for _, backend := range storageBackend.Backends {
				// TODO: Implement tier-based filtering when tier represents media type or storage class
				// For now, tier is the storage profile name (e.g., "STANDARD"), not media type
				// So we don't filter by tier here

				// Check if backend has enough space
				freeBytes := backend.TotalBytes - backend.UsedBytes
				if freeBytes < fileSize {
					continue
				}

				// Apply headroom buffer to mitigate stale capacity data from heartbeats
				// Require at least placementHeadroomPercent free space after the write
				minFreeAfterWrite := (backend.TotalBytes * placementHeadroomPercent) / 100
				if freeBytes-fileSize < minFreeAfterWrite {
					continue
				}

				candidates = append(candidates, backendCandidate{
					location:    reg.Location,
					backendID:   backend.Id,
					backendType: backend.Type,
					freeBytes:   freeBytes,
				})

				// One backend per storage backend for now
				break
			}
		}
	}

	// Sort candidates by free space (most free first) to spread load evenly
	// This helps mitigate stale capacity info by preferring less-utilized servers
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].freeBytes > candidates[j].freeBytes
	})

	// Build targets from sorted candidates
	var targets []*manager_pb.ReplicationTarget
	for i, c := range candidates {
		if uint32(len(targets)) >= numReplicas {
			break
		}
		targets = append(targets, &manager_pb.ReplicationTarget{
			Location:    c.location,
			BackendId:   c.backendID,
			BackendType: c.backendType,
			Priority:    int32(i + 1),
		})
	}

	return targets
}
