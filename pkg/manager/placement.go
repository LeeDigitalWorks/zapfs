package manager

import (
	"zapfs/proto/manager_pb"
)

// selectReplicationTargets selects file services for storing replicas
// This is called with ms.mu.RLock() held
func (ms *ManagerServer) selectReplicationTargets(fileSize uint64, numReplicas uint32, tier string) []*manager_pb.ReplicationTarget {
	var targets []*manager_pb.ReplicationTarget
	priority := int32(1)

	// Simple implementation: select first N active file services with available space
	// TODO: Implement advanced placement:
	// - Rack-aware placement
	// - Zone-aware placement
	// - Datacenter-aware placement
	// - Load balancing (least utilized first)
	// - Tier-based placement (SSD vs HDD)

	for _, reg := range ms.fileServices {
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

				targets = append(targets, &manager_pb.ReplicationTarget{
					Location:    reg.Location,
					BackendId:   backend.Id,
					BackendType: backend.Type,
					Priority:    priority,
				})

				priority++
				if uint32(len(targets)) >= numReplicas {
					return targets
				}

				// One backend per storage backend for now
				break
			}

			if uint32(len(targets)) >= numReplicas {
				return targets
			}
		}

		if uint32(len(targets)) >= numReplicas {
			return targets
		}
	}

	return targets
}
