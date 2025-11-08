package types

import (
	"math/rand"
)

// PlacementRules contains rules for placing objects of different sizes
type PlacementRules struct {
	// SizeThresholds maps minimum object size to placement rule
	// Use 0 as key for default rule
	SizeThresholds map[uint64]PlacementRule `json:"size_thresholds"`
}

// GetRule returns the appropriate rule for an object of the given size
func (pr PlacementRules) GetRule(size uint64) PlacementRule {
	var bestMatch PlacementRule
	var bestThreshold uint64

	for threshold, rule := range pr.SizeThresholds {
		if size >= threshold && threshold >= bestThreshold {
			bestMatch = rule
			bestThreshold = threshold
		}
	}
	return bestMatch
}

// PlacementRule defines how to place data across backends
type PlacementRule struct {
	// Targets defines which backends can be used and their weights
	Targets []PlacementTarget `json:"targets"`

	// Replication settings (mutually exclusive with EC)
	ReplicaCount int `json:"replica_count,omitempty"` // Number of copies (e.g., 3)

	// Erasure coding settings (mutually exclusive with replication)
	ECScheme *ECScheme `json:"ec_scheme,omitempty"` // EC parameters if using EC

	// Constraints
	SpreadAcrossRacks bool `json:"spread_across_racks,omitempty"` // Ensure replicas/shards on different racks
	SpreadAcrossDCs   bool `json:"spread_across_dcs,omitempty"`   // Ensure replicas/shards in different DCs
}

// IsErasureCoded returns true if this rule uses EC instead of replication
func (r PlacementRule) IsErasureCoded() bool {
	return r.ECScheme != nil && r.ECScheme.DataShards > 0
}

// PlacementTarget defines a backend that can be used for placement
type PlacementTarget struct {
	BackendID string `json:"backend_id"`
	Weight    uint32 `json:"weight"` // Relative weight for selection (higher = more likely)
}

// SelectBackend picks a backend from targets using weighted random selection
func (r PlacementRule) SelectBackend(rng *rand.Rand) string {
	if len(r.Targets) == 0 {
		return ""
	}
	if len(r.Targets) == 1 {
		return r.Targets[0].BackendID
	}

	var totalWeight uint32
	for _, t := range r.Targets {
		totalWeight += t.Weight
	}

	if totalWeight == 0 {
		// All weights zero, pick random
		return r.Targets[rng.Intn(len(r.Targets))].BackendID
	}

	pick := rng.Uint32() % totalWeight
	var cumulative uint32
	for _, t := range r.Targets {
		cumulative += t.Weight
		if pick < cumulative {
			return t.BackendID
		}
	}
	return r.Targets[len(r.Targets)-1].BackendID
}

// SelectBackends picks n unique backends for replica/shard placement
func (r PlacementRule) SelectBackends(n int, rng *rand.Rand) []string {
	if n <= 0 || len(r.Targets) == 0 {
		return nil
	}
	if n > len(r.Targets) {
		n = len(r.Targets)
	}

	// Shuffle targets by weight then pick first n
	type weightedTarget struct {
		id    string
		score float64
	}

	weighted := make([]weightedTarget, len(r.Targets))
	for i, t := range r.Targets {
		// Score = random * weight (higher weight = higher chance of being first)
		weighted[i] = weightedTarget{
			id:    t.BackendID,
			score: rng.Float64() * float64(t.Weight+1),
		}
	}

	// Sort by score descending
	for i := 0; i < len(weighted)-1; i++ {
		for j := i + 1; j < len(weighted); j++ {
			if weighted[j].score > weighted[i].score {
				weighted[i], weighted[j] = weighted[j], weighted[i]
			}
		}
	}

	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = weighted[i].id
	}
	return result
}

// BackendSet tracks available backends and their capacity
type BackendSet struct {
	backends map[string]*Backend
}

// Add adds or updates a backend in the set
func (bs *BackendSet) Add(b *Backend) {
	if bs.backends == nil {
		bs.backends = make(map[string]*Backend)
	}
	bs.backends[b.ID] = b
}

// Get retrieves a backend by ID
func (bs *BackendSet) Get(id string) (*Backend, bool) {
	if bs.backends == nil {
		return nil, false
	}
	b, ok := bs.backends[id]
	return b, ok
}

// Remove removes a backend from the set
func (bs *BackendSet) Remove(id string) {
	if bs.backends != nil {
		delete(bs.backends, id)
	}
}

// List returns all backends
func (bs *BackendSet) List() []*Backend {
	if bs.backends == nil {
		return nil
	}
	result := make([]*Backend, 0, len(bs.backends))
	for _, b := range bs.backends {
		result = append(result, b)
	}
	return result
}

// ByType returns backends matching the given storage type
func (bs *BackendSet) ByType(t StorageType) []*Backend {
	var result []*Backend
	for _, b := range bs.backends {
		if b.Type == t {
			result = append(result, b)
		}
	}
	return result
}

// ByMediaType returns backends matching the given media type
func (bs *BackendSet) ByMediaType(m MediaType) []*Backend {
	var result []*Backend
	for _, b := range bs.backends {
		if b.MediaType == m {
			result = append(result, b)
		}
	}
	return result
}
