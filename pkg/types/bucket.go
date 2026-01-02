// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package types

import "github.com/google/uuid"

// BucketInfo represents bucket metadata
type BucketInfo struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	OwnerID   string    `json:"owner_id"`
	Region    string    `json:"region,omitempty"`
	CreatedAt int64     `json:"created_at"`

	// Default storage profile for objects in this bucket
	DefaultProfileID string `json:"default_profile_id,omitempty"`

	// Versioning: "Enabled", "Suspended", or "" (disabled)
	Versioning string `json:"versioning,omitempty"`
}
