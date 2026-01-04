// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import (
	"encoding/xml"
	"time"
)

// FederationConfig stores the external S3 connection details for a federated bucket.
// This is stored per-bucket in the metadata database.
type FederationConfig struct {
	// Bucket is the local bucket name (primary key)
	Bucket string `json:"bucket"`

	// External S3 connection details
	Endpoint        string `json:"endpoint"`          // e.g., "s3.amazonaws.com"
	Region          string `json:"region"`            // e.g., "us-east-1"
	AccessKeyID     string `json:"access_key_id"`     // External S3 credentials
	SecretAccessKey string `json:"secret_access_key"` // Encrypted at rest
	ExternalBucket  string `json:"external_bucket"`   // Bucket name on external S3
	PathStyle       bool   `json:"path_style"`        // Use path-style addressing

	// Migration tracking
	MigrationStartedAt int64  `json:"migration_started_at,omitempty"` // Unix timestamp
	MigrationPaused    bool   `json:"migration_paused,omitempty"`     // Is migration paused
	ObjectsDiscovered  int64  `json:"objects_discovered,omitempty"`   // Total objects found
	ObjectsSynced      int64  `json:"objects_synced,omitempty"`       // Objects with local chunks
	BytesSynced        int64  `json:"bytes_synced,omitempty"`         // Total bytes synced
	LastSyncKey        string `json:"last_sync_key,omitempty"`        // Resume point for discovery
	DualWriteEnabled   bool   `json:"dual_write_enabled,omitempty"`   // Write to both local and external

	// Timestamps
	CreatedAt int64 `json:"created_at"`
	UpdatedAt int64 `json:"updated_at"`
}

// ProgressPercent returns the migration progress as a percentage (0-100)
func (c *FederationConfig) ProgressPercent() float64 {
	if c.ObjectsDiscovered == 0 {
		return 0
	}
	return float64(c.ObjectsSynced) / float64(c.ObjectsDiscovered) * 100
}

// IsComplete returns true if all discovered objects have been synced
func (c *FederationConfig) IsComplete() bool {
	return c.ObjectsDiscovered > 0 && c.ObjectsSynced >= c.ObjectsDiscovered
}

// CopyObjectResult is the XML response body for CopyObject operations.
type CopyObjectResult struct {
	ETag         string    `xml:"ETag"`
	LastModified time.Time `xml:"LastModified"`
}

// ToXML serializes CopyObjectResult to XML with the proper namespace.
func (r *CopyObjectResult) ToXML() ([]byte, error) {
	type copyResult struct {
		XMLName      xml.Name  `xml:"CopyObjectResult"`
		ETag         string    `xml:"ETag"`
		LastModified time.Time `xml:"LastModified"`
	}
	result := copyResult{
		ETag:         r.ETag,
		LastModified: r.LastModified,
	}
	return xml.Marshal(result)
}
