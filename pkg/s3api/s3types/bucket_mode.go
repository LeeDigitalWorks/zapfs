// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

type BucketMode uint64

const (
	BucketModeNone BucketMode = iota
	BucketModeMigrating
	BucketModeAdminLocked
	BucketModeAsyncDelete
	BucketModeReadOnly
	BucketModePassthrough // Proxy requests to external S3, metadata stored locally (federation)
)

// IsFederated returns true if the bucket is federated (passthrough or migrating)
func (m BucketMode) IsFederated() bool {
	return m == BucketModePassthrough || m == BucketModeMigrating
}

// String returns the string representation of the bucket mode
func (m BucketMode) String() string {
	switch m {
	case BucketModeNone:
		return "none"
	case BucketModePassthrough:
		return "passthrough"
	case BucketModeMigrating:
		return "migrating"
	case BucketModeAdminLocked:
		return "admin_locked"
	case BucketModeAsyncDelete:
		return "async_delete"
	case BucketModeReadOnly:
		return "read_only"
	default:
		return "unknown"
	}
}

// ParseBucketMode parses a string into a BucketMode
func ParseBucketMode(s string) BucketMode {
	switch s {
	case "none", "":
		return BucketModeNone
	case "passthrough":
		return BucketModePassthrough
	case "migrating":
		return BucketModeMigrating
	case "admin_locked":
		return BucketModeAdminLocked
	case "async_delete":
		return BucketModeAsyncDelete
	case "read_only":
		return BucketModeReadOnly
	default:
		return BucketModeNone
	}
}
