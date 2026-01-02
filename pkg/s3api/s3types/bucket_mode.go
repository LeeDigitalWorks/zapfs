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
)
