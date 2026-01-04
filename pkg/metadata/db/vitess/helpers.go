// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

// Note: All helper functions for scanning database rows are now provided by
// the shared sql package. See pkg/metadata/db/sql/helpers.go for implementations:
// - ScanObject, ScanObjects
// - ScanBucket, ScanBuckets
// - ScanMultipartUpload, ScanMultipartUploadBasic
// - ScanPart
// - StorageClass, ContentType
