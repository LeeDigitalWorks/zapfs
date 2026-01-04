// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

// Note: All transaction operations (CreateBucket, GetBucket, DeleteBucket, ListBuckets,
// PutObject, GetObject, DeleteObject, CreateMultipartUpload, etc.) are now provided
// by the shared *sql.TxStore. See pkg/metadata/db/sql/tx_*.go for implementations.
//
// The WithTx method in postgres.go creates a sql.TxStore with PostgresDialect.
