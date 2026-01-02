// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"io"
)

type IndexKind int

const (
	IndexKindLevelDB IndexKind = iota
)

type Indexer[K comparable, V any] interface {
	io.Closer
	Put(key K, value V) error
	Get(key K) (V, error)
	Delete(key K) error
	Iterate(func(key K, value V) error) error
	Stream(filter func(value V) bool) <-chan V

	// Destroy removes the underlying idx file
	Destroy() error

	// Sync forces buffered writes to disk
	Sync() error

	// PutSync writes with immediate fsync (slower but durable)
	// Use for critical operations like RefCount updates
	PutSync(key K, value V) error

	// DeleteSync deletes with immediate fsync (slower but durable)
	DeleteSync(key K) error
}
