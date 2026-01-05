// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package compression provides data compression and decompression utilities
// for ZapFS storage. It supports multiple algorithms (LZ4, ZSTD, Snappy)
// with a unified interface.
package compression

// Algorithm represents a compression algorithm
type Algorithm string

const (
	// None indicates no compression
	None Algorithm = "none"
	// LZ4 uses the LZ4 compression algorithm (fast, moderate ratio)
	LZ4 Algorithm = "lz4"
	// ZSTD uses the Zstandard compression algorithm (balanced speed/ratio)
	ZSTD Algorithm = "zstd"
	// Snappy uses the Snappy compression algorithm (very fast, lower ratio)
	Snappy Algorithm = "snappy"
)

// IsValid returns true if the algorithm is recognized
func (a Algorithm) IsValid() bool {
	switch a {
	case None, LZ4, ZSTD, Snappy:
		return true
	default:
		return false
	}
}

// String returns the string representation of the algorithm
func (a Algorithm) String() string {
	return string(a)
}

// ParseAlgorithm parses a string into an Algorithm.
// Returns None for empty or unrecognized strings.
func ParseAlgorithm(s string) Algorithm {
	algo := Algorithm(s)
	if algo.IsValid() {
		return algo
	}
	return None
}
