// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package backend

import "os"

// Fdatasync falls back to standard Sync on non-Linux platforms.
// On macOS, fsync already has fdatasync-like behavior.
// On Windows, this provides full sync semantics.
func Fdatasync(f *os.File) error {
	return f.Sync()
}

// FadviseDontNeed is a no-op on non-Linux platforms.
// macOS and Windows don't support fadvise.
func FadviseDontNeed(f *os.File) error {
	return nil
}

// OpenDirectIO falls back to regular file creation on non-Linux platforms.
// O_DIRECT is Linux-specific; macOS uses F_NOCACHE which requires different handling.
func OpenDirectIO(path string) (*os.File, error) {
	return os.Create(path)
}

// Fallocate is a no-op on non-Linux platforms.
// macOS and Windows don't support fallocate syscall.
func Fallocate(f *os.File, size int64) error {
	return nil
}
