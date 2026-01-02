// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package backend

import (
	"os"

	"golang.org/x/sys/unix"
)

// Fdatasync syncs file data to disk without flushing unnecessary metadata.
// This is faster than fsync() because it only flushes metadata needed for
// correct data retrieval (e.g., file size) but not atime/mtime.
func Fdatasync(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}

// FadviseDontNeed advises the kernel that the file data won't be accessed
// soon, allowing it to free the page cache. Use after writing large files
// that won't be re-read immediately.
func FadviseDontNeed(f *os.File) error {
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}

// OpenDirectIO opens a file with O_DIRECT flag to bypass the page cache.
// This reduces memory pressure and provides more predictable latency for
// large sequential writes. Requires aligned buffers for I/O.
func OpenDirectIO(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|unix.O_DIRECT, 0644)
}

// Fallocate preallocates disk space for a file.
// This ensures contiguous blocks on disk, reducing fragmentation and
// avoiding "no space left" errors mid-write.
// Supported on ext4, XFS, Btrfs. Silently fails on unsupported filesystems.
func Fallocate(f *os.File, size int64) error {
	// Mode 0 = default allocation (extends file size if needed)
	return unix.Fallocate(int(f.Fd()), 0, 0, size)
}
