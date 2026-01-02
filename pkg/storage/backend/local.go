// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

func init() {
	Register(types.StorageTypeLocal, NewLocal)
}

// Local implements BackendStorage for local filesystem
type Local struct {
	basePath       string
	dirsPreCreated bool // true if prefix directories were pre-created
	useDirectIO    bool // true to use O_DIRECT (Linux only)
}

// NewLocal creates a local filesystem backend
func NewLocal(cfg types.BackendConfig) (types.BackendStorage, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("path required for local backend")
	}

	// Ensure base path exists
	if err := os.MkdirAll(cfg.Path, 0755); err != nil {
		return nil, fmt.Errorf("create base path: %w", err)
	}

	l := &Local{
		basePath:    cfg.Path,
		useDirectIO: cfg.DirectIO,
	}

	// Pre-create chunk prefix directories (256 * 256 = 65,536 dirs)
	// This eliminates MkdirAll calls on every write
	if err := l.preCreateDirs(); err != nil {
		// Non-fatal: fall back to creating dirs on demand
		l.dirsPreCreated = false
	} else {
		l.dirsPreCreated = true
	}

	return l, nil
}

// preCreateDirs creates all 65,536 prefix directories for chunk storage.
// Directory structure: {base}/{xx}/{yy}/ where xx and yy are hex bytes (00-ff).
func (l *Local) preCreateDirs() error {
	hexChars := "0123456789abcdef"
	for _, a := range hexChars {
		for _, b := range hexChars {
			prefix1 := string([]rune{a, b})
			for _, c := range hexChars {
				for _, d := range hexChars {
					prefix2 := string([]rune{c, d})
					dir := filepath.Join(l.basePath, prefix1, prefix2)
					if err := os.MkdirAll(dir, 0755); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (l *Local) Type() types.StorageType {
	return types.StorageTypeLocal
}

// isChunkPath returns true if the key looks like a chunk path (xx/yy/chunkID).
// Chunk paths have exactly 2 path separators and first two segments are 2 hex chars.
func (l *Local) isChunkPath(key string) bool {
	// Expected format: "xx/yy/chunkID" where xx and yy are hex bytes
	if len(key) < 6 { // minimum: "00/00/x"
		return false
	}
	// Check format: 2 hex chars, slash, 2 hex chars, slash, rest
	if key[2] != '/' || key[5] != '/' {
		return false
	}
	return isHexByte(key[0:2]) && isHexByte(key[3:5])
}

// isHexByte returns true if s is a 2-character hex string (00-ff).
func isHexByte(s string) bool {
	if len(s) != 2 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// writeBufferSize is the buffer size for io.CopyBuffer.
// Using 4MB to match chunk size reduces syscall overhead.
const writeBufferSize = 4 * 1024 * 1024

func (l *Local) Write(ctx context.Context, key string, data io.Reader, size int64) error {
	path := filepath.Join(l.basePath, key)
	parentDir := filepath.Dir(path)

	// Create parent directory if needed.
	// Skip if pre-created dirs exist AND this looks like a chunk path (xx/yy/chunkID).
	// For non-chunk paths, always ensure the directory exists.
	if !l.dirsPreCreated || !l.isChunkPath(key) {
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			return fmt.Errorf("create parent dir: %w", err)
		}
	}

	// Open file with appropriate flags
	var f *os.File
	var err error
	if l.useDirectIO {
		f, err = OpenDirectIO(path)
	} else {
		f, err = os.Create(path)
	}
	if err != nil {
		// If direct IO fails (unsupported filesystem), fall back to buffered
		if l.useDirectIO {
			f, err = os.Create(path)
		}
		if err != nil {
			return fmt.Errorf("create file: %w", err)
		}
	}
	defer f.Close()

	// Preallocate disk space if size is known (reduces fragmentation)
	if size > 0 {
		_ = Fallocate(f, size) // Ignore error, not all filesystems support it
	}

	// Use pooled buffer to reduce allocations
	buf := utils.GetBuffer(writeBufferSize)
	defer utils.PutBuffer(buf)

	if _, err := io.CopyBuffer(f, data, buf); err != nil {
		os.Remove(path) // Clean up on error
		return fmt.Errorf("write data: %w", err)
	}

	// Use fdatasync for better performance (skips unnecessary metadata flush)
	if err := Fdatasync(f); err != nil {
		os.Remove(path)
		return fmt.Errorf("sync: %w", err)
	}

	// Free page cache since chunk data is unlikely to be re-read immediately
	_ = FadviseDontNeed(f)

	return nil
}

func (l *Local) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	path := filepath.Join(l.basePath, key)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, err
	}
	return f, nil
}

func (l *Local) ReadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	path := filepath.Join(l.basePath, key)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, err
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("seek: %w", err)
	}

	// Wrap in a limited reader if length specified
	if length > 0 {
		return &limitedReadCloser{
			Reader: io.LimitReader(f, length),
			Closer: f,
		}, nil
	}
	return f, nil
}

func (l *Local) Delete(ctx context.Context, key string) error {
	path := filepath.Join(l.basePath, key)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil // Already gone
	}
	return err
}

func (l *Local) Exists(ctx context.Context, key string) (bool, error) {
	path := filepath.Join(l.basePath, key)
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (l *Local) Size(ctx context.Context, key string) (int64, error) {
	path := filepath.Join(l.basePath, key)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Errorf("key not found: %s", key)
		}
		return 0, err
	}
	return info.Size(), nil
}

func (l *Local) Close() error {
	return nil
}

// limitedReadCloser wraps a limited reader with a closer
type limitedReadCloser struct {
	io.Reader
	io.Closer
}
