package backend

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

func init() {
	Register(types.StorageTypeLocal, NewLocal)
}

// Local implements BackendStorage for local filesystem
type Local struct {
	basePath string
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

	return &Local{basePath: cfg.Path}, nil
}

func (l *Local) Type() types.StorageType {
	return types.StorageTypeLocal
}

func (l *Local) Write(ctx context.Context, key string, data io.Reader, size int64) error {
	path := filepath.Join(l.basePath, key)

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create parent dir: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, data); err != nil {
		os.Remove(path) // Clean up on error
		return fmt.Errorf("write data: %w", err)
	}

	return f.Sync()
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
