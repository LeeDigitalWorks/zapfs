package backend

import (
	"syscall"

	"zapfs/proto/common_pb"
)

// NewBackendStatus creates a Backend proto with disk stats
func NewBackendStatus(id, path string, backendType string) *common_pb.Backend {
	backend := &common_pb.Backend{
		Id:   id,
		Path: path,
		Type: backendType,
	}
	FillBackendStatus(backend)
	return backend
}

// FillBackendStatus populates TotalBytes and UsedBytes for local backends
func FillBackendStatus(backend *common_pb.Backend) {
	if backend.Path == "" {
		return // Not a local backend
	}
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(backend.Path, &fs)
	if err != nil {
		return
	}
	backend.TotalBytes = fs.Blocks * uint64(fs.Bsize)
	backend.UsedBytes = backend.TotalBytes - (uint64(fs.Bavail) * uint64(fs.Bsize))
}
