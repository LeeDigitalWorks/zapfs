package file

import (
	"context"
	"runtime"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"

	"google.golang.org/protobuf/types/known/emptypb"
)

func (fs *FileServer) Ping(ctx context.Context, req *file_pb.PingRequest) (*file_pb.PingResponse, error) {
	return &file_pb.PingResponse{
		CurrentTime: uint64(time.Now().UnixNano()),
	}, nil
}

func (fs *FileServer) FileServerStatus(ctx context.Context, req *file_pb.FileServerStatusRequest) (*file_pb.FileServerStatusResponse, error) {
	// Collect backend info
	var backends []*common_pb.Backend
	for id, b := range fs.store.ListBackends() {
		backendProto := &common_pb.Backend{
			Id:         id,
			Type:       string(b.Type),
			Path:       b.Path,
			TotalBytes: b.TotalBytes,
			UsedBytes:  b.UsedBytes,
		}
		// Set MediaType if available
		if b.MediaType != "" {
			backendProto.MediaType = string(b.MediaType)
		}
		// Fill in actual disk stats for local backends
		if b.Path != "" && b.Type == types.StorageTypeLocal {
			backend.FillBackendStatus(backendProto)
		}
		backends = append(backends, backendProto)
	}

	// Collect memory stats
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	return &file_pb.FileServerStatusResponse{
		Backends: backends,
		MemoryStatus: &file_pb.MemStatus{
			Goroutines: int32(runtime.NumGoroutine()),
			All:        mem.Sys,
			Used:       mem.Alloc,
			Free:       mem.Frees,
			Self:       mem.HeapAlloc,
			Heap:       mem.HeapSys,
			Stack:      mem.StackSys,
		},
	}, nil
}

func (fs *FileServer) MarkBackendReadOnly(ctx context.Context, req *file_pb.MarkBackendReadOnlyRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (fs *FileServer) MarkBackendWritable(ctx context.Context, req *file_pb.MarkBackendWritableRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
