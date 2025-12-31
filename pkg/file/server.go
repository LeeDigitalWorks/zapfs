package file

import (
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/grpc/pool"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/store"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"

	"google.golang.org/grpc"
)

type FileServer struct {
	file_pb.UnimplementedFileServiceServer

	store *store.FileStore

	// peerPool manages connections to peer file servers for replication
	peerPool *pool.Pool[file_pb.FileServiceClient]
}

// fileClientFactory creates a file service client from a gRPC connection
func fileClientFactory(cc grpc.ClientConnInterface) file_pb.FileServiceClient {
	return file_pb.NewFileServiceClient(cc)
}

func NewFileServer(mux *http.ServeMux, cfg store.Config, manager *backend.Manager) (*FileServer, error) {
	fs := &FileServer{
		peerPool: pool.NewPool(fileClientFactory),
	}

	var err error
	fs.store, err = store.NewFileStore(cfg, manager)
	if err != nil {
		return nil, err
	}

	mux.HandleFunc("/", fs.ServeHTTP)
	return fs, nil
}

func (fs *FileServer) Shutdown() {
	fs.store.Close()
	if fs.peerPool != nil {
		fs.peerPool.Close()
	}
}
