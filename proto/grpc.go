package proto

import (
	"context"
	"errors"
	"sync"

	zctx "github.com/LeeDigitalWorks/zapfs/pkg/context"
	pool "github.com/LeeDigitalWorks/zapfs/pkg/grpc/pool"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/metadata_pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// Re-export constants from pkg/grpc/pool for backwards compatibility
const (
	Max_Message_Size = pool.MaxMessageSize
	KeepAliveTime    = pool.KeepAliveTime
	KeepAliveTimeout = pool.KeepAliveTimeout
)

// ConnectionFactory is a function that creates a new gRPC client connection.
type ConnectionFactory func() (*grpc.ClientConn, error)

// GRPCBalancer implements a simple connection balancer for gRPC clients.
//
// Deprecated: Use pkg/grpc/pool.Pool[T] for new code. Pool provides:
//   - Health checking via gRPC connectivity state
//   - Per-host connection management
//   - ClusterPool[T] for Raft cluster leader routing
type GRPCBalancer struct {
	grpc.ClientConnInterface
	mu          sync.Mutex
	connections map[*grpc.ClientConn]struct{}

	factory   ConnectionFactory
	connCount uint64
}

func NewGRPCBalancer(factory ConnectionFactory, connCount uint64) *GRPCBalancer {
	return &GRPCBalancer{
		connections: make(map[*grpc.ClientConn]struct{}),
		factory:     factory,
		connCount:   connCount,
	}
}

func (b *GRPCBalancer) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	conn, err := b.getConnection(ctx)
	if err != nil {
		return err
	}
	return conn.Invoke(ctx, method, args, reply, opts...)
}

func (b *GRPCBalancer) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn, err := b.getConnection(ctx)
	if err != nil {
		return nil, err
	}
	return conn.NewStream(ctx, desc, method, opts...)
}

func (b *GRPCBalancer) getConnection(_ context.Context) (*grpc.ClientConn, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for len(b.connections) < int(b.connCount) {
		conn, err := b.factory()
		if err != nil {
			return nil, err
		}
		b.connections[conn] = struct{}{}
	}

	for conn := range b.connections {
		return conn, nil
	}

	return nil, errors.New("no connections available")
}

func (b *GRPCBalancer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var errs []error
	for conn := range b.connections {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
		delete(b.connections, conn)
	}

	return errors.Join(errs...)
}

func DefaultFactory(address string, opts ...grpc.DialOption) ConnectionFactory {
	options := append(opts,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    KeepAliveTime,
			Timeout: KeepAliveTimeout,
		}),
	)

	return func() (*grpc.ClientConn, error) {
		return grpc.NewClient(address, options...)
	}
}

func NewFileClient(address string, useBalancer bool, connCount uint64, opts ...grpc.DialOption) (file_pb.FileServiceClient, error) {
	factory := DefaultFactory(address, opts...)
	if !useBalancer {
		conn, err := factory()
		if err != nil {
			logger.Error().Err(err).Msg("failed to create file client")
			return nil, err
		}
		return file_pb.NewFileServiceClient(conn), nil
	}
	return file_pb.NewFileServiceClient(NewGRPCBalancer(factory, connCount)), nil
}

func NewMetadataClient(address string, useBalancer bool, connCount uint64, opts ...grpc.DialOption) (metadata_pb.MetadataServiceClient, error) {
	factory := DefaultFactory(address, opts...)
	if !useBalancer {
		conn, err := factory()
		if err != nil {
			logger.Error().Err(err).Msg("failed to create metadata client")
			return nil, err
		}
		return metadata_pb.NewMetadataServiceClient(conn), nil
	}
	return metadata_pb.NewMetadataServiceClient(NewGRPCBalancer(factory, connCount)), nil
}

func NewManagerClient(address string, useBalancer bool, connCount uint64, opts ...grpc.DialOption) (manager_pb.ManagerServiceClient, error) {
	factory := DefaultFactory(address, opts...)
	if !useBalancer {
		conn, err := factory()
		if err != nil {
			logger.Error().Err(err).Msg("failed to create manager client")
			return nil, err
		}
		return manager_pb.NewManagerServiceClient(conn), nil
	}
	return manager_pb.NewManagerServiceClient(NewGRPCBalancer(factory, connCount)), nil
}

func NewGRPCServer(opts ...grpc.ServerOption) *grpc.Server {
	var options []grpc.ServerOption
	options = append(options,
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    KeepAliveTime,    // wait time before ping if no activity
			Timeout: KeepAliveTimeout, // ping timeout
			// MaxConnectionAge: 10 * time.Hour,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             KeepAliveTime, // min time a client should wait before sending a ping
			PermitWithoutStream: true,
		}),
		grpc.MaxRecvMsgSize(Max_Message_Size),
		grpc.MaxSendMsgSize(Max_Message_Size),
		grpc.UnaryInterceptor(requestIDUnaryInterceptor()),
	)
	for _, opt := range opts {
		if opt != nil {
			options = append(options, opt)
		}
	}
	return grpc.NewServer(options...)
}

func requestIDUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		incomingMd, _ := metadata.FromIncomingContext(ctx)
		idList := incomingMd.Get(zctx.RequestKey)
		var reqID string
		if len(idList) > 0 {
			reqID = idList[0]
		}
		if reqID == "" {
			ctx, reqID = zctx.WithUUID(ctx)
		}

		ctx = metadata.NewOutgoingContext(ctx,
			metadata.New(map[string]string{
				zctx.RequestKey: reqID,
			}))

		ctx = zctx.FromUUID(ctx, reqID)

		grpc.SetTrailer(ctx, metadata.Pairs(zctx.RequestKey, reqID))

		return handler(ctx, req)
	}
}
