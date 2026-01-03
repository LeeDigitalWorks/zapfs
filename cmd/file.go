// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/debug"
	"github.com/LeeDigitalWorks/zapfs/pkg/file"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/store"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
	"github.com/LeeDigitalWorks/zapfs/proto"
	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// FileServerOpts holds all configuration for the file server
type FileServerOpts struct {
	// Network binding
	BindAddr  string // Address to bind to (e.g., "0.0.0.0:8001" or ":8001")
	HTTPPort  int    // HTTP port (binds to same IP as gRPC)
	DebugPort int    // Debug HTTP port

	// Service identity and discovery
	NodeID        string // Stable node identifier (e.g., "file-1" or pod name in K8s)
	AdvertiseAddr string // Address to advertise to peers (e.g., "file-1:8001" or "pod.svc:8001")

	// Storage
	IndexPath string
	ECScheme  types.ECScheme
	DirectIO  bool // Use O_DIRECT for writes (Linux only)
	Backends  []BackendOpts

	// Reconciliation configuration
	ReconciliationInterval    time.Duration // How often to run reconciliation (0 = disabled)
	ReconciliationGracePeriod time.Duration // Grace period before deleting orphan chunks

	// TLS
	CertFile string
	KeyFile  string

	// Manager registration
	ManagerAddr string // Manager service address for registration
}

// BackendOpts holds configuration for a storage backend
type BackendOpts struct {
	ID           string
	Type         types.StorageType
	Path         string // For local disk backends
	Endpoint     string // For S3/remote backends
	Bucket       string
	Region       string
	AccessKey    string
	SecretKey    string
	StorageClass string
	Enabled      bool
	DirectIO     bool // Use O_DIRECT for writes (Linux only, bypasses page cache)
	MinFreeSpace string
}

var fileCmd = &cobra.Command{
	Use:   "file",
	Short: "Start file server",
	Long: `Start a ZapFS file server that handles chunk storage and retrieval.
File servers are responsible for actual data storage on disk or remote backends.`,
	Run: runFileServer,
}

func init() {
	rootCmd.AddCommand(fileCmd)

	f := fileCmd.Flags()

	// Network binding (following etcd/CockroachDB pattern)
	// --bind-addr: What interface to listen on
	// --advertise-addr: What address to tell peers (must be reachable from other nodes)
	f.String("bind_addr", "0.0.0.0:8001", "Address to bind gRPC server (host:port). Use 0.0.0.0 to listen on all interfaces.")
	f.Int("http_port", 8000, "HTTP port (binds to same interface as gRPC)")
	f.Int("debug_port", 8010, "Debug/metrics HTTP port")

	// Service identity and discovery
	// In Docker Compose: node_id=file-1, advertise_addr=file-1:8001
	// In Kubernetes: node_id=$(POD_NAME), advertise_addr=$(POD_NAME).file.ns.svc.cluster.local:8001
	f.String("node_id", "", "Stable node identifier (e.g., 'file-1' in Docker, pod name in K8s). Required.")
	f.String("advertise_addr", "", "Address to advertise to peers (host:port). Env: ADVERTISE_ADDR. Required for cluster operation.")

	// Storage
	f.String("index_path", filepath.Join(os.TempDir(), "dir.idx"), "Path to store file index data")
	f.String("ec_scheme", "4+2", "Erasure coding scheme (e.g., '4+2', '8+4', '10+4')")
	f.Bool("direct_io", false, "Use O_DIRECT for disk writes (Linux only, bypasses page cache)")

	// Reconciliation configuration
	f.Duration("reconciliation_interval", 6*time.Hour, "How often to run chunk reconciliation with registry (0 = disabled)")
	f.Duration("reconciliation_grace_period", 2*time.Hour, "Grace period before deleting orphan chunks (protects in-flight uploads)")

	// TLS
	f.String("cert_file", "", "Path to TLS certificate file")
	f.String("key_file", "", "Path to TLS key file")

	// Manager registration
	f.String("manager_addr", "", "Manager service address (host:port) for cluster registration")

	viper.BindPFlags(f)
}

// getAdvertiseAddr returns the address to advertise for service registration.
// This follows the etcd/CockroachDB pattern where the advertise address must be
// explicitly configured for distributed deployments.
//
// Priority:
//  1. ADVERTISE_ADDR environment variable (for K8s/Docker injection)
//  2. --advertise_addr flag
//
// The address must be reachable from other nodes in the cluster.
// Examples:
//   - Docker Compose: "file-1:8001" (service name + port)
//   - Kubernetes: "file-0.file-headless.default.svc.cluster.local:8001" (pod FQDN)
func getAdvertiseAddr(flagValue string) string {
	// Environment variable takes precedence (allows K8s/Docker to inject)
	if addr := os.Getenv("ADVERTISE_ADDR"); addr != "" {
		return addr
	}
	return flagValue
}

func runFileServer(cmd *cobra.Command, args []string) {
	utils.LoadConfiguration("file", false)
	opts := loadFileOpts(cmd)

	debug.SetNotReady()

	// Create backend manager from configuration
	backendManager := backend.NewManager()
	var storeBackends []*types.Backend

	if len(opts.Backends) == 0 {
		// Fallback to default local backend if none configured
		backendID := "local-" + opts.NodeID
		if err := backendManager.Add(backendID, types.BackendConfig{
			Type:     types.StorageTypeLocal,
			Path:     filepath.Join(opts.IndexPath, "data"),
			DirectIO: opts.DirectIO,
		}); err != nil {
			logger.Fatal().Err(err).Msg("failed to create default local backend")
		}
		storeBackends = append(storeBackends, &types.Backend{
			ID:         backendID,
			Type:       types.StorageTypeLocal,
			TotalBytes: 100 * 1024 * 1024 * 1024, // 100GB placeholder
		})
		logger.Info().Str("backend_id", backendID).Msg("Using default local backend")
	} else {
		// Configure backends from TOML
		for _, b := range opts.Backends {
			if !b.Enabled {
				logger.Debug().Str("backend_id", b.ID).Msg("Skipping disabled backend")
				continue
			}

			cfg := types.BackendConfig{
				Type:      b.Type,
				Path:      b.Path,
				Endpoint:  b.Endpoint,
				Bucket:    b.Bucket,
				Region:    b.Region,
				AccessKey: b.AccessKey,
				SecretKey: b.SecretKey,
				DirectIO:  b.DirectIO,
			}

			if err := backendManager.Add(b.ID, cfg); err != nil {
				logger.Fatal().Err(err).Str("backend_id", b.ID).Msg("failed to create backend")
			}

			storeBackends = append(storeBackends, &types.Backend{
				ID:         b.ID,
				Type:       b.Type,
				Path:       b.Path,
				TotalBytes: 100 * 1024 * 1024 * 1024, // 100GB placeholder - could be configured
			})

			logger.Info().
				Str("backend_id", b.ID).
				Str("type", string(b.Type)).
				Str("path", b.Path).
				Msg("Configured storage backend")
		}
	}

	// Create file store
	cfg := store.Config{
		IndexPath: opts.IndexPath,
		Backends:  storeBackends,
		ECScheme:  opts.ECScheme,
	}

	logger.Info().
		Str("ec_scheme", opts.ECScheme.String()).
		Int("data_shards", opts.ECScheme.DataShards).
		Int("parity_shards", opts.ECScheme.ParityShards).
		Int("num_backends", len(storeBackends)).
		Msg("File server configuration")

	fileMux := http.NewServeMux()
	fs, err := file.NewFileServer(fileMux, cfg, backendManager)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create file server")
	}

	// Register with manager if manager address is provided
	var managerClient *client.ManagerClientPool
	var reconciliationSvc *file.ReconciliationService
	if opts.ManagerAddr != "" {
		managerClient = client.NewManagerClientPool(client.ManagerClientPoolConfig{
			SeedAddrs:      []string{opts.ManagerAddr},
			DialTimeout:    5 * time.Second,
			RequestTimeout: 10 * time.Second,
			MaxRetries:     3,
		})
		defer managerClient.Close()

		// Register file server with manager
		if err := registerFileServer(cmd.Context(), managerClient, fs, opts); err != nil {
			logger.Warn().Err(err).Msg("failed to register with manager (will retry via heartbeat)")
		} else {
			logger.Info().Msg("Successfully registered file server with manager")
		}

		// Start heartbeat goroutine
		go heartbeatFileServer(cmd.Context(), managerClient, fs, opts)

		// Start reconciliation service if enabled
		if opts.ReconciliationInterval > 0 {
			reconciliationSvc = file.NewReconciliationService(file.ReconciliationConfig{
				ServerID:    opts.AdvertiseAddr,
				GracePeriod: opts.ReconciliationGracePeriod,
				Interval:    opts.ReconciliationInterval,
			}, managerClient, fs.Store())
			reconciliationSvc.Start(cmd.Context())
			logger.Info().
				Dur("interval", opts.ReconciliationInterval).
				Dur("grace_period", opts.ReconciliationGracePeriod).
				Msg("Started chunk reconciliation service")
		}
	} else {
		logger.Warn().Msg("No manager address provided - file server will not register (replication may not work)")
	}

	// Parse bind address to get host for HTTP servers
	bindHost, _, err := net.SplitHostPort(opts.BindAddr)
	if err != nil {
		logger.Fatal().Err(err).Str("bind_addr", opts.BindAddr).Msg("invalid bind_addr format, expected host:port")
	}

	// Register admin handlers on debug mux (must be before GetMux())
	fs.RegisterAdminHandlers()

	// Start servers
	httpServer := startHTTPServer(fileMux, bindHost, opts.HTTPPort)
	debugServer := startHTTPServer(debug.GetMux(), bindHost, opts.DebugPort)
	grpcServer := startFileGRPCServer(opts, fs)

	debug.SetReady()

	waitForShutdown()

	debug.SetNotReady()
	if reconciliationSvc != nil {
		reconciliationSvc.Stop()
	}
	grpcServer.GracefulStop()
	httpServer.Shutdown(cmd.Context())
	debugServer.Shutdown(cmd.Context())
	fs.Shutdown()
}

func loadFileOpts(cmd *cobra.Command) FileServerOpts {
	f := NewFlagLoader(cmd)

	// Parse EC scheme
	ecSchemeStr := f.String("ec_scheme")
	ecScheme, err := types.ParseECScheme(ecSchemeStr)
	if err != nil {
		logger.Fatal().Err(err).Str("ec_scheme", ecSchemeStr).Msg("invalid EC scheme")
	}

	// Load backends from TOML configuration
	backends := loadBackendOpts()

	// Get node_id (required for cluster operation)
	nodeID := f.String("node_id")
	if nodeID == "" {
		// Try environment variable as fallback
		nodeID = os.Getenv("NODE_ID")
	}
	if nodeID == "" {
		logger.Fatal().Msg("--node_id is required. Set via flag, config, or NODE_ID env var.")
	}

	// Get advertise address (required for cluster operation)
	advertiseAddr := getAdvertiseAddr(f.String("advertise_addr"))
	if advertiseAddr == "" {
		logger.Fatal().Msg("--advertise_addr is required. Set via flag, config, or ADVERTISE_ADDR env var. Example: 'file-1:8001'")
	}

	return FileServerOpts{
		BindAddr:                  f.String("bind_addr"),
		HTTPPort:                  f.Int("http_port"),
		DebugPort:                 f.Int("debug_port"),
		NodeID:                    nodeID,
		AdvertiseAddr:             advertiseAddr,
		IndexPath:                 f.String("index_path"),
		ECScheme:                  ecScheme,
		DirectIO:                  f.Bool("direct_io"),
		Backends:                  backends,
		ReconciliationInterval:    f.Duration("reconciliation_interval"),
		ReconciliationGracePeriod: f.Duration("reconciliation_grace_period"),
		CertFile:                  f.String("cert_file"),
		KeyFile:                   f.String("key_file"),
		ManagerAddr:               f.String("manager_addr"),
	}
}

// loadBackendOpts parses backend configuration from TOML [backends.*] sections
func loadBackendOpts() []BackendOpts {
	var backends []BackendOpts

	// Get backends map from viper
	backendsMap := viper.GetStringMap("backends")
	if len(backendsMap) == 0 {
		return backends
	}

	for id := range backendsMap {
		prefix := "backends." + id + "."

		typeStr := viper.GetString(prefix + "type")
		storageType := types.StorageType(typeStr)
		if storageType == "" {
			storageType = types.StorageTypeLocal
		}

		backend := BackendOpts{
			ID:           id,
			Type:         storageType,
			Path:         viper.GetString(prefix + "path"),
			Endpoint:     viper.GetString(prefix + "endpoint"),
			Bucket:       viper.GetString(prefix + "bucket"),
			Region:       viper.GetString(prefix + "region"),
			AccessKey:    viper.GetString(prefix + "access_key"),
			SecretKey:    viper.GetString(prefix + "secret_key"),
			StorageClass: viper.GetString(prefix + "storage_class"),
			Enabled:      viper.GetBool(prefix + "enabled"),
			MinFreeSpace: viper.GetString(prefix + "min_free_space"),
		}

		backends = append(backends, backend)
		logger.Debug().
			Str("id", id).
			Str("type", typeStr).
			Bool("enabled", backend.Enabled).
			Msg("Loaded backend configuration")
	}

	return backends
}

func startFileGRPCServer(opts FileServerOpts, fs file_pb.FileServiceServer) *grpc.Server {
	listener, err := utils.NewListener(opts.BindAddr, 0)
	if err != nil {
		logger.Fatal().Err(err).Str("bind_addr", opts.BindAddr).Msg("failed to create gRPC listener")
	}

	grpcOpts := loadTLSServerOpts(opts.CertFile, opts.KeyFile)
	grpcServer := proto.NewGRPCServer(grpcOpts...)
	reflection.Register(grpcServer)
	file_pb.RegisterFileServiceServer(grpcServer, fs)

	go func() {
		logger.Info().
			Str("bind_addr", opts.BindAddr).
			Str("advertise_addr", opts.AdvertiseAddr).
			Msg("Starting file gRPC server")
		if err := grpcServer.Serve(listener); err != nil {
			logger.Fatal().Err(err).Msg("failed to start gRPC server")
		}
	}()

	return grpcServer
}

// registerFileServer registers the file server with the manager
func registerFileServer(ctx context.Context, managerClient *client.ManagerClientPool, fs *file.FileServer, opts FileServerOpts) error {
	// Get backend information from file server
	statusCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	statusResp, err := fs.FileServerStatus(statusCtx, &file_pb.FileServerStatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to get file server status: %w", err)
	}

	// Convert backends to StorageBackend format
	// Group backends by type for StorageBackend
	storageBackends := make(map[string]*manager_pb.StorageBackend)
	for _, backend := range statusResp.Backends {
		backendType := backend.Type
		if backendType == "" {
			backendType = "local"
		}

		sb, exists := storageBackends[backendType]
		if !exists {
			sb = &manager_pb.StorageBackend{
				Type:     backendType,
				Id:       backendType + "-" + opts.NodeID,
				Backends: []*common_pb.Backend{},
			}
			storageBackends[backendType] = sb
		}

		// Add backend to the storage backend
		sb.Backends = append(sb.Backends, backend)
	}

	// Convert map to slice
	var storageBackendsList []*manager_pb.StorageBackend
	for _, sb := range storageBackends {
		storageBackendsList = append(storageBackendsList, sb)
	}

	// Build registration request using advertised address
	// This must be reachable from other nodes (e.g., "file-1:8001" in Docker)
	location := &common_pb.Location{
		Address: opts.AdvertiseAddr,
		Node:    opts.NodeID,
	}

	req := &manager_pb.RegisterServiceRequest{
		ServiceType: manager_pb.ServiceType_FILE_SERVICE,
		Location:    location,
		ServiceMetadata: &manager_pb.RegisterServiceRequest_FileService{
			FileService: &manager_pb.FileServiceMetadata{
				StorageBackends: storageBackendsList,
			},
		},
	}

	// Register with manager
	resp, err := managerClient.RegisterService(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.Message)
	}

	logger.Info().
		Str("manager", opts.ManagerAddr).
		Str("advertise_addr", opts.AdvertiseAddr).
		Str("node_id", opts.NodeID).
		Int("backends", len(storageBackendsList)).
		Uint64("topology_version", resp.Version).
		Msg("File server registered with manager")

	return nil
}

// heartbeatFileServer sends periodic heartbeats to the manager
func heartbeatFileServer(ctx context.Context, managerClient *client.ManagerClientPool, fs *file.FileServer, opts FileServerOpts) {
	ticker := time.NewTicker(30 * time.Second) // Heartbeat every 30 seconds
	defer ticker.Stop()

	var topologyVersion uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get current backend status
			statusCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			statusResp, err := fs.FileServerStatus(statusCtx, &file_pb.FileServerStatusRequest{})
			cancel()

			if err != nil {
				logger.Warn().Err(err).Msg("failed to get file server status for heartbeat")
				continue
			}

			// Convert backends to StorageBackend format (same as registration)
			storageBackends := make(map[string]*manager_pb.StorageBackend)
			for _, backend := range statusResp.Backends {
				backendType := backend.Type
				if backendType == "" {
					backendType = "local"
				}

				sb, exists := storageBackends[backendType]
				if !exists {
					sb = &manager_pb.StorageBackend{
						Type:     backendType,
						Id:       backendType + "-" + opts.NodeID,
						Backends: []*common_pb.Backend{},
					}
					storageBackends[backendType] = sb
				}

				sb.Backends = append(sb.Backends, backend)
			}

			var storageBackendsList []*manager_pb.StorageBackend
			for _, sb := range storageBackends {
				storageBackendsList = append(storageBackendsList, sb)
			}

			// Build heartbeat request using advertised address
			location := &common_pb.Location{
				Address: opts.AdvertiseAddr,
				Node:    opts.NodeID,
			}

			req := &manager_pb.HeartbeatRequest{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Location:    location,
				Version:     topologyVersion,
				ServiceMetadata: &manager_pb.HeartbeatRequest_FileService{
					FileService: &manager_pb.FileServiceMetadata{
						StorageBackends: storageBackendsList,
					},
				},
			}

			// Send heartbeat
			heartbeatCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			resp, err := managerClient.Heartbeat(heartbeatCtx, req)
			cancel()

			if err != nil {
				logger.Warn().Err(err).Msg("failed to send heartbeat to manager")
				continue
			}

			if resp.TopologyChanged {
				logger.Info().
					Uint64("old_version", topologyVersion).
					Uint64("new_version", resp.TopologyVersion).
					Msg("Topology changed, re-registering")
				topologyVersion = resp.TopologyVersion

				// Re-register if topology changed
				if err := registerFileServer(ctx, managerClient, fs, opts); err != nil {
					logger.Warn().Err(err).Msg("failed to re-register after topology change")
				}
			} else {
				topologyVersion = resp.TopologyVersion
			}
		}
	}
}

func startHTTPServer(handler http.Handler, ip string, port int) *http.Server {
	listener, err := utils.NewListener(utils.JoinHostPort(ip, port), 0)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create HTTP listener")
	}

	httpServer := &http.Server{Handler: handler}
	go func() {
		logger.Info().Str("http_addr", utils.JoinHostPort(ip, port)).Msg("Starting HTTP server")
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("failed to start HTTP server")
		}
	}()
	return httpServer
}

func waitForShutdown() {
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGALRM, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-stopChan
}

func loadTLSServerOpts(certFile, keyFile string) []grpc.ServerOption {
	tlsOpt, err := utils.GetServerOption(certFile, keyFile)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to load TLS credentials")
	}
	if tlsOpt != nil {
		logger.Info().Msg("gRPC server using TLS")
		return []grpc.ServerOption{tlsOpt}
	}
	return nil
}
