// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/debug"
	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/manager"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
	"github.com/LeeDigitalWorks/zapfs/proto"
	"github.com/LeeDigitalWorks/zapfs/proto/iam_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// ManagerServerOpts holds configuration for the manager server.
//
// # Port Configuration
//
// The manager uses two ports:
//   - grpc_port (default 8050): For gRPC API (cluster membership, placement queries)
//   - raft_addr (default grpc_port+1): For Raft peer-to-peer consensus
//
// # Raft Networking
//
// Raft nodes communicate via raft_addr for consensus. When joining a cluster:
//   - --join flag takes the LEADER'S gRPC address (e.g., manager-1:8050)
//   - --raft_addr is THIS node's Raft peer address (e.g., manager-2:8051)
//
// The join RPC tells the leader to add this node's raft_addr to the cluster.
type ManagerServerOpts struct {
	IP        string
	GRPCPort  int
	DebugPort int
	AdminPort int // Admin HTTP port for IAM management (default: grpc_port+10)
	CertFile  string
	KeyFile   string
	LogLevel  string

	// Raft configuration
	NodeID          string
	RaftDir         string
	RaftBindAddr    string        // Address for Raft peer communication (default: IP:grpc_port+1)
	Bootstrap       bool          // Bootstrap a new cluster (only one node should do this)
	BootstrapExpect int           // Expected cluster size
	JoinAddr        string        // gRPC address of leader to join (e.g., manager-1:8050)
	LeaderTimeout   time.Duration // Time to wait for leader election

	RegionID string

	// Placement defaults
	DefaultNumReplicas uint32 // Default replication factor when not specified per-request
}

var managerCmd = &cobra.Command{
	Use:   "manager",
	Short: "Start manager server (internal control plane)",
	Long: `Start a ZapFS manager server that handles:
- Service registry (file and metadata services)
- Placement decisions (where to store chunks)
- Raft consensus for high availability
- Admin operations (rebalancing, cluster management)
`,
	Run: runManagerServer,
}

func init() {
	rootCmd.AddCommand(managerCmd)

	f := managerCmd.Flags()
	f.String("ip", utils.DetectedHostAddress(), "IP address to bind to")
	f.Int("grpc_port", 8050, "gRPC port for manager server")
	f.Int("debug_port", 8055, "Debug HTTP port (metrics, pprof)")
	f.Int("admin_port", 8060, "Admin HTTP port for IAM management")
	f.String("cert_file", "", "Path to TLS certificate file")
	f.String("key_file", "", "Path to TLS key file")
	f.String("log_level", "info", "Log level (debug, info, warn, error, fatal)")
	f.String("node_id", "", "Unique node ID (defaults to hostname)")
	f.String("raft_dir", "/tmp/raft", "Raft data directory")
	f.String("raft_addr", "", "Raft bind address for peer communication (default: ip:grpc_port+1)")
	f.Duration("leader_timeout", 5*time.Second, "Time to wait for leader election")
	f.Bool("bootstrap", false, "Bootstrap a new Raft cluster")
	f.Int("bootstrap_expect", 1, "Expected number of servers in cluster")
	f.String("join", "", "gRPC address of cluster leader to join (NOT raft_addr)")
	f.String("region_id", "default", "Region ID for this manager")
	f.Uint32("default_num_replicas", 3, "Default replication factor when not specified per-request")

	// Backup scheduling flags (Enterprise feature: FeatureBackup)
	f.Bool("backup_enabled", false, "Enable automatic backup scheduling")
	f.Duration("backup_interval", 24*time.Hour, "Interval between backups")
	f.String("backup_dir", "/var/lib/zapfs/backups", "Directory to store backups")
	f.Int("backup_retain_count", 7, "Number of backups to retain")
	f.Int("backup_retain_days", 30, "Days to retain backups")
	f.String("backup_prefix", "manager", "Prefix for backup files")

	// LDAP integration flags (Enterprise feature: FeatureLDAP)
	f.String("ldap_url", "", "LDAP server URL (ldap://host:389 or ldaps://host:636)")
	f.String("ldap_bind_dn", "", "LDAP bind DN (cn=admin,dc=example,dc=com)")
	f.String("ldap_bind_pass", "", "LDAP bind password")
	f.String("ldap_base_dn", "", "LDAP base DN for user searches (dc=example,dc=com)")
	f.String("ldap_user_filter", "(uid=%s)", "LDAP user search filter")
	f.String("ldap_username_attr", "uid", "LDAP username attribute")
	f.String("ldap_email_attr", "mail", "LDAP email attribute")
	f.String("ldap_group_attr", "memberOf", "LDAP group membership attribute")
	f.String("ldap_required_group", "", "Required LDAP group for access (optional)")
	f.Bool("ldap_start_tls", false, "Use StartTLS for LDAP connection")
	f.Int("ldap_pool_size", 5, "LDAP connection pool size")
	f.Duration("ldap_timeout", 10*time.Second, "LDAP connection timeout")

	// OIDC integration flags (Enterprise feature: FeatureOIDC)
	f.String("oidc_issuer", "", "OIDC provider issuer URL (e.g., https://accounts.google.com)")
	f.String("oidc_client_id", "", "OIDC client ID")
	f.String("oidc_client_secret", "", "OIDC client secret (optional for public clients)")
	f.String("oidc_redirect_url", "", "OIDC callback URL (e.g., http://localhost:8060/v1/oidc/callback)")
	f.StringSlice("oidc_scopes", []string{"openid", "email", "profile"}, "OIDC scopes to request")
	f.String("oidc_username_claim", "email", "OIDC claim to use as username")
	f.String("oidc_groups_claim", "", "OIDC claim containing group memberships")
	f.StringSlice("oidc_required_groups", nil, "Required OIDC groups for access")
	f.StringSlice("oidc_allowed_domains", nil, "Allowed email domains for OIDC users")

	viper.BindPFlags(f)
}

func runManagerServer(cmd *cobra.Command, args []string) {
	utils.LoadConfiguration("manager", false)
	utils.LoadConfiguration("iam", false)
	opts := loadManagerOpts(cmd)

	debug.SetNotReady()

	if level, err := zerolog.ParseLevel(opts.LogLevel); err == nil {
		logger.SetLevel(level)
	}

	if err := os.MkdirAll(opts.RaftDir, 0755); err != nil {
		logger.Fatal().Err(err).Msg("failed to create raft directory")
	}

	raftConfig := &manager.Config{
		NodeID:          opts.NodeID,
		BindAddr:        opts.RaftBindAddr,
		DataDir:         opts.RaftDir,
		Bootstrap:       opts.Bootstrap,
		BootstrapExpect: opts.BootstrapExpect,
	}

	// Check for existing Raft state BEFORE creating the Raft node
	// This is important because NewManagerServer creates the raft.db file
	hasExistingState := manager.HasExistingRaftState(opts.RaftDir)

	logger.Info().
		Str("node_id", raftConfig.NodeID).
		Str("raft_addr", raftConfig.BindAddr).
		Str("raft_dir", raftConfig.DataDir).
		Bool("bootstrap", raftConfig.Bootstrap).
		Bool("has_existing_state", hasExistingState).
		Str("region", opts.RegionID).
		Msg("Starting manager server")

	// Initialize IAM service first
	iamService, oidcHandler, err := initializeManagerIAM()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize IAM service")
	}
	logger.Info().
		Int("cache_size", iamService.Manager().CacheSize()).
		Bool("sts_enabled", iamService.STS() != nil).
		Bool("kms_enabled", iamService.KMS() != nil).
		Bool("oidc_enabled", oidcHandler != nil).
		Msg("IAM service initialized")

	// Get license checker for limit enforcement (noopChecker for community edition)
	licenseChecker := license.GetChecker()

	managerServer, err := manager.NewManagerServer(opts.RegionID, raftConfig, opts.LeaderTimeout, iamService, licenseChecker, opts.DefaultNumReplicas)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create manager server")
	}

	// Configure multi-region if specified (enterprise feature)
	if viper.IsSet("region") {
		var regionConfig manager.RegionConfig
		if err := viper.UnmarshalKey("region", &regionConfig); err != nil {
			logger.Fatal().Err(err).Msg("failed to parse region config")
		}
		if regionConfig.IsConfigured() {
			if err := managerServer.ConfigureMultiRegion(&regionConfig); err != nil {
				logger.Fatal().Err(err).Msg("failed to configure multi-region")
			}
			logger.Info().
				Str("region", regionConfig.Name).
				Strs("primary_regions", regionConfig.PrimaryRegions).
				Int("peers", len(regionConfig.Peers)).
				Msg("Multi-region configured")
		}
	}

	// Configure backup scheduling (enterprise feature: FeatureBackup)
	backupConfig := manager.BackupSchedulerConfig{
		Enabled:        viper.GetBool("backup_enabled"),
		Interval:       viper.GetDuration("backup_interval"),
		DestinationDir: viper.GetString("backup_dir"),
		RetainCount:    viper.GetInt("backup_retain_count"),
		RetainDays:     viper.GetInt("backup_retain_days"),
		Prefix:         viper.GetString("backup_prefix"),
	}
	if err := managerServer.ConfigureBackupScheduler(backupConfig); err != nil {
		logger.Fatal().Err(err).Msg("failed to configure backup scheduler")
	}

	// Join existing cluster if specified AND this is a fresh node (no existing Raft state)
	// On restart, nodes already have their Raft configuration and just need to participate in election
	if opts.JoinAddr != "" && !opts.Bootstrap {
		if hasExistingState {
			logger.Info().
				Str("raft_dir", opts.RaftDir).
				Str("join_addr", opts.JoinAddr).
				Msg("Found existing Raft state, skipping join (node is restarting)")
		} else {
			logger.Info().Str("join_addr", opts.JoinAddr).Msg("Attempting to join existing cluster")
			if err := joinCluster(opts, opts.JoinAddr); err != nil {
				logger.Fatal().Err(err).Msg("failed to join cluster")
			}
		}
	}

	// Register custom readiness check - service is only ready when cluster has a leader
	debug.SetReadyCheck(func() bool {
		return managerServer.IsClusterReady()
	})

	grpcServer := startManagerGRPCServer(opts, managerServer)
	debugServer := startHTTPServer(debug.GetMux(), opts.IP, opts.DebugPort)
	adminServer := startManagerAdminServer(opts, managerServer, oidcHandler)

	logger.Info().
		Str("grpc_addr", fmt.Sprintf("%s:%d", opts.IP, opts.GRPCPort)).
		Str("debug_addr", fmt.Sprintf("%s:%d", opts.IP, opts.DebugPort)).
		Str("admin_addr", fmt.Sprintf("%s:%d", opts.IP, opts.AdminPort)).
		Msg("Manager server started, waiting for cluster to be ready...")

	// Wait for cluster to have a leader before marking ready
	// Use a longer timeout for initial cluster formation
	clusterTimeout := 30 * time.Second
	if opts.Bootstrap {
		// Bootstrap node should become leader quickly
		clusterTimeout = 10 * time.Second
	}

	if err := managerServer.WaitForClusterReady(clusterTimeout); err != nil {
		logger.Warn().Err(err).Msg("Cluster not ready yet, continuing startup (readiness probe will fail until leader elected)")
	} else {
		state := managerServer.GetClusterState()
		logger.Info().
			Bool("is_leader", state["is_leader"].(bool)).
			Str("leader", state["leader"].(string)).
			Str("state", state["state"].(string)).
			Msg("Cluster is ready")
	}

	debug.SetReady()
	waitForShutdown()
	debug.SetNotReady()

	logger.Info().Msg("Shutting down manager server")
	grpcServer.GracefulStop()
	debugServer.Shutdown(cmd.Context())
	adminServer.Shutdown(cmd.Context())
	managerServer.Shutdown()
	logger.Info().Msg("Manager server stopped")
}

func loadManagerOpts(cmd *cobra.Command) ManagerServerOpts {
	f := NewFlagLoader(cmd)
	opts := ManagerServerOpts{
		IP:                 f.String("ip"),
		GRPCPort:           f.Int("grpc_port"),
		DebugPort:          f.Int("debug_port"),
		AdminPort:          f.Int("admin_port"),
		CertFile:           f.String("cert_file"),
		KeyFile:            f.String("key_file"),
		LogLevel:           f.String("log_level"),
		NodeID:             f.String("node_id"),
		RaftDir:            f.String("raft_dir"),
		RaftBindAddr:       f.String("raft_addr"),
		Bootstrap:          f.Bool("bootstrap"),
		BootstrapExpect:    f.Int("bootstrap_expect"),
		JoinAddr:           f.String("join"),
		RegionID:           f.String("region_id"),
		LeaderTimeout:      f.Duration("leader_timeout"),
		DefaultNumReplicas: f.Uint32("default_num_replicas"),
	}

	// Set defaults
	if opts.NodeID == "" {
		hostname, _ := os.Hostname()
		opts.NodeID = hostname
	}
	if opts.RaftBindAddr == "" {
		opts.RaftBindAddr = fmt.Sprintf("%s:%d", opts.IP, opts.GRPCPort+1)
	}

	return opts
}

func initializeManagerIAM() (*iam.Service, http.Handler, error) {
	// Check for LDAP configuration first (Enterprise feature)
	// Support both CLI flag (ldap_url) and TOML config ([ldap] url = ...)
	ldapURL := viper.GetString("ldap_url")
	if ldapURL == "" {
		ldapURL = viper.GetString("ldap.url")
	}
	if ldapURL != "" {
		svc, err := initializeLDAPBackedIAM(ldapURL)
		return svc, nil, err // LDAP doesn't have HTTP handlers
	}

	// Check for OIDC configuration (Enterprise feature)
	// Support both CLI flag (oidc_issuer) and TOML config ([oidc] issuer = ...)
	oidcIssuer := viper.GetString("oidc_issuer")
	if oidcIssuer == "" {
		oidcIssuer = viper.GetString("oidc.issuer")
	}
	if oidcIssuer != "" {
		return initializeOIDCBackedIAM(oidcIssuer)
	}

	// Try to load IAM config from Viper (which may have loaded from TOML)
	var iamCfg iam.IAMConfig

	// Check if we have IAM config in Viper
	if viper.IsSet("iam") {
		if err := viper.UnmarshalKey("iam", &iamCfg); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal IAM config: %w", err)
		}
		logger.Info().Int("users", len(iamCfg.Users)).Int("groups", len(iamCfg.Groups)).Msg("loaded IAM config from file")
		svc, err := iam.LoadFromConfig(iamCfg)
		return svc, nil, err
	}

	// Fall back to defaults
	logger.Warn().Msg("no IAM config found, using defaults (test credentials)")
	svc, err := iam.NewServiceWithDefaults()
	return svc, nil, err
}

// initializeLDAPBackedIAM is defined in manager_ldap_enterprise.go (enterprise) or
// manager_ldap_stub.go (community). See those files for implementation.

func startManagerAdminServer(opts ManagerServerOpts, ms *manager.ManagerServer, oidcHandler http.Handler) *http.Server {
	mux := http.NewServeMux()

	// Mount IAM admin handlers (uses ManagerServer to notify subscribers on changes)
	iamHandler := manager.NewIAMAdminHandler(ms.GetIAMService())
	iamHandler.SetNotifier(ms) // ManagerServer implements CredentialNotifier
	iamHandler.SetRaftStore(ms.GetRaftCredentialStore()) // Use Raft for mutations (Phase 2)
	mux.Handle("/v1/iam/", iamHandler)

	// Register OIDC handlers if configured (enterprise feature)
	registerOIDCHandlers(mux, oidcHandler)

	// Health and status endpoints
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})

	// Cluster state endpoint - useful for debugging readiness issues
	mux.HandleFunc("GET /v1/cluster", func(w http.ResponseWriter, r *http.Request) {
		state := ms.GetClusterState()
		state["ready"] = ms.IsClusterReady()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(state)
	})

	addr := utils.JoinHostPort(opts.IP, opts.AdminPort)
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		logger.Info().Str("admin_addr", addr).Msg("Manager admin HTTP server listening")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("failed to start admin server")
		}
	}()

	return server
}

func startManagerGRPCServer(opts ManagerServerOpts, ms *manager.ManagerServer) *grpc.Server {
	listener, err := utils.NewListener(utils.JoinHostPort(opts.IP, opts.GRPCPort), 0)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create gRPC listener")
	}

	grpcOpts := loadTLSServerOpts(opts.CertFile, opts.KeyFile)
	grpcServer := proto.NewGRPCServer(grpcOpts...)
	reflection.Register(grpcServer)
	manager_pb.RegisterManagerServiceServer(grpcServer, ms)
	iam_pb.RegisterIAMServiceServer(grpcServer, ms)

	go func() {
		logger.Info().Str("grpc_addr", listener.Addr().String()).Msg("Manager gRPC server listening (Manager + IAM services)")
		if err := grpcServer.Serve(listener); err != nil {
			logger.Fatal().Err(err).Msg("failed to start gRPC server")
		}
	}()

	return grpcServer
}

// joinCluster connects to the leader's gRPC port and requests to join the Raft cluster.
// The leader will then communicate with this node via its raft_addr.
func joinCluster(opts ManagerServerOpts, leaderGRPCAddr string) error {
	logger.Info().
		Str("leader_grpc", leaderGRPCAddr).
		Str("node_id", opts.NodeID).
		Str("raft_addr", opts.RaftBindAddr).
		Msg("Connecting to leader via gRPC to join cluster")

	dialOpt, err := utils.GetServerDialOption(opts.CertFile, opts.KeyFile, "")
	if err != nil {
		return fmt.Errorf("failed to create TLS config: %w", err)
	}

	client, err := proto.NewManagerClient(leaderGRPCAddr, false, 1, dialOpt)
	if err != nil {
		return fmt.Errorf("failed to create manager client: %w", err)
	}

	resp, err := client.RaftAddServer(context.Background(), &manager_pb.RaftAddServerRequest{
		Id:      opts.NodeID,
		Address: opts.RaftBindAddr,
		IsVoter: true,
	})
	if err != nil {
		return fmt.Errorf("failed to add server to cluster: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("failed to join cluster: %s", resp.Message)
	}

	logger.Info().Str("leader", leaderGRPCAddr).Str("node_id", opts.NodeID).Msg("Successfully joined Raft cluster")
	return nil
}
