// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"os"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/cache"
	"github.com/LeeDigitalWorks/zapfs/pkg/debug"
	"github.com/LeeDigitalWorks/zapfs/pkg/env"
	"github.com/LeeDigitalWorks/zapfs/pkg/events"
	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/api"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db/postgres"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db/vitess"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/filter"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/placer"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
	"github.com/LeeDigitalWorks/zapfs/proto"
	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/metadata_pb"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type MetadataServerOpts struct {
	IP        string
	HTTPPort  int
	GRPCPort  int
	DebugPort int
	CertFile  string
	KeyFile   string
	LogLevel  string

	// Service identity for manager registration
	NodeID        string
	AdvertiseAddr string

	ManagerAddr string
	S3Domains   []string
	RegionID    string

	PoolsConfig    string
	ProfilesConfig string
	DataDir        string

	DBDriver       string
	DBDSN          string
	DBMaxOpenConns int
	DBMaxIdleConns int
	DBTLSMode      string
	DBTLSCAFile    string

	// Rate limiting
	RateLimitEnabled        bool
	RateLimitBurstMultipler int64
	RateLimitRedisEnabled   bool
	RateLimitRedisAddr      string
	RateLimitRedisPassword  string
	RateLimitRedisDB        int
	RateLimitRedisPoolSize  int
	RateLimitRedisFailOpen  bool

	// Access logging (enterprise: FeatureAccessLog)
	AccessLogsEnabled       bool
	ClickHouseDSN           string
	AccessLogBatchSize      int
	AccessLogFlushInterval  time.Duration
	AccessLogExportInterval time.Duration

	// Lifecycle scanner (community feature)
	LifecycleScannerEnabled  bool
	LifecycleScanInterval    time.Duration
	LifecycleScanConcurrency int
	LifecycleScanBatchSize   int
	LifecycleMaxTasksPerScan int

	// Cross-region replication (enterprise: FeatureMultiRegion)
	ReplicationAccessKeyID     string
	ReplicationSecretAccessKey string
}

var metadataCmd = &cobra.Command{
	Use:   "metadata",
	Short: "Start metadata server",
	Long: `Start a ZapFS metadata server that handles:
- S3-compatible metadata API
- Bucket and object metadata management
- Interaction with manager server for placement decisions`,
	Run: runMetadataServer,
}

func init() {
	rootCmd.AddCommand(metadataCmd)

	f := metadataCmd.Flags()
	f.String("ip", utils.DetectedHostAddress(), "IP address to bind to")
	f.Int("http_port", 8082, "HTTP port for metadata server")
	f.Int("grpc_port", 8083, "gRPC port for metadata server")
	f.Int("debug_port", 8085, "Debug HTTP port for metadata server")
	f.String("cert_file", "", "Path to TLS certificate file")
	f.String("key_file", "", "Path to TLS key file")
	f.String("log_level", "info", "Log level (debug, info, warn, error, fatal)")

	// Service identity for manager registration
	f.String("node_id", "", "Stable node identifier (e.g., 'metadata-1' in Docker, pod name in K8s)")
	f.String("advertise_addr", "", "Address to advertise to peers (host:port). Env: ADVERTISE_ADDR")

	f.String("manager_addr", "localhost:8050", "Manager server gRPC address")
	f.StringSlice("s3_domains", []string{"localhost"}, "S3 domain names for virtual-hosted style")
	f.String("region_id", "us-west", "Region ID for this metadata server")
	f.String("pools_config", "", "Path to storage pools JSON config file")
	f.String("profiles_config", "", "Path to storage profiles JSON config file")
	f.String("data_dir", "/tmp/zapfs/data", "Base directory for local storage backends")
	f.String("db_driver", "vitess", "Database driver (vitess, mysql, postgres, cockroachdb)")
	f.String("db_dsn", "", "Database connection string")
	f.Int("db_max_open_conns", 25, "Maximum open database connections")
	f.Int("db_max_idle_conns", 5, "Maximum idle database connections")
	f.String("db_tls_mode", "", "Database TLS mode (disabled, preferred, required, verify-ca)")
	f.String("db_tls_ca_file", "", "Path to CA certificate file for database TLS (verify-ca mode)")

	// Rate limiting
	f.Bool("rate_limit_enabled", true, "Enable request rate limiting")
	f.Int64("rate_limit_burst_multiplier", 2, "Burst multiplier for rate limiting")
	f.Bool("rate_limit_redis_enabled", false, "Enable distributed rate limiting via Redis")
	f.String("rate_limit_redis_addr", "localhost:6379", "Redis address for distributed rate limiting")
	f.String("rate_limit_redis_password", "", "Redis password")
	f.Int("rate_limit_redis_db", 0, "Redis database number")
	f.Int("rate_limit_redis_pool_size", 10, "Redis connection pool size")
	f.Bool("rate_limit_redis_fail_open", true, "Allow requests when Redis is unavailable")

	// Access logging (enterprise: FeatureAccessLog)
	f.Bool("access_logs_enabled", false, "Enable access logging (enterprise)")
	f.String("clickhouse_dsn", "", "ClickHouse DSN for access logs")
	f.Int("access_log_batch_size", 10000, "Batch size for access log inserts")
	f.Duration("access_log_flush_interval", 5*time.Second, "Access log flush interval")
	f.Duration("access_log_export_interval", time.Hour, "Access log S3 export interval")

	// Lifecycle scanner (community feature)
	f.Bool("lifecycle_scanner_enabled", false, "Enable lifecycle policy scanning")
	f.Duration("lifecycle_scan_interval", time.Hour, "How often to scan buckets for lifecycle rules")
	f.Int("lifecycle_scan_concurrency", 5, "Number of buckets to scan in parallel")
	f.Int("lifecycle_scan_batch_size", 1000, "Objects per batch when listing")
	f.Int("lifecycle_max_tasks_per_scan", 10000, "Max tasks to enqueue per scan run")

	// Cross-region replication (enterprise: FeatureMultiRegion)
	f.String("replication_access_key_id", "", "Access key for cross-region replication")
	f.String("replication_secret_access_key", "", "Secret key for cross-region replication (use env var ZAPFS_REPLICATION_SECRET_ACCESS_KEY)")

	viper.BindPFlags(f)
}

func runMetadataServer(cmd *cobra.Command, args []string) {
	utils.LoadConfiguration("metadata", false)
	opts := loadMetadataOpts(cmd)

	debug.SetNotReady()

	// Manager client pool with automatic retry and failover
	managerClient := client.NewManagerClientPool(client.ManagerClientPoolConfig{
		SeedAddrs:      []string{opts.ManagerAddr},
		DialTimeout:    5 * time.Second,
		RequestTimeout: 10 * time.Second,
		MaxRetries:     3,
	})

	globalBucketCache := cache.NewGlobalBucketCache(cmd.Context(), managerClient)
	go globalBucketCache.LoadBuckets(cmd.Context(), time.Minute)

	// Local bucket cache
	bucketsCache := cache.New(cmd.Context(), cache.WithMaxSize[string, s3types.Bucket](1_000_000))
	bucketStore := cache.NewBucketStore(bucketsCache)

	// IAM service initialization (syncs from manager in remote mode)
	iamService, err := initializeIAM(cmd.Context(), opts)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize IAM service")
	}
	logger.Info().
		Int("cache_size", iamService.Manager().CacheSize()).
		Bool("sts_enabled", iamService.STS() != nil).
		Bool("kms_enabled", iamService.KMS() != nil).
		Msg("IAM service initialized")

	// Register debug endpoint for IAM cache dump
	debug.RegisterHandlerFunc("/debug/iam/cache", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		entries := iamService.Manager().DumpCache()
		response := map[string]any{
			"cache_size": len(entries),
			"entries":    entries,
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	// Register debug endpoint for bucket cache dump
	debug.RegisterHandlerFunc("/debug/buckets/cache", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		entries := bucketStore.DumpCache()
		response := map[string]any{
			"cache_size": len(entries),
			"is_ready":   bucketStore.IsReady(),
			"entries":    entries,
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	// Filter chain using IAM service
	chain := filter.NewChain()
	chain.AddFilter(filter.NewRequestIDFilter())
	chain.AddFilter(filter.NewParserFilter(opts.S3Domains...))
	chain.AddFilter(filter.NewValidationFilter(globalBucketCache))
	chain.AddFilter(filter.NewAuthenticationFilter(iamService.Manager()))
	chain.AddFilter(filter.NewAuthorizationFilter(filter.AuthorizationConfig{
		PolicyStore:  bucketStore,
		ACLStore:     bucketStore,
		IAMEvaluator: iamService.Evaluator(),
	}))
	if opts.RateLimitEnabled && !env.IsLocal() {
		rateLimitCfg := filter.DefaultRateLimitConfig()
		rateLimitCfg.BurstMultiplier = opts.RateLimitBurstMultipler

		// Configure Redis for distributed rate limiting
		if opts.RateLimitRedisEnabled {
			rateLimitCfg.Redis = filter.RedisRateLimitConfig{
				Enabled:  true,
				Addr:     opts.RateLimitRedisAddr,
				Password: opts.RateLimitRedisPassword,
				DB:       opts.RateLimitRedisDB,
				PoolSize: opts.RateLimitRedisPoolSize,
				FailOpen: opts.RateLimitRedisFailOpen,
				KeyTTL:   time.Hour,
			}
			logger.Info().
				Str("redis_addr", opts.RateLimitRedisAddr).
				Bool("fail_open", opts.RateLimitRedisFailOpen).
				Msg("distributed rate limiting enabled via Redis")
		}

		chain.AddFilter(filter.NewRateLimitFilter(rateLimitCfg, utils.LoadTierConfig()))
	}

	// Storage infrastructure
	pools, profiles, profilePlacer, backendManager := initializeStorage(opts)

	// Database
	rawDB, err := initializeDatabase(opts)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize database")
	}
	// Wrap with metrics instrumentation
	metadataDB := db.NewMetricsDB(rawDB)
	if err := metadataDB.Migrate(cmd.Context()); err != nil {
		logger.Fatal().Err(err).Msg("failed to run database migrations")
	}

	// Start background bucket loader to populate bucket cache from DB
	bucketLoader := cache.NewBucketLoader(
		func(ctx context.Context) iter.Seq2[*types.BucketInfo, error] {
			return func(yield func(*types.BucketInfo, error) bool) {
				var continuationToken string
				for {
					result, err := metadataDB.ListBuckets(ctx, &db.ListBucketsParams{
						MaxBuckets:        1000,
						ContinuationToken: continuationToken,
					})
					if err != nil {
						yield(nil, err)
						return
					}
					for _, bucket := range result.Buckets {
						if !yield(bucket, nil) {
							return
						}
					}
					if !result.IsTruncated {
						return
					}
					continuationToken = result.NextContinuationToken
				}
			}
		},
		bucketStore,
	)
	go bucketLoader.LoadBuckets(cmd.Context(), time.Minute)

	// Task queue for background processing (GC decrement retry, etc.)
	var tq taskqueue.Queue
	var sqlDBConn *sql.DB

	// Unwrap MetricsDB to get the underlying DB for SQL connection
	switch v := rawDB.(type) {
	case *vitess.Vitess:
		sqlDBConn = v.SqlDB()
	case *postgres.Postgres:
		sqlDBConn = v.SqlDB()
	}

	// Start connection pool metrics collector
	if sqlDBConn != nil {
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-cmd.Context().Done():
					return
				case <-ticker.C:
					stats := sqlDBConn.Stats()
					db.UpdateConnectionMetrics(stats.InUse, stats.Idle, stats.OpenConnections)
				}
			}
		}()
	}

	if sqlDBConn != nil {
		tq, err = taskqueue.NewDBQueue(taskqueue.DBQueueConfig{
			DB:        sqlDBConn,
			TableName: "tasks",
		})
		if err != nil {
			logger.Warn().Err(err).Msg("failed to create task queue, GC retries disabled")
		} else {
			logger.Info().Msg("task queue enabled for GC decrement retry")
		}
	}

	// Access logging (enterprise: FeatureAccessLog)
	accessLogMgr, err := InitializeAccessLog(cmd.Context(), AccessLogConfig{
		Enabled:        opts.AccessLogsEnabled,
		ClickHouseDSN:  opts.ClickHouseDSN,
		BatchSize:      opts.AccessLogBatchSize,
		FlushInterval:  opts.AccessLogFlushInterval,
		ExportInterval: opts.AccessLogExportInterval,
		DB:             metadataDB,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize access logging")
	}
	if accessLogMgr != nil {
		defer accessLogMgr.Stop()
	}

	// CRR hook for cross-region replication (enterprise feature)
	// Returns nil in community edition
	crrHook := api.NewCRRHook(tq, opts.RegionID)
	if crrHook != nil {
		logger.Info().Str("region", opts.RegionID).Msg("CRR hook enabled for cross-region replication")
	}

	// Event emitter for S3 notifications (enterprise feature)
	// Creates a noop emitter if taskqueue is not available
	var emitter *events.Emitter
	if tq != nil {
		emitter = events.NewEmitter(events.EmitterConfig{
			Queue:   tq,
			Enabled: true,
			Region:  opts.RegionID,
		})
		logger.Info().Str("region", opts.RegionID).Msg("event emitter enabled for S3 notifications")
	} else {
		emitter = events.NoopEmitter()
	}

	// Metadata server
	serverCfg := api.ServerConfig{
		ManagerClient:     managerClient,
		Chain:             chain,
		GlobalBucketCache: globalBucketCache,
		BucketStore:       bucketStore,
		Pools:             pools,
		Profiles:          profiles,
		ProfilePlacer:     profilePlacer,
		BackendManager:    backendManager,
		DB:                metadataDB,
		DefaultProfile:    "STANDARD",
		IAMService:        iamService, // For KMS operations (enterprise feature)
		TaskQueue:         tq,
		CRRHook:           crrHook, // Cross-region replication (enterprise feature)
		Emitter:           emitter, // S3 event notifications (enterprise feature)
		// Cross-region replication credentials (enterprise: FeatureMultiRegion)
		// Never log the secret key!
		ReplicationCredentials: api.ReplicationCredentials{
			AccessKeyID:     opts.ReplicationAccessKeyID,
			SecretAccessKey: opts.ReplicationSecretAccessKey,
		},
		// Lifecycle scanner configuration
		LifecycleScannerEnabled:  opts.LifecycleScannerEnabled,
		LifecycleScanInterval:    opts.LifecycleScanInterval,
		LifecycleScanConcurrency: opts.LifecycleScanConcurrency,
		LifecycleScanBatchSize:   opts.LifecycleScanBatchSize,
		LifecycleMaxTasksPerScan: opts.LifecycleMaxTasksPerScan,
	}
	if accessLogMgr != nil {
		serverCfg.AccessLogCollector = accessLogMgr.Collector()
	}
	metadataServer := api.NewMetadataServer(cmd.Context(), serverCfg)
	defer metadataServer.Shutdown()

	// Register custom readiness check - service is only ready when caches are populated
	debug.SetReadyCheck(func() bool {
		return bucketStore.IsReady() && globalBucketCache.IsReady()
	})

	// Start servers
	httpMux := http.NewServeMux()
	httpMux.Handle("/", metadataServer)
	httpServer := startHTTPServer(httpMux, opts.IP, opts.HTTPPort)
	grpcServer := startMetadataGRPCServer(opts, metadataServer)
	debugServer := startHTTPServer(debug.GetMux(), opts.IP, opts.DebugPort)

	// Register with manager and start heartbeat loop (if configured)
	if opts.NodeID != "" && opts.AdvertiseAddr != "" {
		if err := registerMetadataServer(cmd.Context(), managerClient, metadataDB, opts); err != nil {
			logger.Warn().Err(err).Msg("failed to register with manager (will retry via heartbeat)")
		} else {
			logger.Info().Msg("Successfully registered metadata server with manager")
		}
		go heartbeatMetadataServer(cmd.Context(), managerClient, metadataDB, opts)
	} else {
		logger.Info().Msg("Manager registration skipped (no node_id/advertise_addr configured)")
	}

	debug.SetReady()
	waitForShutdown()
	debug.SetNotReady()

	httpServer.Shutdown(cmd.Context())
	grpcServer.GracefulStop()
	debugServer.Shutdown(cmd.Context())
}

func loadMetadataOpts(cmd *cobra.Command) MetadataServerOpts {
	f := NewFlagLoader(cmd)

	// Get node_id (optional for backward compatibility)
	nodeID := f.String("node_id")
	if nodeID == "" {
		nodeID = os.Getenv("NODE_ID")
	}

	// Get advertise address (optional for backward compatibility)
	advertiseAddr := getAdvertiseAddr(f.String("advertise_addr"))

	return MetadataServerOpts{
		IP:             f.String("ip"),
		HTTPPort:       f.Int("http_port"),
		GRPCPort:       f.Int("grpc_port"),
		DebugPort:      f.Int("debug_port"),
		CertFile:       f.String("cert_file"),
		KeyFile:        f.String("key_file"),
		LogLevel:       f.String("log_level"),
		NodeID:         nodeID,
		AdvertiseAddr:  advertiseAddr,
		ManagerAddr:    f.String("manager_addr"),
		S3Domains:      f.StringSlice("s3_domains"),
		RegionID:       f.String("region_id"),
		PoolsConfig:    f.String("pools_config"),
		ProfilesConfig: f.String("profiles_config"),
		DataDir:        f.String("data_dir"),
		DBDriver:       f.String("db_driver"),
		DBDSN:          f.String("db_dsn"),
		DBMaxOpenConns: f.Int("db_max_open_conns"),
		DBMaxIdleConns: f.Int("db_max_idle_conns"),
		DBTLSMode:      f.String("db_tls_mode"),
		DBTLSCAFile:    f.String("db_tls_ca_file"),
		// Rate limiting
		RateLimitEnabled:        f.Bool("rate_limit_enabled"),
		RateLimitBurstMultipler: f.Int64("rate_limit_burst_multiplier"),
		RateLimitRedisEnabled:   f.Bool("rate_limit_redis_enabled"),
		RateLimitRedisAddr:      f.String("rate_limit_redis_addr"),
		RateLimitRedisPassword:  f.String("rate_limit_redis_password"),
		RateLimitRedisDB:        f.Int("rate_limit_redis_db"),
		RateLimitRedisPoolSize:  f.Int("rate_limit_redis_pool_size"),
		RateLimitRedisFailOpen:  f.Bool("rate_limit_redis_fail_open"),
		// Access logging
		AccessLogsEnabled:       f.Bool("access_logs_enabled"),
		ClickHouseDSN:           f.String("clickhouse_dsn"),
		AccessLogBatchSize:      f.Int("access_log_batch_size"),
		AccessLogFlushInterval:  f.Duration("access_log_flush_interval"),
		AccessLogExportInterval: f.Duration("access_log_export_interval"),
		// Lifecycle scanner
		LifecycleScannerEnabled:  f.Bool("lifecycle_scanner_enabled"),
		LifecycleScanInterval:    f.Duration("lifecycle_scan_interval"),
		LifecycleScanConcurrency: f.Int("lifecycle_scan_concurrency"),
		LifecycleScanBatchSize:   f.Int("lifecycle_scan_batch_size"),
		LifecycleMaxTasksPerScan: f.Int("lifecycle_max_tasks_per_scan"),
		// Cross-region replication
		ReplicationAccessKeyID:     f.String("replication_access_key_id"),
		ReplicationSecretAccessKey: f.String("replication_secret_access_key"),
	}
}

func initializeIAM(ctx context.Context, opts MetadataServerOpts) (*iam.Service, error) {
	// Metadata always syncs IAM from manager - no local mode
	logger.Info().Str("manager_addr", opts.ManagerAddr).Msg("initializing IAM (syncing from manager)")

	remoteStore, err := iam.NewRemoteCredentialStore(ctx, iam.RemoteStoreConfig{
		ManagerAddrs:  []string{opts.ManagerAddr},
		CacheMaxItems: 1000000, // 1 million items
		CacheTTL:      5 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create remote credential store: %w", err)
	}

	// Initial sync - get all credentials AND policies from manager
	if err := remoteStore.InitialSync(ctx); err != nil {
		logger.Warn().Err(err).Msg("failed initial IAM sync (will retry via streaming)")
	}

	// Start streaming for real-time updates (syncs credentials + policies)
	if err := remoteStore.StartStreaming(ctx); err != nil {
		logger.Warn().Err(err).Msg("failed to start IAM streaming (will retry)")
	}

	// Create IAM service with remote store
	// RemoteCredentialStore implements CredentialStore, PolicyStore, AND GroupStore
	return iam.NewService(iam.ServiceConfig{
		CredentialStore: remoteStore,
		PolicyStore:     remoteStore, // Same store - policies synced alongside credentials
		GroupStore:      remoteStore, // Same store - groups synced alongside credentials
		CacheMaxItems:   1000000,
		CacheTTL:        5 * time.Minute,
	})
}

func initializeStorage(opts MetadataServerOpts) (*types.PoolSet, *types.ProfileSet, *placer.ProfilePlacer, *backend.Manager) {
	var pools *types.PoolSet
	var profiles *types.ProfileSet
	var err error

	// Load or create pools
	if opts.PoolsConfig != "" {
		pools, err = types.LoadPoolsFromFile(opts.PoolsConfig)
		if err != nil {
			logger.Fatal().Err(err).Str("path", opts.PoolsConfig).Msg("failed to load pools config")
		}
		logger.Info().Str("path", opts.PoolsConfig).Int("count", len(pools.List())).Msg("loaded storage pools")
	} else {
		pools = types.NewPoolSet()
		defaultPool := types.NewStoragePool("default", types.StorageTypeLocal, 1.0)
		defaultPool.Backends = []string{opts.DataDir}
		pools.Add(defaultPool)
		logger.Info().Str("data_dir", opts.DataDir).Msg("created default local storage pool")
	}

	// Load or create profiles
	if opts.ProfilesConfig != "" {
		profiles, err = types.LoadProfilesFromFile(opts.ProfilesConfig, pools)
		if err != nil {
			logger.Fatal().Err(err).Str("path", opts.ProfilesConfig).Msg("failed to load profiles config")
		}
		logger.Info().Str("path", opts.ProfilesConfig).Int("count", len(profiles.List())).Msg("loaded storage profiles")
	} else {
		profiles = types.NewProfileSet()
		if poolList := pools.List(); len(poolList) > 0 {
			defaultProfile := types.StandardProfile(poolList[0].ID)
			profiles.Add(defaultProfile)
			logger.Info().Str("profile", defaultProfile.Name).Msg("created default storage profile")
		}
	}

	// Initialize backend manager
	backendManager := backend.NewManager()
	for _, pool := range pools.List() {
		var cfg types.BackendConfig
		switch pool.BackendType {
		case types.StorageTypeLocal:
			dataPath := opts.DataDir
			if len(pool.Backends) > 0 {
				dataPath = pool.Backends[0]
			}
			os.MkdirAll(dataPath, 0755)
			cfg = types.BackendConfig{Type: types.StorageTypeLocal, Path: dataPath}
		case types.StorageTypeS3:
			cfg = types.BackendConfig{Type: types.StorageTypeS3, Endpoint: pool.Endpoint, Region: pool.Region, Bucket: pool.Name}
		default:
			logger.Warn().Str("pool", pool.Name).Str("type", string(pool.BackendType)).Msg("unsupported backend type")
			continue
		}

		if err := backendManager.Add(pool.ID.String(), cfg); err != nil {
			logger.Fatal().Err(err).Str("pool", pool.Name).Msg("failed to initialize backend")
		}
		logger.Info().Str("pool", pool.Name).Str("type", string(pool.BackendType)).Msg("initialized backend")
	}

	// Initialize profile placer
	profilePlacer := placer.NewProfilePlacer(pools)
	for _, profile := range profiles.List() {
		if err := profilePlacer.AddProfile(profile); err != nil {
			logger.Fatal().Err(err).Str("profile", profile.Name).Msg("failed to add profile")
		}
	}

	logger.Info().Int("pools", len(pools.List())).Int("profiles", len(profiles.List())).Msg("storage initialized")
	return pools, profiles, profilePlacer, backendManager
}

func initializeDatabase(opts MetadataServerOpts) (db.DB, error) {
	driver := db.Driver(opts.DBDriver)
	logger.Info().Str("driver", string(driver)).Str("dsn", maskDSN(opts.DBDSN)).Msg("initializing database")

	switch driver {
	case db.DriverVitess, db.DriverMySQL:
		if opts.DBDSN == "" {
			return nil, fmt.Errorf("--db_dsn required for %s driver", driver)
		}
		cfg := vitess.DefaultConfig(opts.DBDSN)
		cfg.MaxOpenConns = opts.DBMaxOpenConns
		cfg.MaxIdleConns = opts.DBMaxIdleConns
		cfg.TLSMode = vitess.TLSMode(opts.DBTLSMode)
		cfg.TLSCAFile = opts.DBTLSCAFile
		return vitess.NewVitess(cfg)
	case db.DriverPostgres, db.DriverCockroach:
		if opts.DBDSN == "" {
			return nil, fmt.Errorf("--db_dsn required for %s driver", driver)
		}
		cfg := postgres.Config{
			DSN:          opts.DBDSN,
			Driver:       driver,
			MaxOpenConns: opts.DBMaxOpenConns,
			MaxIdleConns: opts.DBMaxIdleConns,
		}
		return postgres.NewPostgres(cfg)
	default:
		return nil, fmt.Errorf("unknown driver: %s", driver)
	}
}

func maskDSN(dsn string) string {
	if dsn == "" {
		return "(none)"
	}
	if len(dsn) > 20 {
		return dsn[:10] + "***" + dsn[len(dsn)-5:]
	}
	return "***"
}

func startMetadataGRPCServer(opts MetadataServerOpts, ms *api.MetadataServer) *grpc.Server {
	listener, err := utils.NewListener(utils.JoinHostPort(opts.IP, opts.GRPCPort), 0)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create gRPC listener")
	}

	grpcOpts := loadTLSServerOpts(opts.CertFile, opts.KeyFile)
	grpcServer := proto.NewGRPCServer(grpcOpts...)
	reflection.Register(grpcServer)
	metadata_pb.RegisterMetadataServiceServer(grpcServer, ms)

	// Register usage reporting gRPC service
	api.RegisterUsageService(grpcServer, api.UsageServiceConfig{
		Store:     ms.UsageStore(),
		Collector: ms.UsageCollector(),
		Config:    ms.UsageConfig(),
	})

	go func() {
		logger.Info().Str("grpc_addr", utils.JoinHostPort(opts.IP, opts.GRPCPort)).Msg("Starting metadata gRPC server")
		if err := grpcServer.Serve(listener); err != nil {
			logger.Fatal().Err(err).Msg("failed to start gRPC server")
		}
	}()

	return grpcServer
}

// registerMetadataServer registers the metadata server with the manager
func registerMetadataServer(ctx context.Context, managerClient *client.ManagerClientPool, metadataDB db.DB, opts MetadataServerOpts) error {
	// Get bucket count for data loss detection
	bucketCount, err := metadataDB.CountBuckets(ctx)
	if err != nil {
		return fmt.Errorf("failed to count buckets: %w", err)
	}

	// Build registration request using advertised address
	location := &common_pb.Location{
		Address: opts.AdvertiseAddr,
		Node:    opts.NodeID,
	}

	req := &manager_pb.RegisterServiceRequest{
		ServiceType: manager_pb.ServiceType_METADATA_SERVICE,
		Location:    location,
		ServiceMetadata: &manager_pb.RegisterServiceRequest_MetadataService{
			MetadataService: &manager_pb.MetadataServiceMetadata{
				BucketCount: uint64(bucketCount),
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
		Int64("bucket_count", bucketCount).
		Uint64("topology_version", resp.Version).
		Msg("Metadata server registered with manager")

	return nil
}

// heartbeatMetadataServer sends periodic heartbeats to the manager
// This includes bucket count for data loss detection.
func heartbeatMetadataServer(ctx context.Context, managerClient *client.ManagerClientPool, metadataDB db.DB, opts MetadataServerOpts) {
	ticker := time.NewTicker(30 * time.Second) // Heartbeat every 30 seconds
	defer ticker.Stop()

	var topologyVersion uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get current bucket count for data loss detection
			bucketCount, err := metadataDB.CountBuckets(ctx)
			if err != nil {
				logger.Warn().Err(err).Msg("failed to count buckets for heartbeat")
				continue
			}

			// Build heartbeat request
			location := &common_pb.Location{
				Address: opts.AdvertiseAddr,
				Node:    opts.NodeID,
			}

			req := &manager_pb.HeartbeatRequest{
				ServiceType: manager_pb.ServiceType_METADATA_SERVICE,
				Location:    location,
				Version:     topologyVersion,
				ServiceMetadata: &manager_pb.HeartbeatRequest_MetadataService{
					MetadataService: &manager_pb.MetadataServiceMetadata{
						BucketCount: uint64(bucketCount),
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
					Msg("Topology changed")
				topologyVersion = resp.TopologyVersion
			} else {
				topologyVersion = resp.TopologyVersion
			}
		}
	}
}
