package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/cache"
	"github.com/LeeDigitalWorks/zapfs/pkg/debug"
	"github.com/LeeDigitalWorks/zapfs/pkg/env"
	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/api"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db/vitess"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/filter"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/placer"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
	"github.com/LeeDigitalWorks/zapfs/proto"
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

	// Rate limiting
	RateLimitEnabled        bool
	RateLimitBurstMultipler int64
	RateLimitRedisEnabled   bool
	RateLimitRedisAddr      string
	RateLimitRedisPassword  string
	RateLimitRedisDB        int
	RateLimitRedisPoolSize  int
	RateLimitRedisFailOpen  bool
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

	// Rate limiting
	f.Bool("rate_limit_enabled", true, "Enable request rate limiting")
	f.Int64("rate_limit_burst_multiplier", 2, "Burst multiplier for rate limiting")
	f.Bool("rate_limit_redis_enabled", false, "Enable distributed rate limiting via Redis")
	f.String("rate_limit_redis_addr", "localhost:6379", "Redis address for distributed rate limiting")
	f.String("rate_limit_redis_password", "", "Redis password")
	f.Int("rate_limit_redis_db", 0, "Redis database number")
	f.Int("rate_limit_redis_pool_size", 10, "Redis connection pool size")
	f.Bool("rate_limit_redis_fail_open", true, "Allow requests when Redis is unavailable")

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
	metadataDB, err := initializeDatabase(opts)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize database")
	}
	if err := metadataDB.Migrate(cmd.Context()); err != nil {
		logger.Fatal().Err(err).Msg("failed to run database migrations")
	}

	// Task queue for background processing (GC decrement retry, etc.)
	var tq taskqueue.Queue
	if vitessDB, ok := metadataDB.(*vitess.Vitess); ok {
		tq, err = taskqueue.NewDBQueue(taskqueue.DBQueueConfig{
			DB:        vitessDB.SqlDB(),
			TableName: "tasks",
		})
		if err != nil {
			logger.Warn().Err(err).Msg("failed to create task queue, GC retries disabled")
		} else {
			logger.Info().Msg("task queue enabled for GC decrement retry")
		}
	}

	// Metadata server
	metadataServer := api.NewMetadataServer(cmd.Context(), api.ServerConfig{
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
	})
	defer metadataServer.Shutdown()

	// Start servers
	httpMux := http.NewServeMux()
	httpMux.Handle("/", metadataServer)
	httpServer := startHTTPServer(httpMux, opts.IP, opts.HTTPPort)
	grpcServer := startMetadataGRPCServer(opts, metadataServer)
	debugServer := startHTTPServer(debug.GetMux(), opts.IP, opts.DebugPort)

	debug.SetReady()
	waitForShutdown()
	debug.SetNotReady()

	httpServer.Shutdown(cmd.Context())
	grpcServer.GracefulStop()
	debugServer.Shutdown(cmd.Context())
}

func loadMetadataOpts(cmd *cobra.Command) MetadataServerOpts {
	f := NewFlagLoader(cmd)
	return MetadataServerOpts{
		IP:             f.String("ip"),
		HTTPPort:       f.Int("http_port"),
		GRPCPort:       f.Int("grpc_port"),
		DebugPort:      f.Int("debug_port"),
		CertFile:       f.String("cert_file"),
		KeyFile:        f.String("key_file"),
		LogLevel:       f.String("log_level"),
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
		// Rate limiting
		RateLimitEnabled:        f.Bool("rate_limit_enabled"),
		RateLimitBurstMultipler: f.Int64("rate_limit_burst_multiplier"),
		RateLimitRedisEnabled:   f.Bool("rate_limit_redis_enabled"),
		RateLimitRedisAddr:      f.String("rate_limit_redis_addr"),
		RateLimitRedisPassword:  f.String("rate_limit_redis_password"),
		RateLimitRedisDB:        f.Int("rate_limit_redis_db"),
		RateLimitRedisPoolSize:  f.Int("rate_limit_redis_pool_size"),
		RateLimitRedisFailOpen:  f.Bool("rate_limit_redis_fail_open"),
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
		return vitess.NewVitess(cfg)
	case db.DriverPostgres, db.DriverCockroach:
		return nil, fmt.Errorf("driver %s not yet implemented", driver)
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
