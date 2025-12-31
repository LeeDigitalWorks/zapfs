package api

import (
	"context"

	"github.com/LeeDigitalWorks/zapfs/pkg/cache"
	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/filter"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3action"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/placer"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/proto/metadata_pb"

	"github.com/prometheus/client_golang/prometheus"
)

type MetadataServer struct {
	metadata_pb.UnimplementedMetadataServiceServer

	ctx    context.Context
	cancel context.CancelFunc

	// Service layer for business logic (new architecture)
	svc *service.Service

	// bucketStore provides bucket cache operations and implements PolicyStore
	// and ACLStore interfaces for the authorization filter.
	// This is the local regional cache, loaded from Vitess.
	bucketStore *cache.BucketStore

	// Global Bucket Name Cache -> Owner ID
	// This is synchronized from the manager leader via gRPC
	globalBucketCache *cache.GlobalBucketCache

	chain    *filter.Chain
	handlers map[s3action.Action]Handler

	metricsRequest         *prometheus.CounterVec
	metricsRequestDuration *prometheus.HistogramVec

	// Manager client for placement decisions and cluster state
	// Uses leader-aware routing: writes go to leader, reads can use any node
	managerClient client.Manager

	// File service client for streaming data operations
	fileClientPool client.File

	// Storage infrastructure (kept for backward compatibility / local dev mode)
	pools          *types.PoolSet
	profiles       *types.ProfileSet
	profilePlacer  *placer.ProfilePlacer
	backendManager *backend.Manager
	db             db.DB

	// Default profile name for objects without explicit storage class
	defaultProfile string

	// CRR hook for cross-region replication (enterprise)
	crrHook *CRRHook

	// IAM service for KMS operations (enterprise feature)
	iamService *iam.Service
}

// ServerConfig holds configuration for creating a MetadataServer
type ServerConfig struct {
	ManagerClient     client.Manager
	Chain             *filter.Chain
	GlobalBucketCache *cache.GlobalBucketCache
	BucketStore       *cache.BucketStore
	Pools             *types.PoolSet
	Profiles          *types.ProfileSet
	ProfilePlacer     *placer.ProfilePlacer
	BackendManager    *backend.Manager
	DB                db.DB
	DefaultProfile    string
	FileClientPool    client.File
	CRRHook           *CRRHook        // Enterprise: cross-region replication hook
	IAMService        *iam.Service    // IAM service for KMS operations (enterprise feature)
	TaskQueue         taskqueue.Queue // Optional: for GC decrement retry and background tasks
}

func NewMetadataServer(ctx context.Context, cfg ServerConfig) *MetadataServer {
	ctx, cancel := context.WithCancel(ctx)

	// Create file client pool if not provided
	fileClientPool := cfg.FileClientPool
	if fileClientPool == nil {
		fileClientPool = client.NewFileClientPool()
	}

	// Default profile
	defaultProfile := cfg.DefaultProfile
	if defaultProfile == "" {
		defaultProfile = "STANDARD"
	}

	// Initialize the service layer
	svcCfg := service.Config{
		DB:                cfg.DB,
		ManagerClient:     cfg.ManagerClient,
		FileClientPool:    fileClientPool,
		BucketStore:       cfg.BucketStore,
		GlobalBucketCache: cfg.GlobalBucketCache,
		Profiles:          cfg.Profiles,
		DefaultProfile:    defaultProfile,
		IAMService:        cfg.IAMService,
		CRRHook:           adaptCRRHook(cfg.CRRHook),
		TaskQueue:         cfg.TaskQueue,
	}

	svc, err := service.NewService(svcCfg)
	if err != nil {
		// Log error but don't fail - service layer is optional during migration
		// Handlers will fall back to direct implementation
		svc = nil
	}

	ms := &MetadataServer{
		ctx:               ctx,
		cancel:            cancel,
		svc:               svc,
		managerClient:     cfg.ManagerClient,
		chain:             cfg.Chain,
		fileClientPool:    fileClientPool,
		handlers:          make(map[s3action.Action]Handler),
		bucketStore:       cfg.BucketStore,
		globalBucketCache: cfg.GlobalBucketCache,
		pools:             cfg.Pools,
		profiles:          cfg.Profiles,
		profilePlacer:     cfg.ProfilePlacer,
		backendManager:    cfg.BackendManager,
		db:                cfg.DB,
		defaultProfile:    defaultProfile,
		metricsRequest: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "s3api_requests_counter",
			Help: "Number of S3 API requests received",
		}, []string{"action", "status_code"}),
		metricsRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "s3api_request_duration_seconds",
			Help:    "Duration of S3 API requests",
			Buckets: prometheus.DefBuckets,
		}, []string{"action", "status_code"}),
	}

	// Set CRR hook if provided
	ms.crrHook = cfg.CRRHook

	// Set IAM service if provided (for KMS operations)
	ms.iamService = cfg.IAMService

	// Register S3 API handlers (both HTTP and gRPC can call these)
	// Handlers are organized by file location for maintainability.
	ms.handlers = map[s3action.Action]Handler{
		// =====================================================================
		// bucket.go - Core bucket operations
		// =====================================================================
		s3action.CreateBucket:      ms.CreateBucketHandler,
		s3action.DeleteBucket:      ms.DeleteBucketHandler,
		s3action.ListBuckets:       ms.ListBucketsHandler,
		s3action.HeadBucket:        ms.HeadBucketHandler,
		s3action.GetBucketLocation: ms.GetBucketLocationHandler,

		// =====================================================================
		// versioning.go - Bucket versioning
		// =====================================================================
		s3action.GetBucketVersioning: ms.GetBucketVersioningHandler,
		s3action.PutBucketVersioning: ms.PutBucketVersioningHandler,

		// =====================================================================
		// acl.go - Bucket and object ACLs
		// =====================================================================
		s3action.GetBucketAcl: ms.GetBucketAclHandler,
		s3action.PutBucketAcl: ms.PutBucketAclHandler,
		s3action.GetObjectAcl: ms.GetObjectAclHandler,
		s3action.PutObjectAcl: ms.PutObjectAclHandler,

		// =====================================================================
		// policy.go - Bucket policies
		// =====================================================================
		s3action.GetBucketPolicy:       ms.GetBucketPolicyHandler,
		s3action.PutBucketPolicy:       ms.PutBucketPolicyHandler,
		s3action.DeleteBucketPolicy:    ms.DeleteBucketPolicyHandler,
		s3action.GetBucketPolicyStatus: ms.GetBucketPolicyStatusHandler,

		// =====================================================================
		// cors.go - CORS configuration
		// =====================================================================
		s3action.GetBucketCors:    ms.GetBucketCorsHandler,
		s3action.PutBucketCors:    ms.PutBucketCorsHandler,
		s3action.DeleteBucketCors: ms.DeleteBucketCorsHandler,
		s3action.OptionsPreflight: ms.OptionsPreflightHandler,

		// =====================================================================
		// website.go - Static website hosting
		// =====================================================================
		s3action.GetBucketWebsite:    ms.GetBucketWebsiteHandler,
		s3action.PutBucketWebsite:    ms.PutBucketWebsiteHandler,
		s3action.DeleteBucketWebsite: ms.DeleteBucketWebsiteHandler,

		// =====================================================================
		// tagging.go - Bucket and object tagging
		// =====================================================================
		s3action.GetBucketTagging:    ms.GetBucketTaggingHandler,
		s3action.PutBucketTagging:    ms.PutBucketTaggingHandler,
		s3action.DeleteBucketTagging: ms.DeleteBucketTaggingHandler,
		s3action.GetObjectTagging:    ms.GetObjectTaggingHandler,
		s3action.PutObjectTagging:    ms.PutObjectTaggingHandler,
		s3action.DeleteObjectTagging: ms.DeleteObjectTaggingHandler,

		// =====================================================================
		// encryption.go - Server-side encryption (Enterprise: FeatureKMS)
		// =====================================================================
		s3action.GetBucketEncryption:    ms.GetBucketEncryptionHandler,
		s3action.PutBucketEncryption:    ms.PutBucketEncryptionHandler,
		s3action.DeleteBucketEncryption: ms.DeleteBucketEncryptionHandler,

		// =====================================================================
		// lifecycle.go - Lifecycle rules (Enterprise: FeatureLifecycle)
		// =====================================================================
		s3action.GetBucketLifecycleConfiguration: ms.GetBucketLifecycleConfigurationHandler,
		s3action.PutBucketLifecycleConfiguration: ms.PutBucketLifecycleConfigurationHandler,
		s3action.DeleteBucketLifecycle:           ms.DeleteBucketLifecycleHandler,

		// =====================================================================
		// object_lock.go - WORM / Object Lock (Enterprise: FeatureObjectLock)
		// =====================================================================
		s3action.GetObjectLockConfiguration: ms.GetObjectLockConfigurationHandler,
		s3action.PutObjectLockConfiguration: ms.PutObjectLockConfigurationHandler,
		s3action.GetObjectRetention:         ms.GetObjectRetentionHandler,
		s3action.PutObjectRetention:         ms.PutObjectRetentionHandler,
		s3action.GetObjectLegalHold:         ms.GetObjectLegalHoldHandler,
		s3action.PutObjectLegalHold:         ms.PutObjectLegalHoldHandler,

		// =====================================================================
		// replication.go - Cross-region replication (Enterprise: FeatureMultiRegion)
		// =====================================================================
		s3action.GetBucketReplication:    ms.GetBucketReplicationHandler,
		s3action.PutBucketReplication:    ms.PutBucketReplicationHandler,
		s3action.DeleteBucketReplication: ms.DeleteBucketReplicationHandler,

		// =====================================================================
		// object.go - Core object operations
		// =====================================================================
		s3action.PutObject:              ms.PutObjectHandler,
		s3action.GetObject:              ms.GetObjectHandler,
		s3action.HeadObject:             ms.HeadObjectHandler,
		s3action.DeleteObject:           ms.DeleteObjectHandler,
		s3action.DeleteObjects:          ms.DeleteObjectsHandler,
		s3action.CopyObject:             ms.CopyObjectHandler,
		s3action.ListObjects:            ms.ListObjectsHandler,
		s3action.ListObjectsV2:          ms.ListObjectsV2Handler,
		s3action.ListObjectVersions:     ms.ListObjectVersionsHandler,
		s3action.PostObject:             ms.PostObjectHandler,
		s3action.GetObjectAttributes:    ms.GetObjectAttributesHandler,
		s3action.GetObjectTorrent:       ms.GetObjectTorrentHandler,
		s3action.RestoreObject:          ms.RestoreObjectHandler, // Enterprise: FeatureLifecycle
		s3action.SelectObjectContent:    ms.SelectObjectContentHandler,
		s3action.WriteGetObjectResponse: ms.WriteGetObjectResponseHandler,
		s3action.CreateSession:          ms.CreateSessionHandler,

		// =====================================================================
		// multipart.go - Multipart upload operations
		// =====================================================================
		s3action.CreateMultipartUpload:   ms.CreateMultipartUploadHandler,
		s3action.UploadPart:              ms.UploadPartHandler,
		s3action.UploadPartCopy:          ms.UploadPartCopyHandler,
		s3action.CompleteMultipartUpload: ms.CompleteMultipartUploadHandler,
		s3action.AbortMultipartUpload:    ms.AbortMultipartUploadHandler,
		s3action.ListParts:               ms.ListPartsHandler,
		s3action.ListMultipartUploads:    ms.ListMultipartUploadsHandler,

		// =====================================================================
		// bucket_access.go - Public access block and ownership controls
		// =====================================================================
		s3action.GetPublicAccessBlock:          ms.GetPublicAccessBlockHandler,
		s3action.PutPublicAccessBlock:          ms.PutPublicAccessBlockHandler,
		s3action.DeletePublicAccessBlock:       ms.DeletePublicAccessBlockHandler,
		s3action.GetBucketOwnershipControls:    ms.GetBucketOwnershipControlsHandler,
		s3action.PutBucketOwnershipControls:    ms.PutBucketOwnershipControlsHandler,
		s3action.DeleteBucketOwnershipControls: ms.DeleteBucketOwnershipControlsHandler,

		// =====================================================================
		// bucket_config.go - Logging, payment, acceleration, notifications
		// =====================================================================
		s3action.GetBucketLogging:                   ms.GetBucketLoggingHandler,    // Enterprise: FeatureAuditLog
		s3action.PutBucketLogging:                   ms.PutBucketLoggingHandler,    // Enterprise: FeatureAuditLog
		s3action.GetBucketRequestPayment:            ms.GetBucketRequestPaymentHandler,
		s3action.PutBucketRequestPayment:            ms.PutBucketRequestPaymentHandler,
		s3action.GetBucketAccelerateConfiguration:   ms.GetBucketAccelerateConfigurationHandler,
		s3action.PutBucketAccelerateConfiguration:   ms.PutBucketAccelerateConfigurationHandler,
		s3action.GetBucketNotificationConfiguration: ms.GetBucketNotificationConfigurationHandler,
		s3action.PutBucketNotificationConfiguration: ms.PutBucketNotificationConfigurationHandler,

		// =====================================================================
		// bucket_analytics.go - Analytics, metrics, inventory, intelligent tiering
		// =====================================================================
		s3action.GetBucketAnalyticsConfiguration:             ms.GetBucketAnalyticsConfigurationHandler,
		s3action.PutBucketAnalyticsConfiguration:             ms.PutBucketAnalyticsConfigurationHandler,
		s3action.DeleteBucketAnalyticsConfiguration:          ms.DeleteBucketAnalyticsConfigurationHandler,
		s3action.ListBucketAnalyticsConfigurations:           ms.ListBucketAnalyticsConfigurationsHandler,
		s3action.GetBucketMetricsConfiguration:               ms.GetBucketMetricsConfigurationHandler,
		s3action.PutBucketMetricsConfiguration:               ms.PutBucketMetricsConfigurationHandler,
		s3action.DeleteBucketMetricsConfiguration:            ms.DeleteBucketMetricsConfigurationHandler,
		s3action.ListBucketMetricsConfigurations:             ms.ListBucketMetricsConfigurationsHandler,
		s3action.GetBucketInventoryConfiguration:             ms.GetBucketInventoryConfigurationHandler,
		s3action.PutBucketInventoryConfiguration:             ms.PutBucketInventoryConfigurationHandler,
		s3action.DeleteBucketInventoryConfiguration:          ms.DeleteBucketInventoryConfigurationHandler,
		s3action.ListBucketInventoryConfigurations:           ms.ListBucketInventoryConfigurationsHandler,
		s3action.GetBucketIntelligentTieringConfiguration:    ms.GetBucketIntelligentTieringConfigurationHandler,    // Enterprise: FeatureLifecycle
		s3action.PutBucketIntelligentTieringConfiguration:    ms.PutBucketIntelligentTieringConfigurationHandler,    // Enterprise: FeatureLifecycle
		s3action.DeleteBucketIntelligentTieringConfiguration: ms.DeleteBucketIntelligentTieringConfigurationHandler, // Enterprise: FeatureLifecycle
		s3action.ListBucketIntelligentTieringConfigurations:  ms.ListBucketIntelligentTieringConfigurationsHandler,  // Enterprise: FeatureLifecycle
	}

	return ms
}

func (ms *MetadataServer) Shutdown() error {
	ms.cancel()

	// Close service layer (stops task worker)
	if ms.svc != nil {
		ms.svc.Close()
	}

	// Close manager client connections
	if ms.managerClient != nil {
		ms.managerClient.Close()
	}

	// Close file client connections
	if ms.fileClientPool != nil {
		ms.fileClientPool.Close()
	}

	// Close backend connections (legacy)
	if ms.backendManager != nil {
		ms.backendManager.Close()
	}

	return nil
}
