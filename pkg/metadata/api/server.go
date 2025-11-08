package api

import (
	"context"

	"zapfs/pkg/cache"
	"zapfs/pkg/iam"
	"zapfs/pkg/metadata/client"
	"zapfs/pkg/metadata/db"
	"zapfs/pkg/metadata/filter"
	"zapfs/pkg/metadata/service"
	"zapfs/pkg/s3api/s3action"
	"zapfs/pkg/storage/backend"
	"zapfs/pkg/storage/placer"
	"zapfs/pkg/taskqueue"
	"zapfs/pkg/types"
	"zapfs/proto/metadata_pb"

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
	ms.handlers = map[s3action.Action]Handler{
		// Bucket actions
		s3action.CreateBucket:      ms.CreateBucketHandler,
		s3action.DeleteBucket:      ms.DeleteBucketHandler,
		s3action.ListBuckets:       ms.ListBucketsHandler,
		s3action.HeadBucket:        ms.HeadBucketHandler,
		s3action.GetBucketLocation: ms.GetBucketLocationHandler,

		// Bucket versioning
		s3action.GetBucketVersioning: ms.GetBucketVersioningHandler,
		s3action.PutBucketVersioning: ms.PutBucketVersioningHandler,

		// Bucket ACL
		s3action.GetBucketAcl: ms.GetBucketAclHandler,
		s3action.PutBucketAcl: ms.PutBucketAclHandler,

		// Bucket policy
		s3action.GetBucketPolicy:    ms.GetBucketPolicyHandler,
		s3action.PutBucketPolicy:    ms.PutBucketPolicyHandler,
		s3action.DeleteBucketPolicy: ms.DeleteBucketPolicyHandler,

		// Bucket CORS
		s3action.GetBucketCors:    ms.GetBucketCorsHandler,
		s3action.PutBucketCors:    ms.PutBucketCorsHandler,
		s3action.DeleteBucketCors: ms.DeleteBucketCorsHandler,

		// Bucket website
		s3action.GetBucketWebsite:    ms.GetBucketWebsiteHandler,
		s3action.PutBucketWebsite:    ms.PutBucketWebsiteHandler,
		s3action.DeleteBucketWebsite: ms.DeleteBucketWebsiteHandler,

		// Bucket tagging
		s3action.GetBucketTagging:    ms.GetBucketTaggingHandler,
		s3action.PutBucketTagging:    ms.PutBucketTaggingHandler,
		s3action.DeleteBucketTagging: ms.DeleteBucketTaggingHandler,

		// Bucket encryption
		s3action.GetBucketEncryption:    ms.GetBucketEncryptionHandler,
		s3action.PutBucketEncryption:    ms.PutBucketEncryptionHandler,
		s3action.DeleteBucketEncryption: ms.DeleteBucketEncryptionHandler,

		// Bucket lifecycle
		s3action.GetBucketLifecycleConfiguration: ms.GetBucketLifecycleConfigurationHandler,
		s3action.PutBucketLifecycleConfiguration: ms.PutBucketLifecycleConfigurationHandler,
		s3action.DeleteBucketLifecycle:           ms.DeleteBucketLifecycleHandler,

		// Object Lock (WORM)
		s3action.GetObjectLockConfiguration: ms.GetObjectLockConfigurationHandler,
		s3action.PutObjectLockConfiguration: ms.PutObjectLockConfigurationHandler,

		// Bucket replication (enterprise feature)
		s3action.GetBucketReplication:    ms.GetBucketReplicationHandler,
		s3action.PutBucketReplication:    ms.PutBucketReplicationHandler,
		s3action.DeleteBucketReplication: ms.DeleteBucketReplicationHandler,

		// Object actions
		s3action.PutObject:          ms.PutObjectHandler,
		s3action.GetObject:          ms.GetObjectHandler,
		s3action.HeadObject:         ms.HeadObjectHandler,
		s3action.DeleteObject:       ms.DeleteObjectHandler,
		s3action.DeleteObjects:      ms.DeleteObjectsHandler,
		s3action.CopyObject:         ms.CopyObjectHandler,
		s3action.ListObjects:        ms.ListObjectsHandler,
		s3action.ListObjectsV2:      ms.ListObjectsV2Handler,
		s3action.ListObjectVersions: ms.ListObjectVersionsHandler,

		// Object ACL
		s3action.GetObjectAcl: ms.GetObjectAclHandler,
		s3action.PutObjectAcl: ms.PutObjectAclHandler,

		// Object tagging
		s3action.GetObjectTagging:    ms.GetObjectTaggingHandler,
		s3action.PutObjectTagging:    ms.PutObjectTaggingHandler,
		s3action.DeleteObjectTagging: ms.DeleteObjectTaggingHandler,

		// Object retention and legal hold
		s3action.GetObjectRetention: ms.GetObjectRetentionHandler,
		s3action.PutObjectRetention: ms.PutObjectRetentionHandler,
		s3action.GetObjectLegalHold: ms.GetObjectLegalHoldHandler,
		s3action.PutObjectLegalHold: ms.PutObjectLegalHoldHandler,

		// Multipart upload actions
		s3action.CreateMultipartUpload:   ms.CreateMultipartUploadHandler,
		s3action.UploadPart:              ms.UploadPartHandler,
		s3action.CompleteMultipartUpload: ms.CompleteMultipartUploadHandler,
		s3action.AbortMultipartUpload:    ms.AbortMultipartUploadHandler,
		s3action.ListParts:               ms.ListPartsHandler,
		s3action.ListMultipartUploads:    ms.ListMultipartUploadsHandler,
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
