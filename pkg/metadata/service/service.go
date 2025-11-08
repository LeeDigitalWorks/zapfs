// Package service provides the business logic layer for the metadata server.
// It separates concerns between HTTP handling and domain logic, following
// the pattern established in pkg/iam/service.go.
//
// Usage:
//
//	cfg := service.Config{
//	    DB:             myDB,
//	    ManagerClient:  myManagerClient,
//	    FileClientPool: myFileClient,
//	    BucketStore:    myBucketStore,
//	    Profiles:       myProfiles,
//	}
//	svc, err := service.NewService(cfg)
//	if err != nil {
//	    return err
//	}
//
//	// Use in handlers
//	result, err := svc.Objects().PutObject(ctx, req)
package service

import (
	"context"
	"os"

	enttaskqueue "zapfs/enterprise/taskqueue"
	"zapfs/pkg/metadata/service/bucket"
	"zapfs/pkg/metadata/service/config"
	"zapfs/pkg/metadata/service/encryption"
	"zapfs/pkg/metadata/service/multipart"
	"zapfs/pkg/metadata/service/object"
	"zapfs/pkg/metadata/service/storage"
	"zapfs/pkg/taskqueue"
	"zapfs/pkg/taskqueue/handlers"
)

// Service provides a unified interface for all metadata operations.
// It composes specialized services for different operation types.
type Service struct {
	cfg Config

	// Specialized services
	objects   object.Service
	buckets   bucket.Service
	multipart multipart.Service
	configSvc config.Service

	// Shared components
	storage    *storage.Coordinator
	encryption *encryption.Handler
	taskWorker *taskqueue.Worker
}

// NewService creates a new metadata service with the given configuration.
func NewService(cfg Config) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Apply defaults
	if cfg.DefaultProfile == "" {
		cfg.DefaultProfile = "STANDARD"
	}

	// Initialize storage coordinator (with optional taskqueue for retry)
	storageCoord := storage.NewCoordinator(storage.CoordinatorConfig{
		ManagerClient:  cfg.ManagerClient,
		FileClientPool: cfg.FileClientPool,
		Profiles:       cfg.Profiles,
		DefaultProfile: cfg.DefaultProfile,
		TaskQueue:      cfg.TaskQueue,
	})

	// Initialize encryption handler
	var encHandler *encryption.Handler
	if cfg.IAMService != nil && cfg.IAMService.KMS() != nil {
		encHandler = encryption.NewHandler(cfg.IAMService.KMS())
	} else {
		encHandler = encryption.NewHandler(nil)
	}

	// Initialize object service
	objectSvc, err := object.NewService(object.Config{
		DB:             cfg.DB,
		Storage:        storageCoord,
		Encryption:     encHandler,
		BucketStore:    cfg.BucketStore,
		DefaultProfile: cfg.DefaultProfile,
		Profiles:       cfg.Profiles,
		CRRHook:        cfg.CRRHook,
	})
	if err != nil {
		return nil, err
	}

	// Initialize bucket service
	bucketSvc, err := bucket.NewService(bucket.Config{
		DB:                cfg.DB,
		ManagerClient:     cfg.ManagerClient,
		GlobalBucketCache: cfg.GlobalBucketCache,
		BucketStore:       cfg.BucketStore,
	})
	if err != nil {
		return nil, err
	}

	// Initialize multipart service
	multipartSvc, err := multipart.NewService(multipart.Config{
		DB:             cfg.DB,
		Storage:        storageCoord,
		Profiles:       cfg.Profiles,
		DefaultProfile: cfg.DefaultProfile,
	})
	if err != nil {
		return nil, err
	}

	// Initialize config service
	configSvc, err := config.NewService(config.Config{
		DB:          cfg.DB,
		BucketStore: cfg.BucketStore,
	})
	if err != nil {
		return nil, err
	}

	svc := &Service{
		cfg:        cfg,
		objects:    objectSvc,
		buckets:    bucketSvc,
		multipart:  multipartSvc,
		configSvc:  configSvc,
		storage:    storageCoord,
		encryption: encHandler,
	}

	// Initialize and start task worker if taskqueue is configured
	if cfg.TaskQueue != nil {
		workerID := cfg.TaskWorkerID
		if workerID == "" {
			workerID, _ = os.Hostname()
		}
		concurrency := cfg.TaskWorkerConcurrency
		if concurrency == 0 {
			concurrency = 5
		}
		interval := cfg.TaskWorkerInterval
		if interval == 0 {
			interval = taskqueue.DefaultPollInterval
		}

		worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
			ID:           workerID,
			Queue:        cfg.TaskQueue,
			PollInterval: interval,
			Concurrency:  concurrency,
		})

		// Register community handlers
		worker.RegisterHandler(handlers.NewGCDecrementHandler(cfg.FileClientPool))

		// Register enterprise handlers (returns nil in community edition)
		for _, h := range enttaskqueue.EnterpriseHandlers(enttaskqueue.Dependencies{}) {
			worker.RegisterHandler(h)
		}

		// Start the worker
		worker.Start(context.Background())
		svc.taskWorker = worker
	}

	return svc, nil
}

// Close stops all background processors and releases resources
func (s *Service) Close() {
	if s.taskWorker != nil {
		s.taskWorker.Stop()
	}
}

// Objects returns the object operations service.
func (s *Service) Objects() object.Service {
	return s.objects
}

// Buckets returns the bucket operations service.
func (s *Service) Buckets() bucket.Service {
	return s.buckets
}

// Multipart returns the multipart upload operations service.
func (s *Service) Multipart() multipart.Service {
	return s.multipart
}

// Config returns the config operations service (tagging, ACL, policy, etc.).
func (s *Service) Config() config.Service {
	return s.configSvc
}

// Storage returns the storage coordinator (for testing/advanced use).
func (s *Service) Storage() *storage.Coordinator {
	return s.storage
}

// Encryption returns the encryption handler (for testing/advanced use).
func (s *Service) Encryption() *encryption.Handler {
	return s.encryption
}

// ServiceConfig returns the service configuration.
func (s *Service) ServiceConfig() Config {
	return s.cfg
}
