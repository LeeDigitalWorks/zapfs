package service

import (
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/cache"
	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/object"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// Config holds all configuration for the metadata service layer.
// This follows the pattern established in pkg/iam/service.go.
type Config struct {
	// Database for metadata storage
	DB db.DB

	// External service clients
	ManagerClient  client.Manager
	FileClientPool client.File

	// Caches
	BucketStore       *cache.BucketStore
	GlobalBucketCache *cache.GlobalBucketCache

	// Storage configuration
	Profiles       *types.ProfileSet
	DefaultProfile string

	// Enterprise features (may be nil)
	IAMService *iam.Service   // For KMS operations
	CRRHook    object.CRRHook // For cross-region replication

	// Task queue for background processing (optional)
	// If provided, enables GC retry processing and other background tasks
	TaskQueue taskqueue.Queue

	// Task worker configuration
	TaskWorkerID          string        // Worker identifier (default: hostname)
	TaskWorkerConcurrency int           // Number of concurrent task processors (default: 5)
	TaskWorkerInterval    time.Duration // Poll interval (default: 1s)

	// Lifecycle scanner configuration (community feature)
	LifecycleScannerEnabled  bool          // Enable lifecycle scanning (default: false)
	LifecycleScanInterval    time.Duration // How often to scan (default: 1h)
	LifecycleScanConcurrency int           // Parallel bucket processing (default: 5)
	LifecycleScanBatchSize   int           // Objects per batch (default: 1000)
	LifecycleMaxTasksPerScan int           // Max tasks per scan run (default: 10000)
}

// DefaultConfig returns a Config with sensible defaults.
// Required fields (DB, ManagerClient, FileClientPool) must still be provided.
func DefaultConfig() Config {
	return Config{
		DefaultProfile: "STANDARD",
	}
}

// Validate checks that required configuration is present.
func (c *Config) Validate() error {
	if c.DB == nil {
		return NewValidationError("DB is required")
	}
	if c.ManagerClient == nil {
		return NewValidationError("ManagerClient is required")
	}
	if c.FileClientPool == nil {
		return NewValidationError("FileClientPool is required")
	}
	return nil
}
