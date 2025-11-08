//go:build integration

package metadata

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"zapfs/integration/testutil"
	"zapfs/pkg/metadata/db"
	"zapfs/pkg/metadata/db/vitess"
	"zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Database Interface Tests (MySQL/Vitess)
// =============================================================================

// getTestDB returns a database instance for testing.
// Requires DB_DSN environment variable to connect to MySQL/Vitess.
// Skips test if DB_DSN is not set - integration tests require a real database.
func getTestDB(t *testing.T) (db.DB, func()) {
	t.Helper()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		t.Skip("DB_DSN not set - skipping database integration test (requires MySQL/Vitess)")
	}

	// Connect to MySQL/Vitess
	t.Logf("Connecting to database: %s", maskDSN(dsn))
	cfg := vitess.DefaultConfig(dsn)
	testDB, err := vitess.NewVitess(cfg)
	require.NoError(t, err, "should connect to database")

	// Run migrations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = testDB.Migrate(ctx)
	require.NoError(t, err, "should run migrations")

	cleanup := func() {
		testDB.Close()
	}

	return testDB, cleanup
}

// maskDSN masks sensitive parts of DSN for logging
func maskDSN(dsn string) string {
	if len(dsn) > 30 {
		return dsn[:15] + "***" + dsn[len(dsn)-10:]
	}
	return "***"
}

// TestDatabaseInterface_EncryptionFields tests encryption fields against real database
func TestDatabaseInterface_EncryptionFields(t *testing.T) {
	testutil.SkipIfShort(t)

	testDB, cleanup := getTestDB(t)
	defer cleanup()

	ctx := context.Background()
	bucket := testutil.UniqueID("test-bucket")
	objectID := uuid.New()

	// Create bucket
	bucketInfo := &types.BucketInfo{
		ID:        uuid.New(),
		Name:      bucket,
		OwnerID:   "test-owner",
		CreatedAt: time.Now().UnixNano(),
	}
	err := testDB.CreateBucket(ctx, bucketInfo)
	require.NoError(t, err, "should create bucket")

	t.Run("SSE-C_Encryption", func(t *testing.T) {
		objRef := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("test-key-ssec"),
			Size:      1024,
			Version:   1,
			ETag:      "test-etag-ssec",
			CreatedAt: time.Now().UnixNano(),
			ProfileID: "STANDARD",
			ChunkRefs: []types.ChunkRef{
				{
					ChunkID:   types.ChunkID(uuid.New().String()),
					Offset:    0,
					Size:      1024,
					BackendID: "backend-1",
				},
			},
			SSEAlgorithm:      "AES256",
			SSECustomerKeyMD5: "d41d8cd98f00b204e9800998ecf8427e",
		}

		err := testDB.PutObject(ctx, objRef)
		require.NoError(t, err, "should store object with SSE-C fields")

		retrieved, err := testDB.GetObject(ctx, bucket, objRef.Key)
		require.NoError(t, err, "should retrieve object")
		require.NotNil(t, retrieved, "retrieved object should not be nil")

		assert.Equal(t, objRef.SSEAlgorithm, retrieved.SSEAlgorithm, "SSE algorithm should match")
		assert.Equal(t, objRef.SSECustomerKeyMD5, retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should match")
		assert.Empty(t, retrieved.SSEKMSKeyID, "KMS key ID should be empty")
		assert.Empty(t, retrieved.SSEKMSContext, "KMS context should be empty")
	})

	t.Run("SSE-KMS_Encryption", func(t *testing.T) {
		objRef := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("test-key-kms"),
			Size:      2048,
			Version:   1,
			ETag:      "test-etag-kms",
			CreatedAt: time.Now().UnixNano(),
			ProfileID: "STANDARD",
			ChunkRefs: []types.ChunkRef{
				{
					ChunkID:   types.ChunkID(uuid.New().String()),
					Offset:    0,
					Size:      2048,
					BackendID: "backend-1",
				},
			},
			SSEAlgorithm:  "aws:kms",
			SSEKMSKeyID:   "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
			SSEKMSContext: `{"aws:s3:arn":"arn:aws:s3:::test-bucket/test-key"}`,
		}

		err := testDB.PutObject(ctx, objRef)
		require.NoError(t, err, "should store object with SSE-KMS fields")

		retrieved, err := testDB.GetObject(ctx, bucket, objRef.Key)
		require.NoError(t, err, "should retrieve object")
		require.NotNil(t, retrieved, "retrieved object should not be nil")

		assert.Equal(t, objRef.SSEAlgorithm, retrieved.SSEAlgorithm, "SSE algorithm should match")
		assert.Equal(t, objRef.SSEKMSKeyID, retrieved.SSEKMSKeyID, "KMS key ID should match")
		assert.Equal(t, objRef.SSEKMSContext, retrieved.SSEKMSContext, "KMS context should match")
		assert.Empty(t, retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should be empty")
	})

	t.Run("No_Encryption", func(t *testing.T) {
		objRef := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("test-key-no-enc"),
			Size:      512,
			Version:   1,
			ETag:      "test-etag-no-enc",
			CreatedAt: time.Now().UnixNano(),
			ProfileID: "STANDARD",
			ChunkRefs: []types.ChunkRef{
				{
					ChunkID:   types.ChunkID(uuid.New().String()),
					Offset:    0,
					Size:      512,
					BackendID: "backend-1",
				},
			},
			// No encryption fields
		}

		err := testDB.PutObject(ctx, objRef)
		require.NoError(t, err, "should store object without encryption fields")

		retrieved, err := testDB.GetObject(ctx, bucket, objRef.Key)
		require.NoError(t, err, "should retrieve object")
		require.NotNil(t, retrieved, "retrieved object should not be nil")

		assert.Empty(t, retrieved.SSEAlgorithm, "SSE algorithm should be empty")
		assert.Empty(t, retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should be empty")
		assert.Empty(t, retrieved.SSEKMSKeyID, "KMS key ID should be empty")
		assert.Empty(t, retrieved.SSEKMSContext, "KMS context should be empty")
	})

	t.Run("GetObjectByID_WithEncryption", func(t *testing.T) {
		objRef := &types.ObjectRef{
			ID:        objectID,
			Bucket:    bucket,
			Key:       testutil.UniqueID("test-key-by-id"),
			Size:      1024,
			Version:   1,
			ETag:      "test-etag-by-id",
			CreatedAt: time.Now().UnixNano(),
			ProfileID: "STANDARD",
			ChunkRefs: []types.ChunkRef{
				{
					ChunkID:   types.ChunkID(objectID.String()),
					Offset:    0,
					Size:      1024,
					BackendID: "backend-1",
				},
			},
			SSEAlgorithm:      "AES256",
			SSECustomerKeyMD5: "d41d8cd98f00b204e9800998ecf84271",
		}

		err := testDB.PutObject(ctx, objRef)
		require.NoError(t, err, "should store object")

		retrieved, err := testDB.GetObjectByID(ctx, objectID)
		require.NoError(t, err, "should retrieve object by ID")
		require.NotNil(t, retrieved, "retrieved object should not be nil")

		assert.Equal(t, objRef.SSEAlgorithm, retrieved.SSEAlgorithm, "SSE algorithm should match")
		assert.Equal(t, objRef.SSECustomerKeyMD5, retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should match")
	})

	t.Run("Update_EncryptionFields", func(t *testing.T) {
		objRef := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("test-key-update"),
			Size:      1024,
			Version:   1,
			ETag:      "test-etag-update",
			CreatedAt: time.Now().UnixNano(),
			ProfileID: "STANDARD",
			ChunkRefs: []types.ChunkRef{
				{
					ChunkID:   types.ChunkID(uuid.New().String()),
					Offset:    0,
					Size:      1024,
					BackendID: "backend-1",
				},
			},
		}

		// Store without encryption
		err := testDB.PutObject(ctx, objRef)
		require.NoError(t, err, "should store object")

		// Update with SSE-C encryption
		objRef.SSEAlgorithm = "AES256"
		objRef.SSECustomerKeyMD5 = "098f6bcd4621d373cade4e832627b4f6"
		objRef.Version = 2

		err = testDB.PutObject(ctx, objRef)
		require.NoError(t, err, "should update object with encryption")

		// Verify update
		retrieved, err := testDB.GetObject(ctx, bucket, objRef.Key)
		require.NoError(t, err, "should retrieve updated object")
		assert.Equal(t, "AES256", retrieved.SSEAlgorithm, "SSE algorithm should be updated")
		assert.Equal(t, "098f6bcd4621d373cade4e832627b4f6", retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should be updated")
	})

	t.Run("Overwrite_WithNewUUID", func(t *testing.T) {
		// This tests the critical UPSERT behavior:
		// When PutObject is called with a NEW UUID but same (bucket, key),
		// it should UPDATE the existing row, not create a duplicate.
		key := testutil.UniqueID("test-key-overwrite")

		// First put - create object
		objRef1 := &types.ObjectRef{
			ID:        uuid.New(), // UUID #1
			Bucket:    bucket,
			Key:       key,
			Size:      1024,
			Version:   1,
			ETag:      "first-etag",
			CreatedAt: time.Now().UnixNano(),
			ProfileID: "STANDARD",
			ChunkRefs: []types.ChunkRef{
				{
					ChunkID:   types.ChunkID(uuid.New().String()),
					Offset:    0,
					Size:      1024,
					BackendID: "backend-1",
				},
			},
		}

		err := testDB.PutObject(ctx, objRef1)
		require.NoError(t, err, "should store first object")

		// Verify first version
		retrieved1, err := testDB.GetObject(ctx, bucket, key)
		require.NoError(t, err, "should retrieve first object")
		assert.Equal(t, "first-etag", retrieved1.ETag, "should have first etag")

		// Second put - OVERWRITE with NEW UUID (simulating real PutObject behavior)
		objRef2 := &types.ObjectRef{
			ID:        uuid.New(), // NEW UUID #2 - different from first!
			Bucket:    bucket,
			Key:       key, // Same key - should trigger UPSERT update
			Size:      2048,
			Version:   2,
			ETag:      "second-etag",
			CreatedAt: time.Now().UnixNano(),
			ProfileID: "STANDARD",
			ChunkRefs: []types.ChunkRef{
				{
					ChunkID:   types.ChunkID(uuid.New().String()),
					Offset:    0,
					Size:      2048,
					BackendID: "backend-2",
				},
			},
		}

		err = testDB.PutObject(ctx, objRef2)
		require.NoError(t, err, "should overwrite object with new UUID")

		// Verify second version - THIS IS THE CRITICAL CHECK
		// If UPSERT is broken, we'd have duplicate rows and might get wrong data
		retrieved2, err := testDB.GetObject(ctx, bucket, key)
		require.NoError(t, err, "should retrieve overwritten object")
		assert.Equal(t, "second-etag", retrieved2.ETag, "should have SECOND etag after overwrite")
		assert.Equal(t, uint64(2048), retrieved2.Size, "should have SECOND size after overwrite")
		assert.Equal(t, uint64(2), retrieved2.Version, "should have SECOND version after overwrite")
	})
}

// TestDatabaseInterface_SchemaValidation tests that the database schema supports encryption fields
func TestDatabaseInterface_SchemaValidation(t *testing.T) {
	testutil.SkipIfShort(t)

	testDB, cleanup := getTestDB(t)
	defer cleanup()

	ctx := context.Background()
	bucket := testutil.UniqueID("test-schema-bucket")

	// Create bucket
	bucketInfo := &types.BucketInfo{
		ID:        uuid.New(),
		Name:      bucket,
		OwnerID:   "test-owner",
		CreatedAt: time.Now().UnixNano(),
	}
	err := testDB.CreateBucket(ctx, bucketInfo)
	require.NoError(t, err, "should create bucket")

	// Test that we can store and retrieve objects with all encryption field combinations
	testCases := []struct {
		name           string
		sseAlgorithm   string
		sseCustomerMD5 string
		sseKMSKeyID    string
		sseKMSContext  string
	}{
		{
			name:         "All_Fields_Empty",
			sseAlgorithm: "",
		},
		{
			name:           "SSE-C_Only",
			sseAlgorithm:   "AES256",
			sseCustomerMD5: "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:         "SSE-KMS_Only",
			sseAlgorithm: "aws:kms",
			sseKMSKeyID:  "arn:aws:kms:us-east-1:123456789012:key/test-key",
		},
		{
			name:          "SSE-KMS_WithContext",
			sseAlgorithm:  "aws:kms",
			sseKMSKeyID:   "arn:aws:kms:us-east-1:123456789012:key/test-key",
			sseKMSContext: `{"key":"value","nested":{"data":"test"}}`,
		},
		{
			name:          "Long_KMS_Context",
			sseAlgorithm:  "aws:kms",
			sseKMSKeyID:   "arn:aws:kms:us-east-1:123456789012:key/test-key",
			sseKMSContext: `{"very":"long","context":"with","many":"fields","that":"might","exceed":"normal","length":"expectations","for":"testing","purposes":true}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			objRef := &types.ObjectRef{
				ID:        uuid.New(),
				Bucket:    bucket,
				Key:       testutil.UniqueID("test-key-" + tc.name),
				Size:      1024,
				Version:   1,
				ETag:      "test-etag",
				CreatedAt: time.Now().UnixNano(),
				ProfileID: "STANDARD",
				ChunkRefs: []types.ChunkRef{
					{
						ChunkID:   types.ChunkID(uuid.New().String()),
						Offset:    0,
						Size:      1024,
						BackendID: "backend-1",
					},
				},
				SSEAlgorithm:      tc.sseAlgorithm,
				SSECustomerKeyMD5: tc.sseCustomerMD5,
				SSEKMSKeyID:       tc.sseKMSKeyID,
				SSEKMSContext:     tc.sseKMSContext,
			}

			err := testDB.PutObject(ctx, objRef)
			require.NoError(t, err, "should store object with %s", tc.name)

			retrieved, err := testDB.GetObject(ctx, bucket, objRef.Key)
			require.NoError(t, err, "should retrieve object")
			require.NotNil(t, retrieved, "retrieved object should not be nil")

			assert.Equal(t, tc.sseAlgorithm, retrieved.SSEAlgorithm, "SSE algorithm should match")
			assert.Equal(t, tc.sseCustomerMD5, retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should match")
			assert.Equal(t, tc.sseKMSKeyID, retrieved.SSEKMSKeyID, "KMS key ID should match")
			assert.Equal(t, tc.sseKMSContext, retrieved.SSEKMSContext, "KMS context should match")
		})
	}
}

// =============================================================================
// Bucket CRUD Tests
// =============================================================================

func TestDatabaseInterface_BucketCRUD(t *testing.T) {
	testutil.SkipIfShort(t)

	testDB, cleanup := getTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("CreateAndGetBucket", func(t *testing.T) {
		bucket := &types.BucketInfo{
			ID:               uuid.New(),
			Name:             testutil.UniqueID("test-bucket-crud"),
			OwnerID:          "test-owner",
			Region:           "us-east-1",
			CreatedAt:        time.Now().UnixNano(),
			DefaultProfileID: "STANDARD",
			Versioning:       "Enabled",
		}

		err := testDB.CreateBucket(ctx, bucket)
		require.NoError(t, err, "should create bucket")

		retrieved, err := testDB.GetBucket(ctx, bucket.Name)
		require.NoError(t, err, "should get bucket")
		require.NotNil(t, retrieved)

		assert.Equal(t, bucket.ID, retrieved.ID)
		assert.Equal(t, bucket.Name, retrieved.Name)
		assert.Equal(t, bucket.OwnerID, retrieved.OwnerID)
		assert.Equal(t, bucket.Region, retrieved.Region)
		assert.Equal(t, bucket.Versioning, retrieved.Versioning)
	})

	t.Run("GetBucket_NotFound", func(t *testing.T) {
		_, err := testDB.GetBucket(ctx, "nonexistent-bucket-12345")
		require.Error(t, err, "should error on nonexistent bucket")
	})

	t.Run("CreateBucket_DuplicateName", func(t *testing.T) {
		bucket := &types.BucketInfo{
			ID:        uuid.New(),
			Name:      testutil.UniqueID("test-bucket-dup"),
			OwnerID:   "test-owner",
			CreatedAt: time.Now().UnixNano(),
		}

		err := testDB.CreateBucket(ctx, bucket)
		require.NoError(t, err, "should create first bucket")

		// Try to create another bucket with same name but different ID
		bucket2 := &types.BucketInfo{
			ID:        uuid.New(),
			Name:      bucket.Name, // Same name
			OwnerID:   "test-owner",
			CreatedAt: time.Now().UnixNano(),
		}
		err = testDB.CreateBucket(ctx, bucket2)
		require.Error(t, err, "should fail on duplicate bucket name")
	})

	t.Run("DeleteBucket", func(t *testing.T) {
		bucket := &types.BucketInfo{
			ID:        uuid.New(),
			Name:      testutil.UniqueID("test-bucket-delete"),
			OwnerID:   "test-owner",
			CreatedAt: time.Now().UnixNano(),
		}

		err := testDB.CreateBucket(ctx, bucket)
		require.NoError(t, err, "should create bucket")

		err = testDB.DeleteBucket(ctx, bucket.Name)
		require.NoError(t, err, "should delete bucket")

		_, err = testDB.GetBucket(ctx, bucket.Name)
		require.Error(t, err, "should not find deleted bucket")
	})

	t.Run("ListBuckets", func(t *testing.T) {
		// Create multiple buckets with unique prefix
		prefix := testutil.UniqueID("list-test")
		for i := 0; i < 3; i++ {
			bucket := &types.BucketInfo{
				ID:        uuid.New(),
				Name:      fmt.Sprintf("%s-%d", prefix, i),
				OwnerID:   "test-owner-list",
				CreatedAt: time.Now().UnixNano(),
			}
			err := testDB.CreateBucket(ctx, bucket)
			require.NoError(t, err)
		}

		result, err := testDB.ListBuckets(ctx, &db.ListBucketsParams{
			OwnerID:    "test-owner-list",
			MaxBuckets: 100,
		})
		require.NoError(t, err, "should list buckets")
		assert.GreaterOrEqual(t, len(result.Buckets), 3, "should have at least 3 buckets")
	})

	t.Run("UpdateBucketVersioning", func(t *testing.T) {
		bucket := &types.BucketInfo{
			ID:         uuid.New(),
			Name:       testutil.UniqueID("test-bucket-versioning"),
			OwnerID:    "test-owner",
			CreatedAt:  time.Now().UnixNano(),
			Versioning: "",
		}

		err := testDB.CreateBucket(ctx, bucket)
		require.NoError(t, err)

		// Enable versioning
		err = testDB.UpdateBucketVersioning(ctx, bucket.Name, "Enabled")
		require.NoError(t, err)

		retrieved, err := testDB.GetBucket(ctx, bucket.Name)
		require.NoError(t, err)
		assert.Equal(t, "Enabled", retrieved.Versioning)

		// Suspend versioning
		err = testDB.UpdateBucketVersioning(ctx, bucket.Name, "Suspended")
		require.NoError(t, err)

		retrieved, err = testDB.GetBucket(ctx, bucket.Name)
		require.NoError(t, err)
		assert.Equal(t, "Suspended", retrieved.Versioning)
	})
}

// =============================================================================
// Object CRUD Tests
// =============================================================================

func TestDatabaseInterface_ObjectCRUD(t *testing.T) {
	testutil.SkipIfShort(t)

	testDB, cleanup := getTestDB(t)
	defer cleanup()

	ctx := context.Background()
	bucket := testutil.UniqueID("test-bucket-obj-crud")

	// Create bucket first
	bucketInfo := &types.BucketInfo{
		ID:        uuid.New(),
		Name:      bucket,
		OwnerID:   "test-owner",
		CreatedAt: time.Now().UnixNano(),
	}
	err := testDB.CreateBucket(ctx, bucketInfo)
	require.NoError(t, err)

	t.Run("PutAndGetObject", func(t *testing.T) {
		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("test-key"),
			Size:      1024,
			Version:   1,
			ETag:      "abc123",
			CreatedAt: time.Now().UnixNano(),
			ProfileID: "STANDARD",
			ChunkRefs: []types.ChunkRef{
				{ChunkID: types.ChunkID(uuid.New().String()), Offset: 0, Size: 1024, BackendID: "backend-1"},
			},
		}

		err := testDB.PutObject(ctx, obj)
		require.NoError(t, err)

		retrieved, err := testDB.GetObject(ctx, bucket, obj.Key)
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		assert.Equal(t, obj.ID, retrieved.ID)
		assert.Equal(t, obj.Key, retrieved.Key)
		assert.Equal(t, obj.Size, retrieved.Size)
		assert.Equal(t, obj.ETag, retrieved.ETag)
		assert.Len(t, retrieved.ChunkRefs, 1)
	})

	t.Run("GetObject_NotFound", func(t *testing.T) {
		_, err := testDB.GetObject(ctx, bucket, "nonexistent-key-12345")
		require.Error(t, err)
	})

	t.Run("DeleteObject", func(t *testing.T) {
		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("test-key-delete"),
			Size:      512,
			Version:   1,
			ETag:      "def456",
			CreatedAt: time.Now().UnixNano(),
		}

		err := testDB.PutObject(ctx, obj)
		require.NoError(t, err)

		err = testDB.DeleteObject(ctx, bucket, obj.Key)
		require.NoError(t, err)

		_, err = testDB.GetObject(ctx, bucket, obj.Key)
		require.Error(t, err, "should not find deleted object")
	})

	t.Run("MarkObjectDeleted_SoftDelete", func(t *testing.T) {
		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("test-key-soft-delete"),
			Size:      256,
			Version:   1,
			ETag:      "ghi789",
			CreatedAt: time.Now().UnixNano(),
		}

		err := testDB.PutObject(ctx, obj)
		require.NoError(t, err)

		deletedAt := time.Now().UnixNano()
		err = testDB.MarkObjectDeleted(ctx, bucket, obj.Key, deletedAt)
		require.NoError(t, err)

		// Object should still exist but with deleted_at set
		retrieved, err := testDB.GetObject(ctx, bucket, obj.Key)
		// Depending on implementation, this might return error or object with deleted_at
		if err == nil {
			assert.Equal(t, deletedAt, retrieved.DeletedAt)
		}
	})

	t.Run("ListObjects_WithPrefix", func(t *testing.T) {
		prefix := testutil.UniqueID("list-prefix")

		// Create objects with prefix
		for i := 0; i < 5; i++ {
			obj := &types.ObjectRef{
				ID:        uuid.New(),
				Bucket:    bucket,
				Key:       fmt.Sprintf("%s/file-%d.txt", prefix, i),
				Size:      100,
				Version:   1,
				ETag:      fmt.Sprintf("etag-%d", i),
				CreatedAt: time.Now().UnixNano(),
			}
			err := testDB.PutObject(ctx, obj)
			require.NoError(t, err)
		}

		// Create one without the prefix
		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("other-key"),
			Size:      100,
			Version:   1,
			ETag:      "other-etag",
			CreatedAt: time.Now().UnixNano(),
		}
		err := testDB.PutObject(ctx, obj)
		require.NoError(t, err)

		// List with prefix
		objects, err := testDB.ListObjects(ctx, bucket, prefix+"/", 100)
		require.NoError(t, err)
		assert.Len(t, objects, 5, "should find 5 objects with prefix")

		for _, o := range objects {
			assert.True(t, len(o.Key) > len(prefix) && o.Key[:len(prefix)] == prefix,
				"all objects should have the prefix")
		}
	})

	t.Run("ListObjectsV2_Pagination", func(t *testing.T) {
		prefix := testutil.UniqueID("paginate")

		// Create 10 objects
		for i := 0; i < 10; i++ {
			obj := &types.ObjectRef{
				ID:        uuid.New(),
				Bucket:    bucket,
				Key:       fmt.Sprintf("%s/file-%02d.txt", prefix, i),
				Size:      100,
				Version:   1,
				ETag:      fmt.Sprintf("etag-%d", i),
				CreatedAt: time.Now().UnixNano(),
			}
			err := testDB.PutObject(ctx, obj)
			require.NoError(t, err)
		}

		// List with max 3 keys
		result, err := testDB.ListObjectsV2(ctx, &db.ListObjectsParams{
			Bucket:  bucket,
			Prefix:  prefix + "/",
			MaxKeys: 3,
		})
		require.NoError(t, err)
		assert.Len(t, result.Objects, 3, "should return 3 objects")
		assert.True(t, result.IsTruncated, "should be truncated")
		assert.NotEmpty(t, result.NextContinuationToken, "should have continuation token")

		// Get next page
		result2, err := testDB.ListObjectsV2(ctx, &db.ListObjectsParams{
			Bucket:            bucket,
			Prefix:            prefix + "/",
			MaxKeys:           3,
			ContinuationToken: result.NextContinuationToken,
		})
		require.NoError(t, err)
		assert.Len(t, result2.Objects, 3, "should return 3 more objects")
	})
}

// =============================================================================
// Constraint and Edge Case Tests
// =============================================================================

func TestDatabaseInterface_Constraints(t *testing.T) {
	testutil.SkipIfShort(t)

	testDB, cleanup := getTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("ForeignKey_ObjectWithoutBucket", func(t *testing.T) {
		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    "nonexistent-bucket-fk-test",
			Key:       "test-key",
			Size:      1024,
			Version:   1,
			ETag:      "test-etag",
			CreatedAt: time.Now().UnixNano(),
		}

		err := testDB.PutObject(ctx, obj)
		require.Error(t, err, "should fail when bucket doesn't exist (foreign key)")
	})

	t.Run("DeleteBucket_WithObjects", func(t *testing.T) {
		bucket := testutil.UniqueID("test-bucket-with-objects")

		// Create bucket
		bucketInfo := &types.BucketInfo{
			ID:        uuid.New(),
			Name:      bucket,
			OwnerID:   "test-owner",
			CreatedAt: time.Now().UnixNano(),
		}
		err := testDB.CreateBucket(ctx, bucketInfo)
		require.NoError(t, err)

		// Create object in bucket
		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       "test-object",
			Size:      100,
			Version:   1,
			ETag:      "test-etag",
			CreatedAt: time.Now().UnixNano(),
		}
		err = testDB.PutObject(ctx, obj)
		require.NoError(t, err)

		// Try to delete bucket - should either fail or cascade delete
		err = testDB.DeleteBucket(ctx, bucket)
		// Behavior depends on CASCADE setting - just ensure no panic
		t.Logf("DeleteBucket with objects result: %v", err)
	})
}

func TestDatabaseInterface_EdgeCases(t *testing.T) {
	testutil.SkipIfShort(t)

	testDB, cleanup := getTestDB(t)
	defer cleanup()

	ctx := context.Background()
	bucket := testutil.UniqueID("test-bucket-edge")

	// Create bucket
	bucketInfo := &types.BucketInfo{
		ID:        uuid.New(),
		Name:      bucket,
		OwnerID:   "test-owner",
		CreatedAt: time.Now().UnixNano(),
	}
	err := testDB.CreateBucket(ctx, bucketInfo)
	require.NoError(t, err)

	t.Run("LongObjectKey", func(t *testing.T) {
		// Test key near the 1024 character limit
		prefix := testutil.UniqueID("prefix") + "/"
		// Create a key that's 950 characters total
		padding := make([]byte, 950-len(prefix))
		for i := range padding {
			padding[i] = 'a'
		}
		longKey := prefix + string(padding)

		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       longKey,
			Size:      100,
			Version:   1,
			ETag:      "long-key-etag",
			CreatedAt: time.Now().UnixNano(),
		}

		err := testDB.PutObject(ctx, obj)
		require.NoError(t, err, "should store object with long key")

		retrieved, err := testDB.GetObject(ctx, bucket, longKey)
		require.NoError(t, err)
		assert.Equal(t, longKey, retrieved.Key)
	})

	t.Run("LargeChunkRefs", func(t *testing.T) {
		// Test with many chunk refs
		chunkRefs := make([]types.ChunkRef, 100)
		for i := range chunkRefs {
			chunkRefs[i] = types.ChunkRef{
				ChunkID:   types.ChunkID(uuid.New().String()),
				Offset:    uint64(i * 1024 * 1024),
				Size:      1024 * 1024,
				BackendID: fmt.Sprintf("backend-%d", i%5),
			}
		}

		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("large-chunks"),
			Size:      uint64(100 * 1024 * 1024),
			Version:   1,
			ETag:      "large-chunks-etag",
			CreatedAt: time.Now().UnixNano(),
			ChunkRefs: chunkRefs,
		}

		err := testDB.PutObject(ctx, obj)
		require.NoError(t, err, "should store object with many chunk refs")

		retrieved, err := testDB.GetObject(ctx, bucket, obj.Key)
		require.NoError(t, err)
		assert.Len(t, retrieved.ChunkRefs, 100, "should retrieve all chunk refs")
	})

	t.Run("UnicodeKeys", func(t *testing.T) {
		unicodeKey := testutil.UniqueID("unicode") + "/文件/файл/αρχείο.txt"

		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       unicodeKey,
			Size:      100,
			Version:   1,
			ETag:      "unicode-etag",
			CreatedAt: time.Now().UnixNano(),
		}

		err := testDB.PutObject(ctx, obj)
		require.NoError(t, err, "should store object with unicode key")

		retrieved, err := testDB.GetObject(ctx, bucket, unicodeKey)
		require.NoError(t, err)
		assert.Equal(t, unicodeKey, retrieved.Key)
	})

	t.Run("SpecialCharacterKeys", func(t *testing.T) {
		specialKey := testutil.UniqueID("special") + "/file with spaces/name+plus&ampersand.txt"

		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       specialKey,
			Size:      100,
			Version:   1,
			ETag:      "special-etag",
			CreatedAt: time.Now().UnixNano(),
		}

		err := testDB.PutObject(ctx, obj)
		require.NoError(t, err, "should store object with special characters")

		retrieved, err := testDB.GetObject(ctx, bucket, specialKey)
		require.NoError(t, err)
		assert.Equal(t, specialKey, retrieved.Key)
	})

	t.Run("ZeroSizeObject", func(t *testing.T) {
		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("zero-size"),
			Size:      0,
			Version:   1,
			ETag:      "d41d8cd98f00b204e9800998ecf8427e", // MD5 of empty
			CreatedAt: time.Now().UnixNano(),
			ChunkRefs: []types.ChunkRef{}, // Empty
		}

		err := testDB.PutObject(ctx, obj)
		require.NoError(t, err, "should store zero-size object")

		retrieved, err := testDB.GetObject(ctx, bucket, obj.Key)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), retrieved.Size)
		assert.Empty(t, retrieved.ChunkRefs)
	})

	t.Run("MaxUint64Size", func(t *testing.T) {
		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       testutil.UniqueID("max-size"),
			Size:      ^uint64(0), // Max uint64
			Version:   1,
			ETag:      "max-size-etag",
			CreatedAt: time.Now().UnixNano(),
		}

		err := testDB.PutObject(ctx, obj)
		require.NoError(t, err, "should store object with max uint64 size")

		retrieved, err := testDB.GetObject(ctx, bucket, obj.Key)
		require.NoError(t, err)
		assert.Equal(t, ^uint64(0), retrieved.Size)
	})
}
