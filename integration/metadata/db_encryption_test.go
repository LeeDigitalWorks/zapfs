//go:build integration

package metadata

import (
	"context"
	"testing"
	"time"

	"zapfs/integration/testutil"
	"zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Database Encryption Field Tests
// =============================================================================

// TestObjectEncryptionFields tests that encryption metadata fields can be stored and retrieved
func TestObjectEncryptionFields(t *testing.T) {
	t.Parallel()

	// Use memory database for testing (no external dependencies)
	testDB, cleanup := getTestDB(t)
	defer cleanup()
	ctx := context.Background()

	bucket := testutil.UniqueID("test-bucket")
	key := testutil.UniqueID("test-key")
	objectID := uuid.New()

	// Create a bucket first (required for foreign key constraint in real DB)
	// For memory DB, this might not be strictly necessary, but good practice
	bucketInfo := &types.BucketInfo{
		ID:        uuid.New(),
		Name:      bucket,
		OwnerID:   "test-owner",
		CreatedAt: time.Now().UnixNano(),
	}
	err := testDB.CreateBucket(ctx, bucketInfo)
	require.NoError(t, err, "should create bucket")

	// Test 1: PutObject with SSE-C encryption fields
	t.Run("SSE-C_Encryption", func(t *testing.T) {
		objRef := &types.ObjectRef{
			ID:        objectID,
			Bucket:    bucket,
			Key:       key + "-ssec",
			Size:      1024,
			Version:   1,
			ETag:      "test-etag-ssec",
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
			// SSE-C encryption fields
			SSEAlgorithm:      "AES256",
			SSECustomerKeyMD5: "d41d8cd98f00b204e9800998ecf8427e", // MD5 of empty string
		}

		err := testDB.PutObject(ctx, objRef)
		require.NoError(t, err, "should store object with SSE-C fields")

		// Retrieve and verify
		retrieved, err := testDB.GetObject(ctx, bucket, objRef.Key)
		require.NoError(t, err, "should retrieve object")
		require.NotNil(t, retrieved, "retrieved object should not be nil")

		assert.Equal(t, objRef.SSEAlgorithm, retrieved.SSEAlgorithm, "SSE algorithm should match")
		assert.Equal(t, objRef.SSECustomerKeyMD5, retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should match")
		assert.Empty(t, retrieved.SSEKMSKeyID, "KMS key ID should be empty")
		assert.Empty(t, retrieved.SSEKMSContext, "KMS context should be empty")
	})

	// Test 2: PutObject with SSE-KMS encryption fields
	t.Run("SSE-KMS_Encryption", func(t *testing.T) {
		objRef := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       key + "-kms",
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
			// SSE-KMS encryption fields
			SSEAlgorithm:  "aws:kms",
			SSEKMSKeyID:   "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
			SSEKMSContext: `{"aws:s3:arn":"arn:aws:s3:::test-bucket/test-key"}`,
		}

		err := testDB.PutObject(ctx, objRef)
		require.NoError(t, err, "should store object with SSE-KMS fields")

		// Retrieve and verify
		retrieved, err := testDB.GetObject(ctx, bucket, objRef.Key)
		require.NoError(t, err, "should retrieve object")
		require.NotNil(t, retrieved, "retrieved object should not be nil")

		assert.Equal(t, objRef.SSEAlgorithm, retrieved.SSEAlgorithm, "SSE algorithm should match")
		assert.Equal(t, objRef.SSEKMSKeyID, retrieved.SSEKMSKeyID, "KMS key ID should match")
		assert.Equal(t, objRef.SSEKMSContext, retrieved.SSEKMSContext, "KMS context should match")
		assert.Empty(t, retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should be empty")
	})

	// Test 3: PutObject with no encryption (empty fields)
	t.Run("No_Encryption", func(t *testing.T) {
		objRef := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       key + "-no-enc",
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
			// No encryption fields set (all empty)
		}

		err := testDB.PutObject(ctx, objRef)
		require.NoError(t, err, "should store object without encryption fields")

		// Retrieve and verify
		retrieved, err := testDB.GetObject(ctx, bucket, objRef.Key)
		require.NoError(t, err, "should retrieve object")
		require.NotNil(t, retrieved, "retrieved object should not be nil")

		assert.Empty(t, retrieved.SSEAlgorithm, "SSE algorithm should be empty")
		assert.Empty(t, retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should be empty")
		assert.Empty(t, retrieved.SSEKMSKeyID, "KMS key ID should be empty")
		assert.Empty(t, retrieved.SSEKMSContext, "KMS context should be empty")
	})

	// Test 4: Update object encryption fields
	t.Run("Update_Encryption", func(t *testing.T) {
		objRef := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       key + "-update",
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
		objRef.SSECustomerKeyMD5 = "5eb63bbbe01eeed093cb22bb8f5acdc3"
		objRef.Version = 2

		err = testDB.PutObject(ctx, objRef)
		require.NoError(t, err, "should update object with encryption")

		// Verify update
		retrieved, err := testDB.GetObject(ctx, bucket, objRef.Key)
		require.NoError(t, err, "should retrieve updated object")
		assert.Equal(t, "AES256", retrieved.SSEAlgorithm, "SSE algorithm should be updated")
		assert.Equal(t, "5eb63bbbe01eeed093cb22bb8f5acdc3", retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should be updated")
	})
}

// TestObjectEncryptionFields_GetByID tests GetObjectByID with encryption fields
func TestObjectEncryptionFields_GetByID(t *testing.T) {
	t.Parallel()

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

	objRef := &types.ObjectRef{
		ID:        objectID,
		Bucket:    bucket,
		Key:       testutil.UniqueID("test-key"),
		Size:      1024,
		Version:   1,
		ETag:      "test-etag",
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

	err = testDB.PutObject(ctx, objRef)
	require.NoError(t, err, "should store object")

	// Retrieve by ID
	retrieved, err := testDB.GetObjectByID(ctx, objectID)
	require.NoError(t, err, "should retrieve object by ID")
	require.NotNil(t, retrieved, "retrieved object should not be nil")

	assert.Equal(t, objRef.SSEAlgorithm, retrieved.SSEAlgorithm, "SSE algorithm should match")
	assert.Equal(t, objRef.SSECustomerKeyMD5, retrieved.SSECustomerKeyMD5, "SSE customer key MD5 should match")
}
