// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package object

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/cache"
	"github.com/LeeDigitalWorks/zapfs/pkg/events"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/encryption"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/storage"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"

	"github.com/google/uuid"
)

// serviceImpl implements the Service interface
type serviceImpl struct {
	db             db.DB
	storage        *storage.Coordinator
	encryption     *encryption.Handler
	bucketStore    *cache.BucketStore
	defaultProfile string
	profiles       *types.ProfileSet
	crrHook        CRRHook
	emitter        *events.Emitter
}

// CRRHook defines callbacks for cross-region replication (enterprise feature).
// The bucketInfo parameter is of type *s3types.Bucket but uses interface{} to avoid import cycles.
type CRRHook interface {
	AfterPutObject(ctx context.Context, bucketInfo interface{}, key, etag string, size int64)
	AfterDeleteObject(ctx context.Context, bucketInfo interface{}, key string)
}

// Config holds configuration for the object service
type Config struct {
	DB             db.DB
	Storage        *storage.Coordinator
	Encryption     *encryption.Handler
	BucketStore    *cache.BucketStore
	DefaultProfile string
	Profiles       *types.ProfileSet
	CRRHook        CRRHook         // Optional, enterprise feature
	Emitter        *events.Emitter // Optional, for S3 event notifications
}

// NewService creates a new object service
func NewService(cfg Config) (Service, error) {
	if cfg.DB == nil {
		return nil, newValidationError("DB is required")
	}
	if cfg.Storage == nil {
		return nil, newValidationError("Storage is required")
	}
	if cfg.Profiles == nil {
		return nil, newValidationError("Profiles is required")
	}

	if cfg.DefaultProfile == "" {
		cfg.DefaultProfile = "STANDARD"
	}

	return &serviceImpl{
		db:             cfg.DB,
		storage:        cfg.Storage,
		encryption:     cfg.Encryption,
		bucketStore:    cfg.BucketStore,
		defaultProfile: cfg.DefaultProfile,
		profiles:       cfg.Profiles,
		crrHook:        cfg.CRRHook,
		emitter:        cfg.Emitter,
	}, nil
}

// PutObject stores an object
func (s *serviceImpl) PutObject(ctx context.Context, req *PutObjectRequest) (*PutObjectResult, error) {
	// Validate storage profile
	profileName := req.StorageClass
	if profileName == "" {
		profileName = s.defaultProfile
	}

	profile, exists := s.profiles.Get(profileName)
	if !exists {
		return nil, &Error{
			Code:    ErrCodeInvalidStorageClass,
			Message: "storage profile not found: " + profileName,
		}
	}

	// Validate encryption params
	if req.SSEC != nil && req.SSEKMS != nil {
		return nil, &Error{
			Code:    ErrCodeInvalidEncryption,
			Message: "cannot use both SSE-C and SSE-KMS",
		}
	}

	// Read body (required for encryption or unknown content length)
	var body []byte
	var err error

	needsBuffering := req.SSEC != nil || req.SSEKMS != nil || req.ContentLength < 0
	if needsBuffering {
		body, err = io.ReadAll(req.Body)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, &Error{
					Code:    ErrCodeIncompleteBody,
					Message: "request cancelled while reading body",
					Err:     err,
				}
			}
			return nil, newInternalError(err)
		}
	}

	// Compute ETag from plaintext (before encryption)
	var etag string
	var originalSize uint64
	var dataToStore io.Reader
	var storageSize uint64
	var encryptionMetadata *encryption.Metadata
	var streamingHasher hash.Hash // For streaming path, to compute ETag after write

	if needsBuffering {
		h := utils.Md5PoolGetHasher()
		h.Write(body)
		etag = hex.EncodeToString(h.Sum(nil))
		utils.Md5PoolPutHasher(h)
		originalSize = uint64(len(body))

		// Encrypt if needed
		if req.SSEC != nil || req.SSEKMS != nil {
			params := &encryption.Params{}
			if req.SSEC != nil {
				params.SSEC = &encryption.SSECParams{
					Algorithm: req.SSEC.Algorithm,
					Key:       req.SSEC.Key,
					KeyMD5:    req.SSEC.KeyMD5,
				}
			}
			if req.SSEKMS != nil {
				params.SSEKMS = &encryption.SSEKMSParams{
					KeyID:   req.SSEKMS.KeyID,
					Context: req.SSEKMS.Context,
				}
			}

			result, err := s.encryption.Encrypt(ctx, body, params)
			if err != nil {
				// Check for KMS-specific errors and return appropriate error codes
				errStr := err.Error()
				if strings.Contains(errStr, "KMS key not found") ||
					strings.Contains(errStr, "KMS key disabled") {
					return nil, &Error{
						Code:    ErrCodeKMSKeyNotFound,
						Message: err.Error(),
						Err:     err,
					}
				}
				if strings.Contains(errStr, "KMS service not available") {
					return nil, &Error{
						Code:    ErrCodeKMSError,
						Message: err.Error(),
						Err:     err,
					}
				}
				return nil, newInternalError(err)
			}
			dataToStore = bytes.NewReader(result.Ciphertext)
			storageSize = uint64(len(result.Ciphertext))
			encryptionMetadata = result.Metadata
		} else {
			dataToStore = bytes.NewReader(body)
			storageSize = originalSize
		}
	} else {
		// Streaming path - compute ETag while writing using TeeReader
		streamingHasher = utils.Md5PoolGetHasher()
		dataToStore = io.TeeReader(req.Body, streamingHasher)
		storageSize = uint64(req.ContentLength)
		originalSize = storageSize
	}

	// Ensure streaming hasher is returned to pool when done
	defer func() {
		if streamingHasher != nil {
			utils.Md5PoolPutHasher(streamingHasher)
		}
	}()

	// Generate object ID
	objectID := uuid.New()

	// Write to storage
	writeResult, err := s.storage.WriteObject(ctx, &storage.WriteRequest{
		Bucket:      req.Bucket,
		ObjectID:    objectID.String(),
		Body:        dataToStore,
		Size:        storageSize,
		ProfileName: profileName,
		Replication: profile.Replication,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, &Error{
				Code:    ErrCodeIncompleteBody,
				Message: "request cancelled during upload",
				Err:     err,
			}
		}
		return nil, newInternalError(err)
	}

	// For streaming path, compute ETag now (after data has been fully read by WriteObject)
	if streamingHasher != nil {
		etag = hex.EncodeToString(streamingHasher.Sum(nil))
	}

	// Check if bucket has versioning enabled
	versioningEnabled := false
	if s.bucketStore != nil {
		if bucketInfo, exists := s.bucketStore.GetBucket(req.Bucket); exists {
			versioningEnabled = bucketInfo.Versioning == s3types.VersioningEnabled
		}
	}

	// Build object reference
	now := time.Now().Unix()
	objRef := &types.ObjectRef{
		ID:        objectID,
		Bucket:    req.Bucket,
		Key:       req.Key,
		Size:      originalSize,
		Version:   1,
		ETag:      etag,
		ProfileID: profileName,
		CreatedAt: now,
		ChunkRefs: writeResult.ChunkRefs,
		IsLatest:  versioningEnabled, // Set IsLatest for versioned objects
	}

	// Set encryption metadata
	if encryptionMetadata != nil {
		objRef.SSEAlgorithm = encryptionMetadata.Algorithm
		objRef.SSECustomerKeyMD5 = encryptionMetadata.CustomerKeyMD5
		objRef.SSEKMSKeyID = encryptionMetadata.KMSKeyID
		if encryptionMetadata.KMSKeyID != "" {
			objRef.SSEKMSContext = encryption.BuildStoredKMSContext(
				encryptionMetadata.KMSContext,
				encryptionMetadata.DEKCiphertext,
			)
		}
	}

	// Store metadata and register chunks atomically
	err = s.db.WithTx(ctx, func(tx db.TxStore) error {
		// Insert object
		if err := tx.PutObject(ctx, objRef); err != nil {
			return err
		}

		// Register chunks in chunk_registry and track replicas
		for _, ref := range writeResult.ChunkRefs {
			// Increment ref_count (or insert with ref_count=1)
			if err := tx.IncrementChunkRefCount(ctx, ref.ChunkID.String(), int64(ref.Size)); err != nil {
				return err
			}
			// Track replica location
			if ref.FileServerAddr != "" {
				if err := tx.AddChunkReplica(ctx, ref.ChunkID.String(), ref.FileServerAddr, ref.BackendID); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, newInternalError(err)
	}

	// Trigger CRR hook if configured
	if s.crrHook != nil && s.bucketStore != nil {
		bucketInfo, _ := s.bucketStore.GetBucket(req.Bucket)
		s.crrHook.AfterPutObject(ctx, &bucketInfo, req.Key, etag, int64(writeResult.Size))
	}

	// Emit S3 event notification
	if s.emitter != nil {
		s.emitter.EmitObjectCreated(ctx, events.EventObjectCreatedPut,
			req.Bucket, req.Key, int64(originalSize), etag, objectID.String(), req.Owner, "", "")
	}

	result := &PutObjectResult{
		ETag: etag,
	}

	// Set version ID if versioning is enabled
	if versioningEnabled {
		result.VersionID = objectID.String()
	}

	if encryptionMetadata != nil {
		result.SSEAlgorithm = encryptionMetadata.Algorithm
		result.SSECustomerKeyMD5 = encryptionMetadata.CustomerKeyMD5
		result.SSEKMSKeyID = encryptionMetadata.KMSKeyID
		result.SSEKMSContext = encryptionMetadata.KMSContext
	}

	return result, nil
}

// GetObject retrieves an object
func (s *serviceImpl) GetObject(ctx context.Context, req *GetObjectRequest) (*GetObjectResult, error) {
	var objRef *types.ObjectRef
	var err error

	// Look up object metadata - use version-specific lookup if versionId is provided
	if req.VersionID != "" {
		objRef, err = s.db.GetObjectVersion(ctx, req.Bucket, req.Key, req.VersionID)
	} else {
		objRef, err = s.db.GetObject(ctx, req.Bucket, req.Key)
	}
	if err != nil {
		if errors.Is(err, db.ErrObjectNotFound) {
			return nil, newNotFoundError("object")
		}
		return nil, newInternalError(err)
	}

	// For versioned requests, deleted objects (delete markers) are still accessible
	// but should return 405 Method Not Allowed or similar
	if objRef.IsDeleted() && req.VersionID == "" {
		return nil, newNotFoundError("object")
	}

	// Check conditional headers
	lastModified := time.Unix(0, objRef.CreatedAt).UTC()
	condResult := s.checkConditionalHeaders(req, objRef.ETag, lastModified)
	if !condResult.ShouldProceed {
		if condResult.NotModified {
			return nil, &Error{
				Code:    ErrCodeNotModified,
				Message: "not modified",
			}
		}
		return nil, &Error{
			Code:    ErrCodePreconditionFailed,
			Message: "precondition failed",
		}
	}

	// Validate encryption parameters
	isSSECEncrypted := objRef.SSEAlgorithm == "AES256" && objRef.SSECustomerKeyMD5 != ""
	isSSEKMSEncrypted := objRef.SSEAlgorithm == "aws:kms" && objRef.SSEKMSKeyID != ""

	if isSSECEncrypted {
		if req.SSEC == nil {
			return nil, &Error{
				Code:    ErrCodeInvalidEncryption,
				Message: "SSE-C key required for encrypted object",
			}
		}
		if req.SSEC.KeyMD5 != objRef.SSECustomerKeyMD5 {
			return nil, &Error{
				Code:    ErrCodeInvalidEncryption,
				Message: "SSE-C key MD5 mismatch",
			}
		}
	}

	if isSSEKMSEncrypted && s.encryption != nil && !s.encryption.HasKMS() {
		return nil, &Error{
			Code:    ErrCodeKMSError,
			Message: "KMS service not available",
		}
	}

	// Validate SSE-KMS key ID if provided in request
	if req.SSEKMSKeyID != "" && isSSEKMSEncrypted && req.SSEKMSKeyID != objRef.SSEKMSKeyID {
		return nil, &Error{
			Code:    ErrCodeInvalidEncryption,
			Message: "SSE-KMS key ID mismatch",
		}
	}

	// Determine range
	var offset, length uint64
	var isPartial bool
	if req.Range != nil {
		offset = req.Range.Start
		length = req.Range.Length
		isPartial = true
	} else {
		offset = 0
		length = objRef.Size
	}

	// For encrypted objects, we need to read the full object, decrypt, then apply range
	if isSSECEncrypted || isSSEKMSEncrypted {
		return s.getEncryptedObject(ctx, objRef, req, offset, length, isPartial, lastModified)
	}

	// Create a pipe for streaming
	pr, pw := io.Pipe()

	// Start reading in background
	go func() {
		var readErr error
		if isPartial {
			readErr = s.storage.ReadObjectRange(ctx, &storage.ReadRangeRequest{
				ChunkRefs: objRef.ChunkRefs,
				Offset:    offset,
				Length:    length,
			}, pw)
		} else {
			readErr = s.storage.ReadObject(ctx, &storage.ReadRequest{
				ChunkRefs: objRef.ChunkRefs,
			}, pw)
		}
		pw.CloseWithError(readErr)
	}()

	metadata := &ObjectMetadata{
		ETag:         objRef.ETag,
		LastModified: lastModified,
		Size:         objRef.Size,
		ContentType:  "application/octet-stream",
		StorageClass: objRef.ProfileID,
	}

	result := &GetObjectResult{
		Object:       objRef,
		Body:         pr,
		Metadata:     metadata,
		IsPartial:    isPartial,
		AcceptRanges: "bytes",
	}

	if isPartial {
		result.Range = &ByteRange{
			Start:  offset,
			End:    offset + length - 1,
			Length: length,
		}
	}

	return result, nil
}

// getEncryptedObject handles GetObject for encrypted objects
func (s *serviceImpl) getEncryptedObject(
	ctx context.Context,
	objRef *types.ObjectRef,
	req *GetObjectRequest,
	offset, length uint64,
	isPartial bool,
	lastModified time.Time,
) (*GetObjectResult, error) {
	// Read full encrypted data
	encryptedData, err := s.storage.ReadObjectToBuffer(ctx, objRef.ChunkRefs)
	if err != nil {
		return nil, newInternalError(err)
	}

	// Build encryption metadata from object
	encMeta := &encryption.Metadata{
		Algorithm:      objRef.SSEAlgorithm,
		CustomerKeyMD5: objRef.SSECustomerKeyMD5,
		KMSKeyID:       objRef.SSEKMSKeyID,
	}

	// Parse stored KMS context to extract DEK ciphertext
	if objRef.SSEKMSKeyID != "" {
		encMeta.KMSContext, encMeta.DEKCiphertext = encryption.ParseStoredKMSContext(objRef.SSEKMSContext)
	}

	// Build decryption params
	var params *encryption.Params
	if req.SSEC != nil {
		params = &encryption.Params{
			SSEC: &encryption.SSECParams{
				Algorithm: req.SSEC.Algorithm,
				Key:       req.SSEC.Key,
				KeyMD5:    req.SSEC.KeyMD5,
			},
		}
	}

	// Decrypt
	plaintext, err := s.encryption.Decrypt(ctx, encryptedData, encMeta, params)
	if err != nil {
		return nil, newInternalError(err)
	}

	// Validate size
	if uint64(len(plaintext)) != objRef.Size {
		logger.Error().
			Uint64("expected", objRef.Size).
			Int("actual", len(plaintext)).
			Msg("decrypted size mismatch")
		return nil, newInternalError(errors.New("decrypted size mismatch"))
	}

	// Apply range if needed
	var data []byte
	var finalLength uint64
	if isPartial {
		if offset+length > uint64(len(plaintext)) {
			length = uint64(len(plaintext)) - offset
		}
		data = plaintext[offset : offset+length]
		finalLength = length
	} else {
		data = plaintext
		finalLength = objRef.Size
	}

	metadata := &ObjectMetadata{
		ETag:              objRef.ETag,
		LastModified:      lastModified,
		Size:              objRef.Size,
		ContentType:       "application/octet-stream",
		StorageClass:      objRef.ProfileID,
		SSEAlgorithm:      objRef.SSEAlgorithm,
		SSECustomerKeyMD5: objRef.SSECustomerKeyMD5,
		SSEKMSKeyID:       objRef.SSEKMSKeyID,
	}

	if objRef.SSEKMSKeyID != "" {
		metadata.SSEKMSContext, _ = encryption.ParseStoredKMSContext(objRef.SSEKMSContext)
	}

	result := &GetObjectResult{
		Object:       objRef,
		Body:         io.NopCloser(bytes.NewReader(data)),
		Metadata:     metadata,
		IsPartial:    isPartial,
		AcceptRanges: "bytes",
	}

	if isPartial {
		result.Range = &ByteRange{
			Start:  offset,
			End:    offset + finalLength - 1,
			Length: finalLength,
		}
	}

	return result, nil
}

// HeadObject retrieves object metadata
func (s *serviceImpl) HeadObject(ctx context.Context, bucket, key string) (*HeadObjectResult, error) {
	objRef, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, db.ErrObjectNotFound) {
			return nil, newNotFoundError("object")
		}
		return nil, newInternalError(err)
	}

	if objRef.IsDeleted() {
		return nil, newNotFoundError("object")
	}

	lastModified := time.Unix(0, objRef.CreatedAt).UTC()
	metadata := &ObjectMetadata{
		ETag:              objRef.ETag,
		LastModified:      lastModified,
		Size:              objRef.Size,
		ContentType:       "application/octet-stream",
		StorageClass:      objRef.ProfileID,
		SSEAlgorithm:      objRef.SSEAlgorithm,
		SSECustomerKeyMD5: objRef.SSECustomerKeyMD5,
		SSEKMSKeyID:       objRef.SSEKMSKeyID,
	}

	if objRef.SSEKMSKeyID != "" {
		metadata.SSEKMSContext, _ = encryption.ParseStoredKMSContext(objRef.SSEKMSContext)
	}

	return &HeadObjectResult{
		Object:   objRef,
		Metadata: metadata,
	}, nil
}

// DeleteObject soft-deletes an object and decrements chunk reference counts
func (s *serviceImpl) DeleteObject(ctx context.Context, bucket, key string) (*DeleteObjectResult, error) {
	// Perform delete and ref count decrements atomically in a transaction
	now := time.Now().Unix()

	err := s.db.WithTx(ctx, func(tx db.TxStore) error {
		// Look up object to get its chunk refs
		objRef, err := tx.GetObject(ctx, bucket, key)
		if err != nil {
			return err
		}

		// Mark object deleted
		if err := tx.MarkObjectDeleted(ctx, bucket, key, now); err != nil {
			return err
		}

		// Decrement ref counts atomically (sets zero_ref_since if hits 0)
		for _, ref := range objRef.ChunkRefs {
			if err := tx.DecrementChunkRefCount(ctx, ref.ChunkID.String()); err != nil {
				// Continue on ErrChunkNotFound - chunk may not be in registry yet (legacy)
				if err != db.ErrChunkNotFound {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, db.ErrObjectNotFound) {
			// S3 returns success even for non-existent objects
			return &DeleteObjectResult{}, nil
		}
		return nil, newInternalError(err)
	}

	// Trigger CRR hook if configured
	if s.crrHook != nil && s.bucketStore != nil {
		bucketInfo, _ := s.bucketStore.GetBucket(bucket)
		s.crrHook.AfterDeleteObject(ctx, &bucketInfo, key)
	}

	// Emit S3 event notification
	if s.emitter != nil {
		s.emitter.EmitObjectRemoved(ctx, events.EventObjectRemovedDelete,
			bucket, key, "", "", "", "")
	}

	return &DeleteObjectResult{}, nil
}

// DeleteObjectWithVersion handles versioned delete operations
func (s *serviceImpl) DeleteObjectWithVersion(ctx context.Context, bucket, key, versionID string) (*DeleteObjectResult, error) {
	// If versionID is provided, permanently delete that specific version
	if versionID != "" {
		err := s.db.DeleteObjectVersion(ctx, bucket, key, versionID)
		if err != nil {
			if errors.Is(err, db.ErrObjectNotFound) {
				// S3 returns success even for non-existent versions
				return &DeleteObjectResult{VersionID: versionID}, nil
			}
			return nil, newInternalError(err)
		}
		return &DeleteObjectResult{VersionID: versionID}, nil
	}

	// Check if bucket has versioning enabled
	versioningEnabled := false
	if s.bucketStore != nil {
		if bucketInfo, exists := s.bucketStore.GetBucket(bucket); exists {
			versioningEnabled = bucketInfo.Versioning == "Enabled"
		}
	}

	if versioningEnabled {
		// Create a delete marker instead of actually deleting
		deleteMarkerID, err := s.db.PutDeleteMarker(ctx, bucket, key, "")
		if err != nil {
			return nil, newInternalError(err)
		}
		return &DeleteObjectResult{
			VersionID:    deleteMarkerID,
			DeleteMarker: true,
		}, nil
	}

	// Non-versioned: perform regular delete
	return s.DeleteObject(ctx, bucket, key)
}

// DeleteObjects batch deletes multiple objects and decrements chunk reference counts
func (s *serviceImpl) DeleteObjects(ctx context.Context, req *DeleteObjectsRequest) (*DeleteObjectsResult, error) {
	var deleted []DeletedObject
	var errs []DeleteError

	now := time.Now().Unix()

	// Process each object in its own transaction for partial success handling
	for _, obj := range req.Objects {
		err := s.db.WithTx(ctx, func(tx db.TxStore) error {
			// Get object to collect chunk refs
			objRef, err := tx.GetObject(ctx, req.Bucket, obj.Key)
			if err != nil {
				return err
			}

			// Mark object deleted
			if err := tx.MarkObjectDeleted(ctx, req.Bucket, obj.Key, now); err != nil {
				return err
			}

			// Decrement ref counts atomically
			for _, ref := range objRef.ChunkRefs {
				if err := tx.DecrementChunkRefCount(ctx, ref.ChunkID.String()); err != nil {
					// Continue on ErrChunkNotFound - chunk may not be in registry yet (legacy)
					if err != db.ErrChunkNotFound {
						return err
					}
				}
			}
			return nil
		})

		if err != nil {
			if !errors.Is(err, db.ErrObjectNotFound) {
				errs = append(errs, DeleteError{
					Key:     obj.Key,
					Code:    "InternalError",
					Message: "Failed to delete object",
				})
				continue
			}
			// S3 returns success for non-existent objects
		}

		deleted = append(deleted, DeletedObject{
			Key: obj.Key,
		})

		// Trigger CRR hook
		if s.crrHook != nil && s.bucketStore != nil {
			bucketInfo, _ := s.bucketStore.GetBucket(req.Bucket)
			s.crrHook.AfterDeleteObject(ctx, &bucketInfo, obj.Key)
		}

		// Emit S3 event notification
		if s.emitter != nil {
			s.emitter.EmitObjectRemoved(ctx, events.EventObjectRemovedDelete,
				req.Bucket, obj.Key, "", "", "", "")
		}
	}

	return &DeleteObjectsResult{
		Deleted: deleted,
		Errors:  errs,
	}, nil
}

// CopyObject copies an object
func (s *serviceImpl) CopyObject(ctx context.Context, req *CopyObjectRequest) (*CopyObjectResult, error) {
	// Get source object
	srcObj, err := s.db.GetObject(ctx, req.SourceBucket, req.SourceKey)
	if err != nil {
		if errors.Is(err, db.ErrObjectNotFound) {
			return nil, newNotFoundError("source object")
		}
		return nil, newInternalError(err)
	}

	if srcObj.IsDeleted() {
		return nil, newNotFoundError("source object")
	}

	// Check copy source conditional headers
	lastModified := time.Unix(0, srcObj.CreatedAt).UTC()
	if !s.checkCopySourceConditionals(req, srcObj.ETag, lastModified) {
		return nil, &Error{
			Code:    ErrCodePreconditionFailed,
			Message: "copy source precondition failed",
		}
	}

	// Create new object reference pointing to same chunks
	now := time.Now()
	newObjRef := &types.ObjectRef{
		ID:        uuid.New(),
		Bucket:    req.DestBucket,
		Key:       req.DestKey,
		Size:      srcObj.Size,
		ETag:      srcObj.ETag,
		ChunkRefs: srcObj.ChunkRefs,
		CreatedAt: now.UnixNano(),
		ProfileID: srcObj.ProfileID,
	}

	result := &CopyObjectResult{
		ETag:         srcObj.ETag,
		LastModified: now,
	}

	// Handle encryption:
	// 1. If explicit SSE-KMS requested, we would need to re-encrypt (not yet implemented)
	// 2. If source is encrypted and no explicit dest encryption, copy the encryption metadata
	// 3. If source is unencrypted and bucket has default encryption, would need to encrypt (not yet implemented)
	//
	// For now, copy the encryption metadata from source if no explicit encryption specified.
	// Full re-encryption requires reading, decrypting, encrypting, and writing which is more complex.
	if req.SSEKMS != nil {
		// Explicit SSE-KMS requested for destination
		// TODO: Implement re-encryption - requires reading source, decrypting if needed,
		// encrypting with new key, and writing to storage
		logger.Warn().Msg("explicit SSE-KMS for CopyObject destination not yet implemented, copying source encryption")
	}

	// Copy encryption metadata from source
	if srcObj.SSEAlgorithm != "" {
		newObjRef.SSEAlgorithm = srcObj.SSEAlgorithm
		newObjRef.SSECustomerKeyMD5 = srcObj.SSECustomerKeyMD5
		newObjRef.SSEKMSKeyID = srcObj.SSEKMSKeyID
		newObjRef.SSEKMSContext = srcObj.SSEKMSContext

		// Set result fields
		result.SSEAlgorithm = srcObj.SSEAlgorithm
		result.SSECustomerKeyMD5 = srcObj.SSECustomerKeyMD5
		result.SSEKMSKeyID = srcObj.SSEKMSKeyID
		if srcObj.SSEKMSKeyID != "" {
			result.SSEKMSContext, _ = encryption.ParseStoredKMSContext(srcObj.SSEKMSContext)
		}
	}

	// Store in database and increment chunk ref counts atomically
	err = s.db.WithTx(ctx, func(tx db.TxStore) error {
		if err := tx.PutObject(ctx, newObjRef); err != nil {
			return err
		}

		// Increment ref counts for shared chunks
		for _, ref := range srcObj.ChunkRefs {
			if err := tx.IncrementChunkRefCount(ctx, ref.ChunkID.String(), int64(ref.Size)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, newInternalError(err)
	}

	// Emit S3 event notification for copy
	if s.emitter != nil {
		s.emitter.EmitObjectCreated(ctx, events.EventObjectCreatedCopy,
			req.DestBucket, req.DestKey, int64(srcObj.Size), srcObj.ETag, newObjRef.ID.String(), "", "", "")
	}

	// Handle tagging directive
	taggingDirective := req.TaggingDirective
	if taggingDirective == "" {
		taggingDirective = "COPY"
	}

	if taggingDirective == "COPY" {
		srcTags, err := s.db.GetObjectTagging(ctx, req.SourceBucket, req.SourceKey)
		if err == nil && srcTags != nil {
			if err := s.db.SetObjectTagging(ctx, req.DestBucket, req.DestKey, srcTags); err != nil {
				logger.Warn().Err(err).Msg("failed to copy object tags")
			}
		}
	}

	return result, nil
}

// ListObjects lists objects using v1 API
func (s *serviceImpl) ListObjects(ctx context.Context, req *ListObjectsRequest) (*ListObjectsResult, error) {
	maxKeys := req.MaxKeys
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	listResult, err := s.db.ListObjectsV2(ctx, &db.ListObjectsParams{
		Bucket:     req.Bucket,
		Prefix:     req.Prefix,
		Delimiter:  req.Delimiter,
		MaxKeys:    maxKeys,
		StartAfter: req.Marker,
	})
	if err != nil {
		return nil, newInternalError(err)
	}

	// Build response
	var contents []ObjectEntry
	for _, obj := range listResult.Objects {
		contents = append(contents, ObjectEntry{
			Key:          obj.Key,
			LastModified: time.Unix(0, obj.CreatedAt).UTC(),
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: "STANDARD",
		})
	}

	var nextMarker string
	if listResult.IsTruncated && len(listResult.Objects) > 0 {
		nextMarker = listResult.Objects[len(listResult.Objects)-1].Key
	}

	return &ListObjectsResult{
		Name:           req.Bucket,
		Prefix:         req.Prefix,
		Marker:         req.Marker,
		NextMarker:     nextMarker,
		Delimiter:      req.Delimiter,
		MaxKeys:        maxKeys,
		IsTruncated:    listResult.IsTruncated,
		Contents:       contents,
		CommonPrefixes: listResult.CommonPrefixes,
	}, nil
}

// ListObjectsV2 lists objects using v2 API
func (s *serviceImpl) ListObjectsV2(ctx context.Context, req *ListObjectsV2Request) (*ListObjectsV2Result, error) {
	maxKeys := req.MaxKeys
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	listResult, err := s.db.ListObjectsV2(ctx, &db.ListObjectsParams{
		Bucket:            req.Bucket,
		Prefix:            req.Prefix,
		Delimiter:         req.Delimiter,
		MaxKeys:           maxKeys,
		ContinuationToken: req.ContinuationToken,
		StartAfter:        req.StartAfter,
		FetchOwner:        req.FetchOwner,
	})
	if err != nil {
		return nil, newInternalError(err)
	}

	// Build response
	var contents []ObjectEntry
	for _, obj := range listResult.Objects {
		entry := ObjectEntry{
			Key:          obj.Key,
			LastModified: time.Unix(0, obj.CreatedAt).UTC(),
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: "STANDARD",
		}

		if req.FetchOwner {
			entry.Owner = &ObjectOwner{
				ID:          "", // Would come from bucket info
				DisplayName: "",
			}
		}

		contents = append(contents, entry)
	}

	return &ListObjectsV2Result{
		Name:                  req.Bucket,
		Prefix:                req.Prefix,
		Delimiter:             req.Delimiter,
		MaxKeys:               maxKeys,
		KeyCount:              len(contents) + len(listResult.CommonPrefixes),
		IsTruncated:           listResult.IsTruncated,
		ContinuationToken:     req.ContinuationToken,
		NextContinuationToken: listResult.NextContinuationToken,
		StartAfter:            req.StartAfter,
		Contents:              contents,
		CommonPrefixes:        listResult.CommonPrefixes,
	}, nil
}

// checkConditionalHeaders validates conditional request headers
func (s *serviceImpl) checkConditionalHeaders(req *GetObjectRequest, etag string, lastModified time.Time) *ConditionalResult {
	result := &ConditionalResult{ShouldProceed: true}

	// If-Match
	if req.IfMatch != "" {
		if req.IfMatch != etag {
			result.ShouldProceed = false
			result.StatusCode = 412
			return result
		}
	}

	// If-None-Match
	if req.IfNoneMatch != "" {
		if req.IfNoneMatch == etag {
			result.ShouldProceed = false
			result.StatusCode = 304
			result.NotModified = true
			return result
		}
	}

	// If-Modified-Since
	if req.IfModifiedSince != nil {
		if !lastModified.After(*req.IfModifiedSince) {
			result.ShouldProceed = false
			result.StatusCode = 304
			result.NotModified = true
			return result
		}
	}

	// If-Unmodified-Since
	if req.IfUnmodifiedSince != nil {
		if lastModified.After(*req.IfUnmodifiedSince) {
			result.ShouldProceed = false
			result.StatusCode = 412
			return result
		}
	}

	return result
}

// checkCopySourceConditionals validates copy source conditional headers
func (s *serviceImpl) checkCopySourceConditionals(req *CopyObjectRequest, etag string, lastModified time.Time) bool {
	if req.CopySourceIfMatch != "" && req.CopySourceIfMatch != etag {
		return false
	}
	if req.CopySourceIfNoneMatch != "" && req.CopySourceIfNoneMatch == etag {
		return false
	}
	if req.CopySourceIfModifiedSince != nil && !lastModified.After(*req.CopySourceIfModifiedSince) {
		return false
	}
	if req.CopySourceIfUnmodifiedSince != nil && lastModified.After(*req.CopySourceIfUnmodifiedSince) {
		return false
	}
	return true
}
