// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package multipart

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/events"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/encryption"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/storage"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"

	"github.com/google/uuid"
)

// Storage defines the storage operations needed by MultipartService.
// This interface allows for easy mocking in tests.
type Storage interface {
	WriteObject(ctx context.Context, req *storage.WriteRequest) (*storage.WriteResult, error)
	DecrementChunkRefCounts(ctx context.Context, chunks []types.ChunkRef) []storage.FailedDecrement
	// ReadObject streams object data to a writer. Used for UploadPartCopy.
	ReadObject(ctx context.Context, req *storage.ReadRequest, writer io.Writer) error
	// ReadObjectRange streams a byte range of object data to a writer.
	ReadObjectRange(ctx context.Context, req *storage.ReadRangeRequest, writer io.Writer) error
}

// Config holds configuration for the multipart service
type Config struct {
	DB             db.DB
	Storage        Storage
	Encryption     *encryption.Handler
	Profiles       *types.ProfileSet
	DefaultProfile string
	Emitter        *events.Emitter // Optional, for S3 event notifications
}

// serviceImpl implements the Service interface
type serviceImpl struct {
	db             db.DB
	storage        Storage
	encryption     *encryption.Handler
	profiles       *types.ProfileSet
	defaultProfile string
	emitter        *events.Emitter
}

// NewService creates a new multipart service
func NewService(cfg Config) (Service, error) {
	if cfg.DB == nil {
		return nil, errors.New("DB is required")
	}
	if cfg.Storage == nil {
		return nil, errors.New("Storage is required")
	}

	defaultProfile := cfg.DefaultProfile
	if defaultProfile == "" {
		defaultProfile = "STANDARD"
	}

	return &serviceImpl{
		db:             cfg.DB,
		storage:        cfg.Storage,
		encryption:     cfg.Encryption,
		profiles:       cfg.Profiles,
		defaultProfile: defaultProfile,
		emitter:        cfg.Emitter,
	}, nil
}

func (s *serviceImpl) CreateUpload(ctx context.Context, req *CreateUploadRequest) (*CreateUploadResult, error) {
	// Generate upload ID (base64-encoded UUID)
	uploadUUID := uuid.New()
	uploadID := base64.RawURLEncoding.EncodeToString(uploadUUID[:])

	// Default storage class
	storageClass := req.StorageClass
	if storageClass == "" {
		storageClass = s.defaultProfile
	}

	// Create upload record
	upload := &types.MultipartUpload{
		ID:           uploadUUID,
		UploadID:     uploadID,
		Bucket:       req.Bucket,
		Key:          req.Key,
		OwnerID:      req.OwnerID,
		Initiated:    time.Now().UnixNano(),
		ContentType:  req.ContentType,
		StorageClass: storageClass,
	}

	result := &CreateUploadResult{
		UploadID: uploadID,
	}

	// Handle SSE-KMS encryption
	if req.SSEKMS != nil && s.encryption != nil && s.encryption.HasKMS() {
		// Generate a data encryption key (DEK) using KMS
		// The DEK will be used to encrypt each part
		encResult, err := s.encryption.GenerateDEK(ctx, req.SSEKMS.KeyID)
		if err != nil {
			logger.Error().Err(err).Str("key_id", req.SSEKMS.KeyID).Msg("failed to generate DEK for multipart upload")
			return nil, &Error{
				Code:    ErrCodeInternalError,
				Message: "failed to generate encryption key",
				Err:     err,
			}
		}

		// Store SSE metadata with upload
		upload.SSEAlgorithm = "aws:kms"
		upload.SSEKMSKeyID = req.SSEKMS.KeyID
		upload.SSEKMSContext = req.SSEKMS.Context
		upload.SSEDEKCiphertext = encResult.DEKCiphertext

		// Set response fields
		result.SSEAlgorithm = "aws:kms"
		result.SSEKMSKeyID = req.SSEKMS.KeyID
		result.SSEKMSContext = req.SSEKMS.Context
	}

	if err := s.db.CreateMultipartUpload(ctx, upload); err != nil {
		logger.Error().Err(err).Msg("failed to create multipart upload")
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to create upload",
			Err:     err,
		}
	}

	return result, nil
}

func (s *serviceImpl) UploadPart(ctx context.Context, req *UploadPartRequest) (*UploadPartResult, error) {
	// Validate part number
	if req.PartNumber < 1 || req.PartNumber > 10000 {
		return nil, &Error{
			Code:    ErrCodeInvalidArgument,
			Message: "part number must be between 1 and 10000",
		}
	}

	// Verify upload exists
	upload, err := s.db.GetMultipartUpload(ctx, req.Bucket, req.Key, req.UploadID)
	if err != nil {
		if errors.Is(err, db.ErrUploadNotFound) {
			return nil, &Error{
				Code:    ErrCodeNoSuchUpload,
				Message: "upload not found",
			}
		}
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to get upload",
			Err:     err,
		}
	}

	// Get storage profile
	profile, exists := s.profiles.Get(upload.StorageClass)
	if !exists {
		profile, _ = s.profiles.Get(s.defaultProfile)
	}
	replication := 2
	if profile != nil {
		replication = profile.Replication
	}

	// Generate object ID for this part
	objectID := uuid.New().String()

	// Hash the content while reading
	md5Hasher := utils.Md5PoolGetHasher()
	defer utils.Md5PoolPutHasher(md5Hasher)
	partReader := io.TeeReader(req.Body, md5Hasher)

	// Write to storage
	writeResult, err := s.storage.WriteObject(ctx, &storage.WriteRequest{
		ObjectID:    objectID,
		Body:        partReader,
		Size:        uint64(req.ContentLength),
		Replication: replication,
		ProfileName: upload.StorageClass,
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to write part to storage")
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to write part",
			Err:     err,
		}
	}

	// Compute ETag from hash
	etag := hex.EncodeToString(md5Hasher.Sum(nil))

	// Store part metadata
	part := &types.MultipartPart{
		ID:           uuid.New(),
		UploadID:     req.UploadID,
		PartNumber:   req.PartNumber,
		Size:         int64(writeResult.Size),
		ETag:         etag,
		LastModified: time.Now().UnixNano(),
		ChunkRefs:    writeResult.ChunkRefs,
	}

	if err := s.db.PutPart(ctx, part); err != nil {
		// Note: Storage layer handles orphan chunk reporting internally
		logger.Error().Err(err).Str("object_id", objectID).Msg("failed to store part metadata, chunks may be orphaned")
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to store part metadata",
			Err:     err,
		}
	}

	return &UploadPartResult{
		ETag: etag,
	}, nil
}

func (s *serviceImpl) UploadPartCopy(ctx context.Context, req *UploadPartCopyRequest) (*UploadPartCopyResult, error) {
	// Validate part number
	if req.PartNumber < 1 || req.PartNumber > 10000 {
		return nil, &Error{
			Code:    ErrCodeInvalidArgument,
			Message: "part number must be between 1 and 10000",
		}
	}

	// Verify upload exists
	upload, err := s.db.GetMultipartUpload(ctx, req.Bucket, req.Key, req.UploadID)
	if err != nil {
		if errors.Is(err, db.ErrUploadNotFound) {
			return nil, &Error{
				Code:    ErrCodeNoSuchUpload,
				Message: "upload not found",
			}
		}
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to get upload",
			Err:     err,
		}
	}

	// Get source object
	srcObj, err := s.db.GetObject(ctx, req.SourceBucket, req.SourceKey)
	if err != nil {
		if errors.Is(err, db.ErrObjectNotFound) {
			return nil, &Error{
				Code:    ErrCodeNoSuchUpload, // S3 uses NoSuchKey but we'll use this for now
				Message: "source object not found",
			}
		}
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to get source object",
			Err:     err,
		}
	}

	if srcObj.IsDeleted() {
		return nil, &Error{
			Code:    ErrCodeNoSuchUpload,
			Message: "source object not found",
		}
	}

	// Check copy source conditional headers
	srcLastModified := srcObj.CreatedAt
	if !s.checkCopySourceConditionals(req, srcObj.ETag, srcLastModified) {
		return nil, &Error{
			Code:    ErrCodePreconditionFailed,
			Message: "copy source precondition failed",
		}
	}

	// Determine range to copy and calculate size
	var copySize uint64
	var rangeStart, rangeLength int64

	if req.SourceRange != "" {
		// Parse range: "bytes=start-end"
		start, end, err := parseByteRange(req.SourceRange, int64(srcObj.Size))
		if err != nil {
			return nil, &Error{
				Code:    ErrCodeInvalidArgument,
				Message: "invalid source range: " + err.Error(),
			}
		}
		rangeStart = start
		rangeLength = end - start + 1
		copySize = uint64(rangeLength)
	} else {
		// Copy entire object
		copySize = srcObj.Size
		rangeLength = int64(srcObj.Size)
	}

	// Get storage profile
	profile, ok := s.profiles.Get(upload.StorageClass)
	if !ok {
		profile, ok = s.profiles.Get(s.defaultProfile)
	}
	if !ok || profile == nil {
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "storage profile not found",
		}
	}

	// Generate object ID for this part
	objectID := uuid.New().String()

	// Use io.Pipe to stream data from source to destination while computing MD5
	pr, pw := io.Pipe()
	md5Hasher := utils.Md5PoolGetHasher()
	defer utils.Md5PoolPutHasher(md5Hasher)

	// Writer that computes MD5 hash while writing to pipe
	hashWriter := io.MultiWriter(pw, md5Hasher)

	// Read source object in a goroutine, write to pipe
	var readErr error
	go func() {
		defer pw.Close()
		if req.SourceRange != "" {
			readErr = s.storage.ReadObjectRange(ctx, &storage.ReadRangeRequest{
				ChunkRefs: srcObj.ChunkRefs,
				Offset:    uint64(rangeStart),
				Length:    uint64(rangeLength),
			}, hashWriter)
		} else {
			readErr = s.storage.ReadObject(ctx, &storage.ReadRequest{
				ChunkRefs: srcObj.ChunkRefs,
			}, hashWriter)
		}
		if readErr != nil {
			pw.CloseWithError(readErr)
		}
	}()

	// Write to storage (reads from pipe)
	writeResult, err := s.storage.WriteObject(ctx, &storage.WriteRequest{
		ObjectID:    objectID,
		Size:        copySize,
		Body:        pr,
		Replication: profile.Replication,
	})
	if err != nil {
		// Check if read also failed
		if readErr != nil {
			logger.Error().Err(readErr).Msg("failed to read source object for copy")
		}
		logger.Error().Err(err).Msg("failed to write part from copy")
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to write part",
			Err:     err,
		}
	}

	// Check if read failed (write succeeded but read had issues)
	if readErr != nil {
		logger.Error().Err(readErr).Msg("failed to read source object for copy")
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to read source object",
			Err:     readErr,
		}
	}

	// Compute ETag from the hash
	etag := hex.EncodeToString(md5Hasher.Sum(nil))

	// Store part metadata
	now := time.Now()
	part := &types.MultipartPart{
		ID:           uuid.New(),
		UploadID:     req.UploadID,
		PartNumber:   req.PartNumber,
		Size:         int64(copySize),
		ETag:         etag,
		LastModified: now.UnixNano(),
		ChunkRefs:    writeResult.ChunkRefs,
	}

	if err := s.db.PutPart(ctx, part); err != nil {
		logger.Error().Err(err).Str("object_id", objectID).Msg("failed to store part metadata")
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to store part metadata",
			Err:     err,
		}
	}

	logger.Debug().
		Str("bucket", req.Bucket).
		Str("key", req.Key).
		Str("upload_id", req.UploadID).
		Int("part_number", req.PartNumber).
		Uint64("size", copySize).
		Str("source_bucket", req.SourceBucket).
		Str("source_key", req.SourceKey).
		Msg("part copied")

	return &UploadPartCopyResult{
		ETag:         etag,
		LastModified: now.UnixNano(),
	}, nil
}

// checkCopySourceConditionals checks conditional copy headers.
func (s *serviceImpl) checkCopySourceConditionals(req *UploadPartCopyRequest, etag string, lastModified int64) bool {
	// Check If-Match
	if req.CopySourceIfMatch != "" {
		// Remove quotes if present
		match := strings.Trim(req.CopySourceIfMatch, "\"")
		if match != etag {
			return false
		}
	}

	// Check If-None-Match
	if req.CopySourceIfNoneMatch != "" {
		match := strings.Trim(req.CopySourceIfNoneMatch, "\"")
		if match == etag {
			return false
		}
	}

	// Check If-Modified-Since
	if req.CopySourceIfModifiedSince != nil {
		// Convert nanoseconds to seconds for comparison
		if lastModified/1e9 <= *req.CopySourceIfModifiedSince {
			return false
		}
	}

	// Check If-Unmodified-Since
	if req.CopySourceIfUnmodifiedSince != nil {
		if lastModified/1e9 > *req.CopySourceIfUnmodifiedSince {
			return false
		}
	}

	return true
}

// parseByteRange parses a byte range string like "bytes=0-499" and returns start, end positions.
func parseByteRange(rangeStr string, objectSize int64) (int64, int64, error) {
	if !strings.HasPrefix(rangeStr, "bytes=") {
		return 0, 0, errors.New("range must start with 'bytes='")
	}

	rangeSpec := strings.TrimPrefix(rangeStr, "bytes=")
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid range format")
	}

	var start, end int64
	var err error

	if parts[0] == "" {
		// Suffix range: -500 means last 500 bytes
		if parts[1] == "" {
			return 0, 0, errors.New("invalid range format")
		}
		suffixLen, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, errors.New("invalid suffix length")
		}
		start = objectSize - suffixLen
		if start < 0 {
			start = 0
		}
		end = objectSize - 1
	} else {
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, errors.New("invalid start position")
		}
		if parts[1] == "" {
			// Range to end: 500- means from byte 500 to end
			end = objectSize - 1
		} else {
			end, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return 0, 0, errors.New("invalid end position")
			}
		}
	}

	// Validate range
	if start < 0 || end < 0 || start > end || start >= objectSize {
		return 0, 0, errors.New("range out of bounds")
	}

	// Clamp end to object size
	if end >= objectSize {
		end = objectSize - 1
	}

	return start, end, nil
}

func (s *serviceImpl) CompleteUpload(ctx context.Context, req *CompleteUploadRequest) (*CompleteUploadResult, error) {
	// Verify upload exists
	upload, err := s.db.GetMultipartUpload(ctx, req.Bucket, req.Key, req.UploadID)
	if err != nil {
		if errors.Is(err, db.ErrUploadNotFound) {
			return nil, &Error{
				Code:    ErrCodeNoSuchUpload,
				Message: "upload not found",
			}
		}
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to get upload",
			Err:     err,
		}
	}

	// Get all parts from DB
	allParts, _, err := s.db.ListParts(ctx, req.UploadID, 0, 10000)
	if err != nil {
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to list parts",
			Err:     err,
		}
	}

	// Build part map for validation
	partMap := make(map[int]*types.MultipartPart)
	for _, p := range allParts {
		partMap[p.PartNumber] = p
	}

	// Sort request parts by part number
	sortedParts := make([]PartEntry, len(req.Parts))
	copy(sortedParts, req.Parts)
	sort.Slice(sortedParts, func(i, j int) bool {
		return sortedParts[i].PartNumber < sortedParts[j].PartNumber
	})

	// Validate and collect parts in order
	var totalSize int64
	var allChunkRefs []types.ChunkRef
	var etagParts []string

	for _, reqPart := range sortedParts {
		part, exists := partMap[reqPart.PartNumber]
		if !exists {
			return nil, &Error{
				Code:    ErrCodeInvalidPart,
				Message: "part not found: " + strconv.Itoa(reqPart.PartNumber),
			}
		}

		// Validate ETag (strip quotes)
		expectedETag := strings.Trim(reqPart.ETag, "\"")
		if part.ETag != expectedETag {
			return nil, &Error{
				Code:    ErrCodeInvalidPart,
				Message: "ETag mismatch for part " + strconv.Itoa(reqPart.PartNumber),
			}
		}

		// Adjust chunk offsets to be relative to the start of the combined object
		// Each chunk's offset within the part must be added to the cumulative offset
		for _, ref := range part.ChunkRefs {
			ref.Offset = ref.Offset + uint64(totalSize)
			allChunkRefs = append(allChunkRefs, ref)
		}

		totalSize += part.Size
		etagParts = append(etagParts, part.ETag)
	}

	// Calculate final ETag (MD5 of concatenated part ETags + "-" + part count)
	etagConcat := strings.Join(etagParts, "")
	h := utils.Md5PoolGetHasher()
	h.Write([]byte(etagConcat))
	hashSum := h.Sum(nil)
	utils.Md5PoolPutHasher(h)
	finalETag := hex.EncodeToString(hashSum) + "-" + strconv.Itoa(len(sortedParts))

	// Create final object within transaction
	err = s.db.WithTx(ctx, func(tx db.TxStore) error {
		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    req.Bucket,
			Key:       req.Key,
			Size:      uint64(totalSize),
			ETag:      finalETag,
			CreatedAt: time.Now().UnixNano(),
			ChunkRefs: allChunkRefs,
		}

		// Copy SSE metadata from upload to final object
		if upload.SSEAlgorithm != "" {
			obj.SSEAlgorithm = upload.SSEAlgorithm
			obj.SSEKMSKeyID = upload.SSEKMSKeyID
			// Store the encrypted DEK along with context
			if upload.SSEDEKCiphertext != "" {
				obj.SSEKMSContext = encryption.BuildStoredKMSContext(
					upload.SSEKMSContext,
					upload.SSEDEKCiphertext,
				)
			}
		}

		if err := tx.PutObject(ctx, obj); err != nil {
			return err
		}

		if err := tx.DeleteMultipartUpload(ctx, req.Bucket, req.Key, req.UploadID); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to complete upload",
			Err:     err,
		}
	}

	result := &CompleteUploadResult{
		Location: "http://" + req.Bucket + ".s3.amazonaws.com/" + req.Key,
		Bucket:   req.Bucket,
		Key:      req.Key,
		ETag:     finalETag,
	}

	// Include SSE metadata in result
	if upload.SSEAlgorithm != "" {
		result.SSEAlgorithm = upload.SSEAlgorithm
		result.SSEKMSKeyID = upload.SSEKMSKeyID
		result.SSEKMSContext = upload.SSEKMSContext
	}

	// Emit S3 event notification for multipart upload completion
	if s.emitter != nil {
		s.emitter.EmitObjectCreated(ctx, events.EventObjectCreatedCompleteUpload,
			req.Bucket, req.Key, totalSize, finalETag, "", upload.OwnerID, "", "")
	}

	return result, nil
}

func (s *serviceImpl) AbortUpload(ctx context.Context, bucket, key, uploadID string) error {
	// Verify upload exists
	_, err := s.db.GetMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil {
		if errors.Is(err, db.ErrUploadNotFound) {
			return &Error{
				Code:    ErrCodeNoSuchUpload,
				Message: "upload not found",
			}
		}
		return &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to get upload",
			Err:     err,
		}
	}

	// Collect all chunk refs from all parts to decrement ref counts
	var allChunkRefs []types.ChunkRef
	parts, _, err := s.db.ListParts(ctx, uploadID, 0, 10000) // Get all parts
	if err != nil {
		logger.Warn().Err(err).Str("upload_id", uploadID).Msg("failed to list parts for cleanup")
	} else {
		for _, part := range parts {
			allChunkRefs = append(allChunkRefs, part.ChunkRefs...)
		}
	}

	// Decrement reference counts for all chunks
	// This is done before deleting the upload metadata to ensure chunks are cleaned up
	// even if the upload deletion fails (we can always retry the deletion)
	if len(allChunkRefs) > 0 {
		failed := s.storage.DecrementChunkRefCounts(ctx, allChunkRefs)
		if len(failed) > 0 {
			// Failed decrements are automatically queued for retry by the coordinator
			// if a task queue is configured. Log for visibility.
			logger.Warn().
				Str("upload_id", uploadID).
				Int("failed_count", len(failed)).
				Int("total_chunks", len(allChunkRefs)).
				Msg("some chunk ref count decrements failed during multipart abort (queued for retry if task queue enabled)")
		} else {
			logger.Debug().
				Str("upload_id", uploadID).
				Int("chunks", len(allChunkRefs)).
				Msg("decremented chunk ref counts for aborted multipart upload")
		}
	}

	// Delete upload and parts
	if err := s.db.DeleteMultipartUpload(ctx, bucket, key, uploadID); err != nil {
		return &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to delete upload",
			Err:     err,
		}
	}

	return nil
}

func (s *serviceImpl) ListParts(ctx context.Context, req *ListPartsRequest) (*ListPartsResult, error) {
	// Verify upload exists
	upload, err := s.db.GetMultipartUpload(ctx, req.Bucket, req.Key, req.UploadID)
	if err != nil {
		if errors.Is(err, db.ErrUploadNotFound) {
			return nil, &Error{
				Code:    ErrCodeNoSuchUpload,
				Message: "upload not found",
			}
		}
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to get upload",
			Err:     err,
		}
	}

	// Set defaults
	maxParts := req.MaxParts
	if maxParts <= 0 || maxParts > 1000 {
		maxParts = 1000
	}

	// List parts from DB
	parts, isTruncated, err := s.db.ListParts(ctx, req.UploadID, req.PartNumberMarker, maxParts)
	if err != nil {
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to list parts",
			Err:     err,
		}
	}

	// Convert to response format
	var partInfos []PartInfo
	var nextPartNumberMarker int
	for _, p := range parts {
		partInfos = append(partInfos, PartInfo{
			PartNumber:   p.PartNumber,
			LastModified: p.LastModified,
			ETag:         p.ETag,
			Size:         p.Size,
		})
		nextPartNumberMarker = p.PartNumber
	}

	return &ListPartsResult{
		Bucket:               req.Bucket,
		Key:                  req.Key,
		UploadID:             req.UploadID,
		OwnerID:              upload.OwnerID,
		StorageClass:         upload.StorageClass,
		PartNumberMarker:     req.PartNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             maxParts,
		IsTruncated:          isTruncated,
		Parts:                partInfos,
	}, nil
}

func (s *serviceImpl) ListUploads(ctx context.Context, req *ListUploadsRequest) (*ListUploadsResult, error) {
	// Set defaults
	maxUploads := req.MaxUploads
	if maxUploads <= 0 || maxUploads > 1000 {
		maxUploads = 1000
	}

	// List uploads from DB
	uploads, isTruncated, err := s.db.ListMultipartUploads(
		ctx, req.Bucket, req.Prefix, req.KeyMarker, req.UploadIDMarker, maxUploads,
	)
	if err != nil {
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to list uploads",
			Err:     err,
		}
	}

	// Handle delimiter for folder simulation
	var resultUploads []UploadInfo
	var commonPrefixes []string
	seenPrefixes := make(map[string]bool)

	for _, u := range uploads {
		if req.Delimiter != "" && req.Prefix != "" && strings.HasPrefix(u.Key, req.Prefix) {
			afterPrefix := u.Key[len(req.Prefix):]
			idx := strings.Index(afterPrefix, req.Delimiter)
			if idx >= 0 {
				commonPrefix := req.Prefix + afterPrefix[:idx+len(req.Delimiter)]
				if !seenPrefixes[commonPrefix] {
					seenPrefixes[commonPrefix] = true
					commonPrefixes = append(commonPrefixes, commonPrefix)
				}
				continue
			}
		}

		resultUploads = append(resultUploads, UploadInfo{
			Key:          u.Key,
			UploadID:     u.UploadID,
			OwnerID:      u.OwnerID,
			StorageClass: u.StorageClass,
			Initiated:    u.Initiated,
		})
	}

	// Determine next markers
	var nextKeyMarker, nextUploadIDMarker string
	if isTruncated && len(uploads) > 0 {
		last := uploads[len(uploads)-1]
		nextKeyMarker = last.Key
		nextUploadIDMarker = last.UploadID
	}

	return &ListUploadsResult{
		Bucket:             req.Bucket,
		KeyMarker:          req.KeyMarker,
		UploadIDMarker:     req.UploadIDMarker,
		NextKeyMarker:      nextKeyMarker,
		NextUploadIDMarker: nextUploadIDMarker,
		Delimiter:          req.Delimiter,
		Prefix:             req.Prefix,
		MaxUploads:         maxUploads,
		IsTruncated:        isTruncated,
		Uploads:            resultUploads,
		CommonPrefixes:     commonPrefixes,
	}, nil
}

