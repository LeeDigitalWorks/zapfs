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

	"zapfs/pkg/logger"
	"zapfs/pkg/metadata/db"
	"zapfs/pkg/metadata/service/storage"
	"zapfs/pkg/types"
	"zapfs/pkg/utils"

	"github.com/google/uuid"
)

// Storage defines the storage operations needed by MultipartService.
// This interface allows for easy mocking in tests.
type Storage interface {
	WriteObject(ctx context.Context, req *storage.WriteRequest) (*storage.WriteResult, error)
	DecrementChunkRefCounts(ctx context.Context, chunks []types.ChunkRef) []storage.FailedDecrement
}

// Config holds configuration for the multipart service
type Config struct {
	DB             db.DB
	Storage        Storage
	Profiles       *types.ProfileSet
	DefaultProfile string
}

// serviceImpl implements the Service interface
type serviceImpl struct {
	db             db.DB
	storage        Storage
	profiles       *types.ProfileSet
	defaultProfile string
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
		profiles:       cfg.Profiles,
		defaultProfile: defaultProfile,
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

	if err := s.db.CreateMultipartUpload(ctx, upload); err != nil {
		logger.Error().Err(err).Msg("failed to create multipart upload")
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to create upload",
			Err:     err,
		}
	}

	return &CreateUploadResult{
		UploadID: uploadID,
	}, nil
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
	_ = upload // Validated above

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

		// Adjust chunk offsets
		for _, ref := range part.ChunkRefs {
			ref.Offset = uint64(totalSize)
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

	return &CompleteUploadResult{
		Location: "http://" + req.Bucket + ".s3.amazonaws.com/" + req.Key,
		Bucket:   req.Bucket,
		Key:      req.Key,
		ETag:     finalETag,
	}, nil
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

