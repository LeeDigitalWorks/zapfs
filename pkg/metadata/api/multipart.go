package api

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"zapfs/pkg/logger"
	"zapfs/pkg/metadata/data"
	"zapfs/pkg/metadata/db"
	"zapfs/pkg/metadata/service/multipart"
	"zapfs/pkg/s3api/s3consts"
	"zapfs/pkg/s3api/s3err"
	"zapfs/pkg/s3api/s3types"
	"zapfs/pkg/types"

	"github.com/google/uuid"
)

// CreateMultipartUploadHandler initiates a multipart upload.
// POST /{bucket}/{key}?uploads
func (s *MetadataServer) CreateMultipartUploadHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Generate upload ID (base64-encoded UUID)
	uploadUUID := uuid.New()
	uploadID := base64.RawURLEncoding.EncodeToString(uploadUUID[:])

	// Create upload record
	upload := &types.MultipartUpload{
		ID:           uploadUUID,
		UploadID:     uploadID,
		Bucket:       bucket,
		Key:          key,
		OwnerID:      d.S3Info.OwnerID,
		Initiated:    time.Now().UnixNano(),
		ContentType:  d.Req.Header.Get("Content-Type"),
		StorageClass: d.Req.Header.Get("x-amz-storage-class"),
	}

	if upload.StorageClass == "" {
		upload.StorageClass = "STANDARD"
	}

	// Store in database
	if err := s.db.CreateMultipartUpload(d.Ctx, upload); err != nil {
		logger.Error().Err(err).Msg("failed to create multipart upload")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Build response
	result := s3types.InitiateMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)

	logger.Info().
		Str("bucket", bucket).
		Str("key", key).
		Str("upload_id", uploadID).
		Msg("multipart upload initiated")
}

// UploadPartHandler uploads a part of a multipart upload.
// PUT /{bucket}/{key}?partNumber={partNumber}&uploadId={uploadId}
func (s *MetadataServer) UploadPartHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Parse query parameters
	query := d.Req.URL.Query()
	uploadID := query.Get("uploadId")
	partNumberStr := query.Get("partNumber")

	if uploadID == "" || partNumberStr == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Use the service layer which handles parallel writes to all file servers
	result, err := s.svc.Multipart().UploadPart(d.Ctx, &multipart.UploadPartRequest{
		Bucket:        bucket,
		Key:           key,
		UploadID:      uploadID,
		PartNumber:    partNumber,
		Body:          d.Req.Body,
		ContentLength: d.Req.ContentLength,
	})
	if err != nil {
		var svcErr *multipart.Error
		if errors.As(err, &svcErr) {
			writeXMLErrorResponse(w, d, svcErr.ToS3Error())
		} else {
			logger.Error().Err(err).Msg("failed to upload part")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		}
		return
	}

	// Return ETag in response
	w.Header().Set("ETag", "\""+result.ETag+"\"")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	logger.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Str("upload_id", uploadID).
		Int("part_number", partNumber).
		Msg("part uploaded")
}

// CompleteMultipartUploadHandler completes a multipart upload.
// POST /{bucket}/{key}?uploadId={uploadId}
func (s *MetadataServer) CompleteMultipartUploadHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	uploadID := d.Req.URL.Query().Get("uploadId")
	if uploadID == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Parse request body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	var req s3types.CompleteMultipartUploadRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Verify upload exists
	upload, err := s.db.GetMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil {
		if errors.Is(err, db.ErrUploadNotFound) {
			writeXMLErrorResponse(w, d, s3err.ErrNoSuchUpload)
			return
		}
		logger.Error().Err(err).Msg("failed to get multipart upload")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Get all parts
	allParts, _, err := s.db.ListParts(ctx, uploadID, 0, 10000)
	if err != nil {
		logger.Error().Err(err).Msg("failed to list parts")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Build part map for validation
	partMap := make(map[int]*types.MultipartPart)
	for _, p := range allParts {
		partMap[p.PartNumber] = p
	}

	// Validate and collect parts in order
	var totalSize int64
	var allChunkRefs []types.ChunkRef
	var etagParts []string

	// Sort request parts by part number
	sort.Slice(req.Parts, func(i, j int) bool {
		return req.Parts[i].PartNumber < req.Parts[j].PartNumber
	})

	for _, reqPart := range req.Parts {
		part, exists := partMap[reqPart.PartNumber]
		if !exists {
			writeXMLErrorResponse(w, d, s3err.ErrInvalidPart)
			return
		}

		// Validate ETag (strip quotes)
		expectedETag := strings.Trim(reqPart.ETag, "\"")
		if part.ETag != expectedETag {
			writeXMLErrorResponse(w, d, s3err.ErrInvalidPart)
			return
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
	hash := md5.Sum([]byte(etagConcat))
	finalETag := hex.EncodeToString(hash[:]) + "-" + strconv.Itoa(len(req.Parts))

	// Create final object (within transaction)
	err = s.db.WithTx(ctx, func(tx db.TxStore) error {
		// Create the final object
		obj := &types.ObjectRef{
			ID:        uuid.New(),
			Bucket:    bucket,
			Key:       key,
			Size:      uint64(totalSize),
			ETag:      finalETag,
			CreatedAt: time.Now().UnixNano(),
			ChunkRefs: allChunkRefs,
		}

		if err := tx.PutObject(ctx, obj); err != nil {
			return err
		}

		// Delete the upload and parts
		if err := tx.DeleteMultipartUpload(ctx, bucket, key, uploadID); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		logger.Error().Err(err).Msg("failed to complete multipart upload")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Build response
	result := s3types.CompleteMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Location: "http://" + bucket + ".s3.amazonaws.com/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     "\"" + finalETag + "\"",
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)

	logger.Info().
		Str("bucket", bucket).
		Str("key", key).
		Str("upload_id", uploadID).
		Int64("size", totalSize).
		Int("parts", len(req.Parts)).
		Str("content_type", upload.ContentType).
		Msg("multipart upload completed")
}

// AbortMultipartUploadHandler aborts a multipart upload.
// DELETE /{bucket}/{key}?uploadId={uploadId}
func (s *MetadataServer) AbortMultipartUploadHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	uploadID := d.Req.URL.Query().Get("uploadId")
	if uploadID == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Use service layer which handles chunk cleanup
	if s.svc != nil {
		err := s.svc.Multipart().AbortUpload(d.Ctx, bucket, key, uploadID)
		if err != nil {
			var mpErr *multipart.Error
			if errors.As(err, &mpErr) {
				switch mpErr.Code {
				case multipart.ErrCodeNoSuchUpload:
					writeXMLErrorResponse(w, d, s3err.ErrNoSuchUpload)
				default:
					logger.Error().Err(err).Msg("failed to abort multipart upload")
					writeXMLErrorResponse(w, d, s3err.ErrInternalError)
				}
				return
			}
			logger.Error().Err(err).Msg("failed to abort multipart upload")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
			return
		}
	} else {
		// Legacy path (no service layer)
		_, err := s.db.GetMultipartUpload(d.Ctx, bucket, key, uploadID)
		if err != nil {
			if errors.Is(err, db.ErrUploadNotFound) {
				writeXMLErrorResponse(w, d, s3err.ErrNoSuchUpload)
				return
			}
			logger.Error().Err(err).Msg("failed to get multipart upload")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
			return
		}

		if err := s.db.DeleteMultipartUpload(d.Ctx, bucket, key, uploadID); err != nil {
			logger.Error().Err(err).Msg("failed to delete multipart upload")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
			return
		}
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)

	logger.Info().
		Str("bucket", bucket).
		Str("key", key).
		Str("upload_id", uploadID).
		Msg("multipart upload aborted")
}

// ListPartsHandler lists the parts of a multipart upload.
// GET /{bucket}/{key}?uploadId={uploadId}
func (s *MetadataServer) ListPartsHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	query := d.Req.URL.Query()
	uploadID := query.Get("uploadId")
	if uploadID == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Parse pagination params
	partNumberMarker := 0
	if marker := query.Get("part-number-marker"); marker != "" {
		if m, err := strconv.Atoi(marker); err == nil {
			partNumberMarker = m
		}
	}

	maxParts := 1000
	if mp := query.Get("max-parts"); mp != "" {
		if m, err := strconv.Atoi(mp); err == nil && m > 0 && m <= 1000 {
			maxParts = m
		}
	}

	// Verify upload exists
	upload, err := s.db.GetMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil {
		if errors.Is(err, db.ErrUploadNotFound) {
			writeXMLErrorResponse(w, d, s3err.ErrNoSuchUpload)
			return
		}
		logger.Error().Err(err).Msg("failed to get multipart upload")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// List parts
	parts, isTruncated, err := s.db.ListParts(ctx, uploadID, partNumberMarker, maxParts)
	if err != nil {
		logger.Error().Err(err).Msg("failed to list parts")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Build response
	var partInfos []s3types.PartInfo
	var nextPartNumberMarker int
	for _, p := range parts {
		partInfos = append(partInfos, s3types.PartInfo{
			PartNumber:   p.PartNumber,
			LastModified: time.Unix(0, p.LastModified).UTC().Format(time.RFC3339),
			ETag:         "\"" + p.ETag + "\"",
			Size:         p.Size,
		})
		nextPartNumberMarker = p.PartNumber
	}

	result := s3types.ListPartsResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		Initiator: &s3types.Initiator{
			ID:          upload.OwnerID,
			DisplayName: upload.OwnerID,
		},
		Owner: &s3types.Owner{
			ID:          upload.OwnerID,
			DisplayName: upload.OwnerID,
		},
		StorageClass:         upload.StorageClass,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             maxParts,
		IsTruncated:          isTruncated,
		Parts:                partInfos,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)
}

// ListMultipartUploadsHandler lists in-progress multipart uploads.
// GET /{bucket}?uploads
func (s *MetadataServer) ListMultipartUploadsHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket

	query := d.Req.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	keyMarker := query.Get("key-marker")
	uploadIDMarker := query.Get("upload-id-marker")

	maxUploads := 1000
	if mu := query.Get("max-uploads"); mu != "" {
		if m, err := strconv.Atoi(mu); err == nil && m > 0 && m <= 1000 {
			maxUploads = m
		}
	}

	// List uploads
	uploads, isTruncated, err := s.db.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, maxUploads)
	if err != nil {
		logger.Error().Err(err).Msg("failed to list multipart uploads")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Handle delimiter for folder simulation
	var resultUploads []s3types.MultipartUpload
	var commonPrefixes []s3types.CommonPrefix
	seenPrefixes := make(map[string]bool)

	for _, u := range uploads {
		if delimiter != "" && prefix != "" {
			afterPrefix := u.Key[len(prefix):]
			idx := strings.Index(afterPrefix, delimiter)
			if idx >= 0 {
				commonPrefix := prefix + afterPrefix[:idx+len(delimiter)]
				if !seenPrefixes[commonPrefix] {
					seenPrefixes[commonPrefix] = true
					commonPrefixes = append(commonPrefixes, s3types.CommonPrefix{Prefix: commonPrefix})
				}
				continue
			}
		}

		resultUploads = append(resultUploads, s3types.MultipartUpload{
			Key:      u.Key,
			UploadID: u.UploadID,
			Initiator: &s3types.Initiator{
				ID:          u.OwnerID,
				DisplayName: u.OwnerID,
			},
			Owner: &s3types.Owner{
				ID:          u.OwnerID,
				DisplayName: u.OwnerID,
			},
			StorageClass: u.StorageClass,
			Initiated:    time.Unix(0, u.Initiated).UTC().Format(time.RFC3339),
		})
	}

	// Determine next markers
	var nextKeyMarker, nextUploadIDMarker string
	if isTruncated && len(uploads) > 0 {
		last := uploads[len(uploads)-1]
		nextKeyMarker = last.Key
		nextUploadIDMarker = last.UploadID
	}

	result := s3types.ListMultipartUploadsResult{
		Xmlns:              "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:             bucket,
		KeyMarker:          keyMarker,
		UploadIDMarker:     uploadIDMarker,
		NextKeyMarker:      nextKeyMarker,
		NextUploadIDMarker: nextUploadIDMarker,
		Delimiter:          delimiter,
		Prefix:             prefix,
		MaxUploads:         maxUploads,
		IsTruncated:        isTruncated,
		Uploads:            resultUploads,
		CommonPrefixes:     commonPrefixes,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(result)
}
