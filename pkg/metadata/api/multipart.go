// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/multipart"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// CreateMultipartUploadHandler initiates a multipart upload.
// POST /{bucket}/{key}?uploads
func (s *MetadataServer) CreateMultipartUploadHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Get bucket info for ACL validation
	var bucketInfo *s3types.Bucket
	if info, exists := s.bucketStore.GetBucket(bucket); exists {
		bucketInfo = &info
	}

	// Validate ACL headers against ownership controls
	if errCode := ValidateACLForOwnership(d.Req, bucketInfo); errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}

	// Parse ACL from request headers
	acl, errCode := ACLFromRequest(d.Req, d.S3Info.OwnerID)
	if errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}

	// Build service request
	req := &multipart.CreateUploadRequest{
		Bucket:       bucket,
		Key:          key,
		OwnerID:      d.S3Info.OwnerID,
		ContentType:  d.Req.Header.Get("Content-Type"),
		StorageClass: d.Req.Header.Get("x-amz-storage-class"),
		ACL:          acl,
	}

	// Parse SSE-KMS headers (if present)
	ssekmsHeaders, errCode := parseSSEKMSHeaders(d.Req)
	if errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}
	if ssekmsHeaders != nil {
		req.SSEKMS = &multipart.SSEKMSParams{
			KeyID:   ssekmsHeaders.KeyID,
			Context: ssekmsHeaders.Context,
		}
	}

	// If no SSE headers provided, check bucket default encryption
	if req.SSEKMS == nil {
		if bucketInfo, exists := s.bucketStore.GetBucket(bucket); exists && bucketInfo.Encryption != nil {
			// Apply bucket default encryption
			if kmsParams := getBucketDefaultEncryptionForMultipart(bucketInfo.Encryption); kmsParams != nil {
				req.SSEKMS = kmsParams
			}
		}
	}

	// Call service layer
	result, err := s.svc.Multipart().CreateUpload(d.Ctx, req)
	if err != nil {
		var mpErr *multipart.Error
		if errors.As(err, &mpErr) {
			writeXMLErrorResponse(w, d, mpErr.ToS3Error())
		} else {
			logger.Error().Err(err).Msg("failed to create multipart upload")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		}
		return
	}

	// Build XML response
	xmlResult := s3types.InitiateMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadID: result.UploadID,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))

	// SSE-KMS response headers
	if result.SSEKMSKeyID != "" {
		w.Header().Set(s3consts.XAmzServerSideEncryption, result.SSEAlgorithm)
		w.Header().Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, result.SSEKMSKeyID)
		if result.SSEKMSContext != "" {
			w.Header().Set(s3consts.XAmzServerSideEncryptionContext, result.SSEKMSContext)
		}
	}

	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(xmlResult)
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

	// Use verified body for streaming signed requests, otherwise use raw body
	body := io.Reader(d.Req.Body)
	contentLength := d.Req.ContentLength
	if d.VerifiedBody != nil {
		body = d.VerifiedBody
		// For streaming signed requests, x-amz-decoded-content-length contains
		// the actual payload size (Content-Length includes chunked framing overhead)
		if decodedLen := d.Req.Header.Get(s3consts.XAmzDecodedLength); decodedLen != "" {
			if parsed, err := strconv.ParseInt(decodedLen, 10, 64); err == nil {
				contentLength = parsed
			}
		}
	}

	// Use the service layer which handles parallel writes to all file servers
	result, err := s.svc.Multipart().UploadPart(d.Ctx, &multipart.UploadPartRequest{
		Bucket:        bucket,
		Key:           key,
		UploadID:      uploadID,
		PartNumber:    partNumber,
		Body:          body,
		ContentLength: contentLength,
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

	var xmlReq s3types.CompleteMultipartUploadRequest
	if err := xml.Unmarshal(body, &xmlReq); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Build service request
	parts := make([]multipart.PartEntry, len(xmlReq.Parts))
	for i, p := range xmlReq.Parts {
		parts[i] = multipart.PartEntry{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		}
	}

	// Call service layer
	result, err := s.svc.Multipart().CompleteUpload(ctx, &multipart.CompleteUploadRequest{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		Parts:    parts,
	})
	if err != nil {
		var mpErr *multipart.Error
		if errors.As(err, &mpErr) {
			writeXMLErrorResponse(w, d, mpErr.ToS3Error())
		} else {
			logger.Error().Err(err).Msg("failed to complete multipart upload")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		}
		return
	}

	// Build XML response
	xmlResult := s3types.CompleteMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Location: result.Location,
		Bucket:   result.Bucket,
		Key:      result.Key,
		ETag:     "\"" + result.ETag + "\"",
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))

	// SSE-KMS response headers
	if result.SSEKMSKeyID != "" {
		w.Header().Set(s3consts.XAmzServerSideEncryption, result.SSEAlgorithm)
		w.Header().Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, result.SSEKMSKeyID)
		if result.SSEKMSContext != "" {
			w.Header().Set(s3consts.XAmzServerSideEncryptionContext, result.SSEKMSContext)
		}
	}

	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(xmlResult)
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

// UploadPartCopyHandler copies data from an existing object as a part.
// PUT /{bucket}/{key}?partNumber={partNumber}&uploadId={uploadId}
// with x-amz-copy-source header
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
func (s *MetadataServer) UploadPartCopyHandler(d *data.Data, w http.ResponseWriter) {
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

	// Parse x-amz-copy-source header
	copySource := d.Req.Header.Get(s3consts.XAmzCopySource)
	if copySource == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// URL decode the copy source
	copySource, err = url.QueryUnescape(copySource)
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Parse source bucket/key from copy source header
	// Format: /bucket/key or bucket/key
	copySource = strings.TrimPrefix(copySource, "/")
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) != 2 {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}
	srcBucket, srcKey := parts[0], parts[1]

	// Parse optional range header
	sourceRange := d.Req.Header.Get(s3consts.XAmzCopySourceRange)

	// Parse conditional headers
	var copySourceIfModifiedSince, copySourceIfUnmodifiedSince *int64
	if v := d.Req.Header.Get(s3consts.XAmzCopySourceIfModifiedSince); v != "" {
		if t, err := http.ParseTime(v); err == nil {
			ts := t.Unix()
			copySourceIfModifiedSince = &ts
		}
	}
	if v := d.Req.Header.Get(s3consts.XAmzCopySourceIfUnmodifiedSince); v != "" {
		if t, err := http.ParseTime(v); err == nil {
			ts := t.Unix()
			copySourceIfUnmodifiedSince = &ts
		}
	}

	// Use the service layer
	result, err := s.svc.Multipart().UploadPartCopy(d.Ctx, &multipart.UploadPartCopyRequest{
		Bucket:                      bucket,
		Key:                         key,
		UploadID:                    uploadID,
		PartNumber:                  partNumber,
		SourceBucket:                srcBucket,
		SourceKey:                   srcKey,
		SourceRange:                 sourceRange,
		CopySourceIfMatch:           d.Req.Header.Get(s3consts.XAmzCopySourceIfMatch),
		CopySourceIfNoneMatch:       d.Req.Header.Get(s3consts.XAmzCopySourceIfNoneMatch),
		CopySourceIfModifiedSince:   copySourceIfModifiedSince,
		CopySourceIfUnmodifiedSince: copySourceIfUnmodifiedSince,
	})
	if err != nil {
		var svcErr *multipart.Error
		if errors.As(err, &svcErr) {
			writeXMLErrorResponse(w, d, svcErr.ToS3Error())
		} else {
			logger.Error().Err(err).Msg("failed to copy part")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		}
		return
	}

	// Build XML response
	type CopyPartResult struct {
		XMLName      xml.Name `xml:"CopyPartResult"`
		LastModified string   `xml:"LastModified"`
		ETag         string   `xml:"ETag"`
	}

	xmlResult := CopyPartResult{
		LastModified: time.Unix(0, result.LastModified).UTC().Format(time.RFC3339),
		ETag:         "\"" + result.ETag + "\"",
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(xmlResult)

	logger.Debug().
		Str("bucket", bucket).
		Str("key", key).
		Str("upload_id", uploadID).
		Int("part_number", partNumber).
		Str("source_bucket", srcBucket).
		Str("source_key", srcKey).
		Msg("part copied")
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
