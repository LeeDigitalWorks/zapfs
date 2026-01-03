// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/object"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/signature"
	"github.com/LeeDigitalWorks/zapfs/pkg/usage"
)

// PutObjectHandler stores an object.
// PUT /{bucket}/{key}
func (s *MetadataServer) PutObjectHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Check expected bucket owner header if provided
	if bucket != "" {
		bucketInfo, exists := s.bucketStore.GetBucket(bucket)
		if exists {
			if errCode := checkExpectedBucketOwner(d.Req, bucketInfo.OwnerID); errCode != nil {
				writeXMLErrorResponse(w, d, *errCode)
				return
			}
			if errCode := checkRequestPayer(d.Req, bucketInfo.RequestPayment); errCode != nil {
				writeXMLErrorResponse(w, d, *errCode)
				return
			}
		}
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

	// Build service request
	req := &object.PutObjectRequest{
		Bucket:        bucket,
		Key:           key,
		Body:          body,
		ContentLength: contentLength,
		StorageClass:  d.Req.Header.Get(s3consts.XAmzStorageClass),
		Owner:         d.S3Info.OwnerID,
	}

	// Parse SSE-C headers (if present)
	ssecHeaders, errCode := parseSSECHeaders(d.Req)
	if errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}
	if ssecHeaders != nil {
		req.SSEC = &object.SSECParams{
			Algorithm: ssecHeaders.Algorithm,
			Key:       ssecHeaders.Key,
			KeyMD5:    ssecHeaders.KeyMD5,
		}
	}

	// Parse SSE-KMS headers (if present)
	ssekmsHeaders, errCode := parseSSEKMSHeaders(d.Req)
	if errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}
	if ssekmsHeaders != nil {
		req.SSEKMS = &object.SSEKMSParams{
			KeyID:   ssekmsHeaders.KeyID,
			Context: ssekmsHeaders.Context,
		}
	}

	// If no SSE headers provided, check bucket default encryption
	if req.SSEC == nil && req.SSEKMS == nil {
		if bucketInfo, exists := s.bucketStore.GetBucket(bucket); exists && bucketInfo.Encryption != nil {
			// Apply bucket default encryption
			if kmsParams := getBucketDefaultEncryption(bucketInfo.Encryption); kmsParams != nil {
				req.SSEKMS = kmsParams
			}
		}
	}

	// Call service layer
	result, err := s.svc.Objects().PutObject(ctx, req)
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	// Set response headers
	w.Header().Set("ETag", "\""+result.ETag+"\"")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))

	// Set version ID if versioning is enabled
	if result.VersionID != "" {
		w.Header().Set(s3consts.XAmzVersionID, result.VersionID)
	}

	// SSE-C response headers
	if result.SSECustomerKeyMD5 != "" {
		w.Header().Set(s3consts.XAmzServerSideEncryptionCustomerAlgo, result.SSEAlgorithm)
		w.Header().Set(s3consts.XAmzServerSideEncryptionCustomerKeyMD5, result.SSECustomerKeyMD5)
	}

	// SSE-KMS response headers
	if result.SSEKMSKeyID != "" {
		w.Header().Set(s3consts.XAmzServerSideEncryption, result.SSEAlgorithm)
		w.Header().Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, result.SSEKMSKeyID)
		if result.SSEKMSContext != "" {
			w.Header().Set(s3consts.XAmzServerSideEncryptionContext, result.SSEKMSContext)
		}
	}

	w.WriteHeader(http.StatusOK)

	// Record usage metrics
	s.usageCollector.RecordRequest(d.S3Info.OwnerID, bucket, "PutObject")
	s.usageCollector.RecordStorageDelta(d.S3Info.OwnerID, bucket, d.Req.ContentLength, 1, req.StorageClass)
	s.usageCollector.RecordBandwidth(d.S3Info.OwnerID, bucket, d.Req.ContentLength, usage.DirectionIngress)
}

// GetObjectHandler retrieves an object.
// GET /{bucket}/{key}
func (s *MetadataServer) GetObjectHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Check expected bucket owner header if provided
	if bucket != "" {
		bucketInfo, exists := s.bucketStore.GetBucket(bucket)
		if exists {
			if errCode := checkExpectedBucketOwner(d.Req, bucketInfo.OwnerID); errCode != nil {
				writeXMLErrorResponse(w, d, *errCode)
				return
			}
			if errCode := checkRequestPayer(d.Req, bucketInfo.RequestPayment); errCode != nil {
				writeXMLErrorResponse(w, d, *errCode)
				return
			}
		}
	}

	// Parse versionId query parameter
	versionID := d.Req.URL.Query().Get("versionId")

	// Build service request
	req := &object.GetObjectRequest{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
	}

	// Parse Range header
	rangeHeader := d.Req.Header.Get("Range")
	if rangeHeader != "" {
		// Parse range - format: "bytes=start-end"
		// Use MaxUint64 as sentinel for "unknown size" - service layer will validate
		offset, length, valid := parseRangeHeader(rangeHeader, ^uint64(0))
		if valid {
			req.Range = &object.ByteRange{
				Start:  offset,
				Length: length,
			}
		}
	}

	// Parse SSE-C headers
	ssecHeaders, errCode := parseSSECHeaders(d.Req)
	if errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}
	if ssecHeaders != nil {
		req.SSEC = &object.SSECParams{
			Algorithm: ssecHeaders.Algorithm,
			Key:       ssecHeaders.Key,
			KeyMD5:    ssecHeaders.KeyMD5,
		}
	}

	// Parse SSE-KMS key ID (for validation)
	requestKMSKeyID := d.Req.Header.Get(s3consts.XAmzServerSideEncryptionAwsKmsKeyID)
	req.SSEKMSKeyID = requestKMSKeyID

	// Check for conflicting SSE headers (both SSE-C and SSE-KMS)
	if req.SSEC != nil && requestKMSKeyID != "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidEncryptionAlgorithm)
		return
	}

	// Parse conditional headers
	if v := d.Req.Header.Get("If-Match"); v != "" {
		req.IfMatch = strings.Trim(v, "\"")
	}
	if v := d.Req.Header.Get("If-None-Match"); v != "" {
		req.IfNoneMatch = strings.Trim(v, "\"")
	}
	if v := d.Req.Header.Get("If-Modified-Since"); v != "" {
		if t, err := time.Parse(http.TimeFormat, v); err == nil {
			req.IfModifiedSince = &t
		}
	}
	if v := d.Req.Header.Get("If-Unmodified-Since"); v != "" {
		if t, err := time.Parse(http.TimeFormat, v); err == nil {
			req.IfUnmodifiedSince = &t
		}
	}

	// Call service layer
	result, err := s.svc.Objects().GetObject(ctx, req)
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}
	defer result.Body.Close()

	// Set response headers
	w.Header().Set("ETag", "\""+result.Metadata.ETag+"\"")
	w.Header().Set("Last-Modified", result.Metadata.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Type", result.Metadata.ContentType)
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))

	// Set version ID if versioning is enabled or a specific version was requested
	if result.Object != nil {
		if bucketInfo, exists := s.bucketStore.GetBucket(bucket); exists {
			if bucketInfo.Versioning == s3types.VersioningEnabled || versionID != "" {
				w.Header().Set(s3consts.XAmzVersionID, result.Object.ID.String())
			}
		}
	}

	if result.AcceptRanges != "" {
		w.Header().Set("Accept-Ranges", result.AcceptRanges)
	}

	// SSE headers
	if result.Metadata.SSEAlgorithm != "" {
		if result.Metadata.SSECustomerKeyMD5 != "" {
			w.Header().Set(s3consts.XAmzServerSideEncryptionCustomerAlgo, result.Metadata.SSEAlgorithm)
			w.Header().Set(s3consts.XAmzServerSideEncryptionCustomerKeyMD5, result.Metadata.SSECustomerKeyMD5)
		} else if result.Metadata.SSEKMSKeyID != "" {
			w.Header().Set(s3consts.XAmzServerSideEncryption, result.Metadata.SSEAlgorithm)
			w.Header().Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, result.Metadata.SSEKMSKeyID)
			if result.Metadata.SSEKMSContext != "" {
				w.Header().Set(s3consts.XAmzServerSideEncryptionContext, result.Metadata.SSEKMSContext)
			}
		}
	}

	if result.IsPartial && result.Range != nil {
		// Partial content response (206)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d",
			result.Range.Start, result.Range.End, result.Metadata.Size))
		w.Header().Set("Content-Length", strconv.FormatUint(result.Range.Length, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		// Full content response (200)
		w.Header().Set("Content-Length", strconv.FormatUint(result.Metadata.Size, 10))
		w.WriteHeader(http.StatusOK)
	}

	// Stream body to response
	bytesWritten, _ := io.Copy(w, result.Body)

	// Record usage metrics
	s.usageCollector.RecordRequest(d.S3Info.OwnerID, bucket, "GetObject")
	s.usageCollector.RecordBandwidth(d.S3Info.OwnerID, bucket, bytesWritten, usage.DirectionEgress)
}

func (s *MetadataServer) DeleteObjectHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Parse versionId query parameter
	versionID := d.Req.URL.Query().Get("versionId")

	// Check expected bucket owner header if provided
	if bucket != "" {
		bucketInfo, exists := s.bucketStore.GetBucket(bucket)
		if exists {
			if errCode := checkExpectedBucketOwner(d.Req, bucketInfo.OwnerID); errCode != nil {
				writeXMLErrorResponse(w, d, *errCode)
				return
			}
		}
	}

	// Call service layer with version ID
	result, err := s.svc.Objects().DeleteObjectWithVersion(ctx, bucket, key, versionID)
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))

	// Return version ID and delete marker info for versioned deletes
	if result != nil {
		if result.VersionID != "" {
			w.Header().Set(s3consts.XAmzVersionID, result.VersionID)
		}
		if result.DeleteMarker {
			w.Header().Set(s3consts.XAmzDeleteMarker, "true")
		}
	}

	w.WriteHeader(http.StatusNoContent)

	// Record usage metrics
	// Note: Storage delta for delete is tracked by service layer / GC
	s.usageCollector.RecordRequest(d.S3Info.OwnerID, bucket, "DeleteObject")
}

// ListObjectsHandler lists objects using the V1 API.
// GET /{bucket}
func (s *MetadataServer) ListObjectsHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket

	// Parse query parameters
	query := d.Req.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	marker := query.Get("marker")
	maxKeysStr := query.Get("max-keys")

	maxKeys := 1000
	if maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 && mk <= 1000 {
			maxKeys = mk
		}
	}

	// Call service layer
	listResult, err := s.svc.Objects().ListObjects(ctx, &object.ListObjectsRequest{
		Bucket:    bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
		Marker:    marker,
		MaxKeys:   maxKeys,
	})
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	// Build response contents
	var contents []s3types.ObjectContent
	for _, obj := range listResult.Contents {
		contents = append(contents, s3types.ObjectContent{
			Key:          obj.Key,
			LastModified: obj.LastModified.Format(time.RFC3339),
			ETag:         "\"" + obj.ETag + "\"",
			Size:         obj.Size,
			StorageClass: obj.StorageClass,
		})
	}

	// Build common prefixes
	var commonPrefixes []s3types.CommonPrefix
	for _, p := range listResult.CommonPrefixes {
		commonPrefixes = append(commonPrefixes, s3types.CommonPrefix{Prefix: p})
	}

	result := s3types.ListObjectsResult{
		Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:           listResult.Name,
		Prefix:         listResult.Prefix,
		Marker:         listResult.Marker,
		Delimiter:      listResult.Delimiter,
		MaxKeys:        listResult.MaxKeys,
		IsTruncated:    listResult.IsTruncated,
		NextMarker:     listResult.NextMarker,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ")
	encoder.Encode(result)

	// Record usage metrics
	s.usageCollector.RecordRequest(d.S3Info.OwnerID, bucket, "ListObjects")
}

// HeadObjectHandler returns object metadata without the body.
// HEAD /{bucket}/{key}
func (s *MetadataServer) HeadObjectHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Check expected bucket owner header if provided
	if bucket != "" {
		bucketInfo, exists := s.bucketStore.GetBucket(bucket)
		if exists {
			if errCode := checkExpectedBucketOwner(d.Req, bucketInfo.OwnerID); errCode != nil {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			if errCode := checkRequestPayer(d.Req, bucketInfo.RequestPayment); errCode != nil {
				w.WriteHeader(http.StatusForbidden)
				return
			}
		}
	}

	// Call service layer
	result, err := s.svc.Objects().HeadObject(ctx, bucket, key)
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	// Parse SSE-C headers from request
	ssecHeaders, errCode := parseSSECHeaders(d.Req)
	if errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}

	// Validate SSE-KMS key ID if provided in request
	requestKeyID := d.Req.Header.Get(s3consts.XAmzServerSideEncryptionAwsKmsKeyID)
	if requestKeyID != "" && result.Metadata.SSEKMSKeyID != "" && requestKeyID != result.Metadata.SSEKMSKeyID {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Check for conflicting SSE headers (both SSE-C and SSE-KMS)
	if ssecHeaders != nil && requestKeyID != "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidEncryptionAlgorithm)
		return
	}

	// Validate SSE-C: if object is SSE-C encrypted, request must include matching key
	if result.Metadata.SSECustomerKeyMD5 != "" {
		// Object is SSE-C encrypted - key is required
		if ssecHeaders == nil {
			writeXMLErrorResponse(w, d, s3err.ErrSSECustomerKeyMissing)
			return
		}
		// Validate key MD5 matches
		if ssecHeaders.KeyMD5 != result.Metadata.SSECustomerKeyMD5 {
			writeXMLErrorResponse(w, d, s3err.ErrSSECustomerKeyMD5Mismatch)
			return
		}
	}

	// Handle conditional requests (If-None-Match)
	ifNoneMatch := d.Req.Header.Get("If-None-Match")
	if ifNoneMatch != "" {
		// Strip quotes from the header value
		ifNoneMatch = strings.Trim(ifNoneMatch, "\"")
		if ifNoneMatch == result.Metadata.ETag {
			// Set headers before 304 response
			w.Header().Set("ETag", "\""+result.Metadata.ETag+"\"")
			if result.Metadata.SSEKMSKeyID != "" {
				w.Header().Set(s3consts.XAmzServerSideEncryption, result.Metadata.SSEAlgorithm)
				w.Header().Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, result.Metadata.SSEKMSKeyID)
			}
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// Set response headers
	w.Header().Set("Content-Length", strconv.FormatUint(result.Metadata.Size, 10))
	w.Header().Set("ETag", "\""+result.Metadata.ETag+"\"")
	w.Header().Set("Last-Modified", result.Metadata.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Type", result.Metadata.ContentType)
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))

	// Return SSE headers if encrypted
	if result.Metadata.SSEAlgorithm != "" {
		if result.Metadata.SSECustomerKeyMD5 != "" {
			w.Header().Set(s3consts.XAmzServerSideEncryptionCustomerAlgo, result.Metadata.SSEAlgorithm)
			w.Header().Set(s3consts.XAmzServerSideEncryptionCustomerKeyMD5, result.Metadata.SSECustomerKeyMD5)
		} else if result.Metadata.SSEKMSKeyID != "" {
			w.Header().Set(s3consts.XAmzServerSideEncryption, result.Metadata.SSEAlgorithm)
			w.Header().Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, result.Metadata.SSEKMSKeyID)
			if result.Metadata.SSEKMSContext != "" {
				w.Header().Set(s3consts.XAmzServerSideEncryptionContext, result.Metadata.SSEKMSContext)
			}
		}
	}

	w.WriteHeader(http.StatusOK)

	// Record usage metrics
	s.usageCollector.RecordRequest(d.S3Info.OwnerID, bucket, "HeadObject")
}

// ListObjectsV2Handler lists objects using the V2 API.
// GET /{bucket}?list-type=2
func (s *MetadataServer) ListObjectsV2Handler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket

	// Parse query parameters
	query := d.Req.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	maxKeysStr := query.Get("max-keys")
	continuationToken := query.Get("continuation-token")
	startAfter := query.Get("start-after")
	fetchOwner := query.Get("fetch-owner") == "true"

	maxKeys := 1000
	if maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 && mk <= 1000 {
			maxKeys = mk
		}
	}

	// Call service layer
	listResult, err := s.svc.Objects().ListObjectsV2(ctx, &object.ListObjectsV2Request{
		Bucket:            bucket,
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeys,
		ContinuationToken: continuationToken,
		StartAfter:        startAfter,
		FetchOwner:        fetchOwner,
	})
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	// Build response contents
	var contents []s3types.ListObjectEntry
	for _, obj := range listResult.Contents {
		entry := s3types.ListObjectEntry{
			Key:          obj.Key,
			LastModified: obj.LastModified.Format(time.RFC3339),
			ETag:         "\"" + obj.ETag + "\"",
			Size:         int64(obj.Size),
			StorageClass: obj.StorageClass,
		}

		if fetchOwner && obj.Owner != nil {
			entry.Owner = &s3types.ObjectOwner{
				ID:          obj.Owner.ID,
				DisplayName: obj.Owner.DisplayName,
			}
		}

		contents = append(contents, entry)
	}

	// Build common prefixes
	var commonPrefixes []s3types.CommonPrefix
	for _, p := range listResult.CommonPrefixes {
		commonPrefixes = append(commonPrefixes, s3types.CommonPrefix{Prefix: p})
	}

	result := s3types.ListObjectsV2Result{
		Xmlns:                 "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                  listResult.Name,
		Prefix:                listResult.Prefix,
		Delimiter:             listResult.Delimiter,
		MaxKeys:               listResult.MaxKeys,
		KeyCount:              listResult.KeyCount,
		IsTruncated:           listResult.IsTruncated,
		Contents:              contents,
		CommonPrefixes:        commonPrefixes,
		ContinuationToken:     listResult.ContinuationToken,
		NextContinuationToken: listResult.NextContinuationToken,
		StartAfter:            listResult.StartAfter,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ")
	encoder.Encode(result)

	// Record usage metrics
	s.usageCollector.RecordRequest(d.S3Info.OwnerID, bucket, "ListObjectsV2")
}

// CopyObjectHandler copies an object within/between buckets.
// PUT /{bucket}/{key} with x-amz-copy-source header
func (s *MetadataServer) CopyObjectHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	destBucket := d.S3Info.Bucket
	destKey := d.S3Info.Key

	// Parse copy source header
	copySource := d.Req.Header.Get(s3consts.XAmzCopySource)
	if copySource == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Remove leading slash and URL decode
	copySource = strings.TrimPrefix(copySource, "/")
	copySource, _ = url.QueryUnescape(copySource)

	// Parse bucket/key from copy source
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) != 2 {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}
	srcBucket, srcKey := parts[0], parts[1]

	// Check expected bucket owners if provided
	if destBucket != "" {
		destBucketInfo, exists := s.bucketStore.GetBucket(destBucket)
		if exists {
			if errCode := checkExpectedBucketOwner(d.Req, destBucketInfo.OwnerID); errCode != nil {
				writeXMLErrorResponse(w, d, *errCode)
				return
			}
		}
	}

	if srcBucket != "" {
		srcBucketInfo, exists := s.bucketStore.GetBucket(srcBucket)
		if exists {
			if errCode := checkExpectedSourceBucketOwner(d.Req, srcBucketInfo.OwnerID); errCode != nil {
				writeXMLErrorResponse(w, d, *errCode)
				return
			}
		}
	}

	// Parse directives from headers
	metadataDirective := strings.ToUpper(d.Req.Header.Get(s3consts.XAmzMetadataDirective))
	if metadataDirective == "" {
		metadataDirective = "COPY"
	}
	taggingDirective := strings.ToUpper(d.Req.Header.Get(s3consts.XAmzTaggingDirective))
	if taggingDirective == "" {
		taggingDirective = "COPY"
	}

	// Parse conditional headers
	var copySourceIfMatch, copySourceIfNoneMatch string
	var copySourceIfModifiedSince, copySourceIfUnmodifiedSince *time.Time

	if v := d.Req.Header.Get(s3consts.XAmzCopySourceIfMatch); v != "" {
		copySourceIfMatch = v
	}
	if v := d.Req.Header.Get(s3consts.XAmzCopySourceIfNoneMatch); v != "" {
		copySourceIfNoneMatch = v
	}
	if v := d.Req.Header.Get(s3consts.XAmzCopySourceIfModifiedSince); v != "" {
		if t, err := time.Parse(http.TimeFormat, v); err == nil {
			copySourceIfModifiedSince = &t
		}
	}
	if v := d.Req.Header.Get(s3consts.XAmzCopySourceIfUnmodifiedSince); v != "" {
		if t, err := time.Parse(http.TimeFormat, v); err == nil {
			copySourceIfUnmodifiedSince = &t
		}
	}

	// Parse SSE-KMS headers for destination encryption (if present)
	var ssekms *object.SSEKMSParams
	ssekmsHeaders, errCode := parseSSEKMSHeaders(d.Req)
	if errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}
	if ssekmsHeaders != nil {
		ssekms = &object.SSEKMSParams{
			KeyID:   ssekmsHeaders.KeyID,
			Context: ssekmsHeaders.Context,
		}
	}

	// If no SSE headers provided, check destination bucket default encryption
	if ssekms == nil {
		if bucketInfo, exists := s.bucketStore.GetBucket(destBucket); exists && bucketInfo.Encryption != nil {
			ssekms = getBucketDefaultEncryption(bucketInfo.Encryption)
		}
	}

	// Call service layer
	result, err := s.svc.Objects().CopyObject(ctx, &object.CopyObjectRequest{
		SourceBucket:                srcBucket,
		SourceKey:                   srcKey,
		DestBucket:                  destBucket,
		DestKey:                     destKey,
		MetadataDirective:           metadataDirective,
		TaggingDirective:            taggingDirective,
		CopySourceIfMatch:           copySourceIfMatch,
		CopySourceIfNoneMatch:       copySourceIfNoneMatch,
		CopySourceIfModifiedSince:   copySourceIfModifiedSince,
		CopySourceIfUnmodifiedSince: copySourceIfUnmodifiedSince,
		SSEKMS:                      ssekms,
	})
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	// Build XML response
	type CopyObjectResult struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		LastModified string   `xml:"LastModified"`
		ETag         string   `xml:"ETag"`
	}

	xmlResult := CopyObjectResult{
		LastModified: result.LastModified.UTC().Format(time.RFC3339),
		ETag:         "\"" + result.ETag + "\"",
	}

	// SSE-C response headers
	if result.SSECustomerKeyMD5 != "" {
		w.Header().Set(s3consts.XAmzServerSideEncryptionCustomerAlgo, result.SSEAlgorithm)
		w.Header().Set(s3consts.XAmzServerSideEncryptionCustomerKeyMD5, result.SSECustomerKeyMD5)
	}

	// SSE-KMS response headers
	if result.SSEKMSKeyID != "" {
		w.Header().Set(s3consts.XAmzServerSideEncryption, result.SSEAlgorithm)
		w.Header().Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, result.SSEKMSKeyID)
		if result.SSEKMSContext != "" {
			w.Header().Set(s3consts.XAmzServerSideEncryptionContext, result.SSEKMSContext)
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(xmlResult)

	// Record usage metrics
	// Note: Storage delta for copy requires source object size from service layer
	s.usageCollector.RecordRequest(d.S3Info.OwnerID, destBucket, "CopyObject")
}

// DeleteObjectsHandler deletes multiple objects in a single request.
// POST /{bucket}?delete
func (s *MetadataServer) DeleteObjectsHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket

	// Check expected bucket owner header if provided
	if bucket != "" {
		bucketInfo, exists := s.bucketStore.GetBucket(bucket)
		if exists {
			if errCode := checkExpectedBucketOwner(d.Req, bucketInfo.OwnerID); errCode != nil {
				writeXMLErrorResponse(w, d, *errCode)
				return
			}
		}
	}

	// Parse request body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	var req s3types.DeleteObjectsRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Limit to 1000 objects per request (S3 limit)
	if len(req.Objects) > 1000 {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Build service request
	var objects []object.DeleteObjectEntry
	for _, obj := range req.Objects {
		objects = append(objects, object.DeleteObjectEntry{
			Key:       obj.Key,
			VersionID: obj.VersionID,
		})
	}

	// Call service layer
	result, err := s.svc.Objects().DeleteObjects(ctx, &object.DeleteObjectsRequest{
		Bucket:  bucket,
		Objects: objects,
		Quiet:   req.Quiet,
	})
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	// Build response
	var deleted []s3types.DeletedObject
	for _, d := range result.Deleted {
		deleted = append(deleted, s3types.DeletedObject{
			Key:                   d.Key,
			VersionID:             d.VersionID,
			DeleteMarker:          d.DeleteMarker,
			DeleteMarkerVersionID: d.DeleteMarkerVersionID,
		})
	}

	var deleteErrors []s3types.DeleteError
	for _, e := range result.Errors {
		deleteErrors = append(deleteErrors, s3types.DeleteError{
			Key:       e.Key,
			VersionID: e.VersionID,
			Code:      e.Code,
			Message:   e.Message,
		})
	}

	xmlResult := s3types.DeleteObjectsResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Deleted: deleted,
		Error:   deleteErrors,
	}

	// In quiet mode, only return errors
	if req.Quiet {
		xmlResult.Deleted = nil
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(xmlResult)

	// Record usage metrics
	s.usageCollector.RecordRequest(d.S3Info.OwnerID, bucket, "DeleteObjects")
}

// ============================================================================
// Not Yet Implemented Object Operations
// ============================================================================

// GetObjectAttributesHandler returns object attributes without full GET.
// GET /{bucket}/{key}?attributes
//
// Returns subset of HeadObject info based on x-amz-object-attributes header.
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html
func (s *MetadataServer) GetObjectAttributesHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Parse x-amz-object-attributes header (required)
	attrsHeader := d.Req.Header.Get(s3consts.XAmzObjectAttributes)
	if attrsHeader == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Parse requested attributes
	requestedAttrs := parseObjectAttributes(attrsHeader)
	if len(requestedAttrs) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidArgument)
		return
	}

	// Check expected bucket owner header if provided
	if bucket != "" {
		bucketInfo, exists := s.bucketStore.GetBucket(bucket)
		if exists {
			if errCode := checkExpectedBucketOwner(d.Req, bucketInfo.OwnerID); errCode != nil {
				writeXMLErrorResponse(w, d, *errCode)
				return
			}
		}
	}

	// Get object metadata
	result, err := s.svc.Objects().HeadObject(ctx, bucket, key)
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	// Build response with only requested attributes
	resp := s3types.GetObjectAttributesResponse{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
	}

	for attr := range requestedAttrs {
		switch attr {
		case "ETag":
			// Strip any quotes from ETag
			etag := strings.Trim(result.Metadata.ETag, "\"")
			resp.ETag = etag
		case "Checksum":
			// Checksum data is not currently stored in ObjectRef
			// This will be nil/omitted until checksum storage is implemented
		case "ObjectParts":
			// Extract parts count from ETag format for multipart uploads
			// Multipart ETags have format: "etag-N" where N is parts count
			etag := strings.Trim(result.Metadata.ETag, "\"")
			if idx := strings.LastIndex(etag, "-"); idx > 0 {
				if partsCount, err := strconv.Atoi(etag[idx+1:]); err == nil && partsCount > 0 {
					resp.ObjectParts = &s3types.ObjectAttributesParts{
						TotalPartsCount: partsCount,
					}
				}
			}
		case "StorageClass":
			storageClass := result.Metadata.StorageClass
			if storageClass == "" {
				storageClass = "STANDARD"
			}
			resp.StorageClass = storageClass
		case "ObjectSize":
			size := int64(result.Metadata.Size)
			resp.ObjectSize = &size
		}
	}

	// Set Last-Modified header
	w.Header().Set("Last-Modified", result.Metadata.LastModified.Format(http.TimeFormat))
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	xml.NewEncoder(w).Encode(resp)
}

// parseObjectAttributes parses the x-amz-object-attributes header into a set of attribute names.
// Valid values: ETag, Checksum, ObjectParts, StorageClass, ObjectSize
func parseObjectAttributes(header string) map[string]struct{} {
	validAttrs := map[string]struct{}{
		"ETag":         {},
		"Checksum":     {},
		"ObjectParts":  {},
		"StorageClass": {},
		"ObjectSize":   {},
	}

	result := make(map[string]struct{})
	for _, attr := range strings.Split(header, ",") {
		attr = strings.TrimSpace(attr)
		if _, valid := validAttrs[attr]; valid {
			result[attr] = struct{}{}
		}
	}
	return result
}

// PostObjectHandler handles form-based uploads (browser uploads).
// POST /{bucket}
//
// Allows uploading objects via HTML form with policy-based authorization.
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
func (s *MetadataServer) PostObjectHandler(d *data.Data, w http.ResponseWriter) {
	ctx := d.Ctx
	bucket := d.S3Info.Bucket

	// Parse multipart form (32MB max memory, rest goes to temp files)
	if err := d.Req.ParseMultipartForm(32 << 20); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedPOSTRequest)
		return
	}

	form := d.Req.MultipartForm
	if form == nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedPOSTRequest)
		return
	}

	// Extract form fields
	getField := func(name string) string {
		if vals, ok := form.Value[name]; ok && len(vals) > 0 {
			return vals[0]
		}
		// Try case-insensitive lookup
		for k, vals := range form.Value {
			if strings.EqualFold(k, name) && len(vals) > 0 {
				return vals[0]
			}
		}
		return ""
	}

	// Build PostFormData from form fields
	formData := &postFormData{
		Key:                   getField("key"),
		Policy:                getField("policy"),
		Signature:             getField("x-amz-signature"),
		Algorithm:             getField("x-amz-algorithm"),
		Date:                  getField("x-amz-date"),
		Credential:            getField("x-amz-credential"),
		ACL:                   getField("acl"),
		ContentType:           getField("Content-Type"),
		ContentDisposition:    getField("Content-Disposition"),
		SuccessActionRedirect: getField("success_action_redirect"),
		Tagging:               getField("tagging"),
	}

	// Parse success_action_status
	if statusStr := getField("success_action_status"); statusStr != "" {
		if status, err := strconv.Atoi(statusStr); err == nil {
			formData.SuccessActionStatus = status
		}
	}

	// Collect x-amz-meta-* fields
	formData.Metadata = make(map[string]string)
	for k, vals := range form.Value {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") && len(vals) > 0 {
			metaKey := strings.TrimPrefix(strings.ToLower(k), "x-amz-meta-")
			formData.Metadata[metaKey] = vals[0]
		}
	}

	// Get file from form
	fileHeaders := form.File["file"]
	if len(fileHeaders) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMissingFields)
		return
	}
	fileHeader := fileHeaders[0]
	formData.Filename = fileHeader.Filename
	formData.FileSize = fileHeader.Size

	// Verify POST policy using the auth filter's IAM manager
	// Note: The auth filter already validated that this is a POST policy request
	// and let it through. Now we verify the policy signature and conditions.
	result, errCode := s.verifyPostPolicy(ctx, formData, bucket)
	if errCode != s3err.ErrNone {
		writeXMLErrorResponse(w, d, errCode)
		return
	}

	// Open file for reading
	file, err := fileHeader.Open()
	if err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}
	defer file.Close()

	// Determine content type
	contentType := formData.ContentType
	if contentType == "" {
		contentType = fileHeader.Header.Get("Content-Type")
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Determine owner ID
	ownerID := ""
	if result.Identity != nil && result.Identity.Account != nil {
		ownerID = result.Identity.Account.ID
	}

	// Build service request
	// TODO: ContentType should be passed to PutObjectRequest when supported
	_ = contentType
	req := &object.PutObjectRequest{
		Bucket:        bucket,
		Key:           result.Key,
		Body:          file,
		ContentLength: formData.FileSize,
		Owner:         ownerID,
	}

	// Store object using service layer
	putResult, err := s.svc.Objects().PutObject(ctx, req)
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	// Record usage
	if s.usageCollector != nil {
		s.usageCollector.RecordStorageDelta(ownerID, bucket, formData.FileSize, 1, "STANDARD")
		s.usageCollector.RecordBandwidth(ownerID, bucket, formData.FileSize, usage.DirectionIngress)
	}

	// Handle response based on success_action_redirect or success_action_status
	if formData.SuccessActionRedirect != "" {
		// Redirect to the specified URL with bucket, key, etag as query params
		redirectURL := formData.SuccessActionRedirect
		if strings.Contains(redirectURL, "?") {
			redirectURL += "&"
		} else {
			redirectURL += "?"
		}
		redirectURL += fmt.Sprintf("bucket=%s&key=%s&etag=%s",
			url.QueryEscape(bucket),
			url.QueryEscape(result.Key),
			url.QueryEscape(putResult.ETag))
		http.Redirect(w, d.Req, redirectURL, http.StatusSeeOther)
		return
	}

	// Return based on success_action_status (default 204)
	status := formData.SuccessActionStatus
	if status == 0 {
		status = http.StatusNoContent
	}

	switch status {
	case http.StatusOK, http.StatusCreated:
		// Return XML response
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("ETag", "\""+putResult.ETag+"\"")
		w.WriteHeader(status)
		response := s3types.PostObjectResponse{
			Bucket: bucket,
			Key:    result.Key,
			ETag:   putResult.ETag,
		}
		xml.NewEncoder(w).Encode(response)
	default:
		// 204 No Content
		w.Header().Set("ETag", "\""+putResult.ETag+"\"")
		w.WriteHeader(http.StatusNoContent)
	}
}

// postFormData holds parsed POST form fields
type postFormData struct {
	Key                   string
	Policy                string
	Signature             string
	Algorithm             string
	Date                  string
	Credential            string
	ACL                   string
	ContentType           string
	ContentDisposition    string
	SuccessActionRedirect string
	SuccessActionStatus   int
	Tagging               string
	Metadata              map[string]string
	Filename              string
	FileSize              int64
}

// postPolicyResult holds the result of policy verification
type postPolicyResult struct {
	Identity *iam.Identity
	Key      string
}

// verifyPostPolicy verifies the POST policy signature and conditions
func (s *MetadataServer) verifyPostPolicy(ctx context.Context, form *postFormData, bucket string) (*postPolicyResult, s3err.ErrorCode) {
	// Get IAM manager from chain (it's in the auth filter)
	iamManager := s.chain.GetIAMManager()
	if iamManager == nil {
		return nil, s3err.ErrInternalError
	}

	// Create verifier and verify
	verifier := signature.NewPostPolicyVerifier(iamManager, "us-east-1")
	sigForm := &signature.PostFormData{
		Key:                   form.Key,
		Policy:                form.Policy,
		Signature:             form.Signature,
		Algorithm:             form.Algorithm,
		Date:                  form.Date,
		Credential:            form.Credential,
		ACL:                   form.ACL,
		ContentType:           form.ContentType,
		SuccessActionRedirect: form.SuccessActionRedirect,
		SuccessActionStatus:   form.SuccessActionStatus,
		Metadata:              form.Metadata,
		Filename:              form.Filename,
		FileSize:              form.FileSize,
	}

	result, errCode := verifier.VerifyPostForm(ctx, sigForm, bucket)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	return &postPolicyResult{
		Identity: result.Identity,
		Key:      result.Key,
	}, s3err.ErrNone
}

// GetObjectTorrentHandler returns torrent file for an object.
// GET /{bucket}/{key}?torrent
//
// Legacy AWS feature - not commonly used.
func (s *MetadataServer) GetObjectTorrentHandler(d *data.Data, w http.ResponseWriter) {
	// Torrent support is a legacy AWS feature
	// Not planned for implementation
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// SelectObjectContentHandler performs S3 Select queries on objects.
// POST /{bucket}/{key}?select&select-type=2
//
// Complex feature requiring SQL parsing - not planned for initial release.
func (s *MetadataServer) SelectObjectContentHandler(d *data.Data, w http.ResponseWriter) {
	// S3 Select requires SQL parsing for CSV/JSON/Parquet
	// Complex feature - not planned for initial release
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// WriteGetObjectResponseHandler writes Lambda response for Object Lambda.
// POST /WriteGetObjectResponse
//
// AWS Lambda-specific feature - not applicable to ZapFS.
func (s *MetadataServer) WriteGetObjectResponseHandler(d *data.Data, w http.ResponseWriter) {
	// Lambda@Edge specific feature - not applicable
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// CreateSessionHandler creates an S3 Express session.
// GET /{bucket}?session
//
// S3 Express One Zone specific feature - not applicable to ZapFS.
func (s *MetadataServer) CreateSessionHandler(d *data.Data, w http.ResponseWriter) {
	// S3 Express One Zone specific - not applicable
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}
