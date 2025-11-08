package api

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"zapfs/pkg/metadata/data"
	"zapfs/pkg/metadata/service/object"
	"zapfs/pkg/s3api/s3consts"
	"zapfs/pkg/s3api/s3err"
	"zapfs/pkg/s3api/s3types"
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

	// Build service request
	req := &object.PutObjectRequest{
		Bucket:        bucket,
		Key:           key,
		Body:          d.Req.Body,
		ContentLength: d.Req.ContentLength,
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

	// Call service layer
	result, err := s.svc.Objects().PutObject(ctx, req)
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	// Set response headers
	w.Header().Set("ETag", "\""+result.ETag+"\"")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))

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

	// Build service request
	req := &object.GetObjectRequest{
		Bucket: bucket,
		Key:    key,
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
	io.Copy(w, result.Body)
}

func (s *MetadataServer) DeleteObjectHandler(d *data.Data, w http.ResponseWriter) {
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
		}
	}

	// Call service layer
	_, err := s.svc.Objects().DeleteObject(ctx, bucket, key)
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)
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

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(xmlResult)
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
}
