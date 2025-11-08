package api

import (
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"

	"zapfs/pkg/metadata/data"
	"zapfs/pkg/metadata/service/bucket"
	"zapfs/pkg/s3api/s3consts"
	"zapfs/pkg/s3api/s3err"
	"zapfs/pkg/s3api/s3types"
)

func (s *MetadataServer) CreateBucketHandler(d *data.Data, w http.ResponseWriter) {
	// Check if service layer is available
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Parse ACL from header (default to private)
	acl := d.Req.Header.Get(s3consts.XAmzACL)
	if acl == "" {
		acl = "private"
	}

	// Call service layer
	_, err := s.svc.Buckets().CreateBucket(d.Ctx, &bucket.CreateBucketRequest{
		Bucket:  d.S3Info.Bucket,
		OwnerID: d.S3Info.OwnerID,
		ACL:     acl,
	})
	if err != nil {
		var bucketErr *bucket.Error
		if errors.As(err, &bucketErr) {
			writeXMLErrorResponse(w, d, bucketErr.ToS3Error())
			return
		}
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *MetadataServer) DeleteBucketHandler(d *data.Data, w http.ResponseWriter) {
	// Check if service layer is available
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	if d.S3Info.Bucket == "" {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchBucket)
		return
	}

	// Call service layer
	err := s.svc.Buckets().DeleteBucket(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var bucketErr *bucket.Error
		if errors.As(err, &bucketErr) {
			writeXMLErrorResponse(w, d, bucketErr.ToS3Error())
			return
		}
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ListBucketsHandler lists all buckets owned by the authenticated user.
// GET /?max-buckets={max}&prefix={prefix}&continuation-token={token}&bucket-region={region}
func (s *MetadataServer) ListBucketsHandler(d *data.Data, w http.ResponseWriter) {
	// Check if service layer is available
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	query := d.Req.URL.Query()

	// Parse query parameters
	prefix := query.Get("prefix")
	bucketRegion := query.Get("bucket-region")
	continuationToken := query.Get("continuation-token")
	maxBucketsStr := query.Get("max-buckets")

	// Parse max-buckets (default: 10000, range: 1-10000)
	maxBuckets := 10000
	if maxBucketsStr != "" {
		if mb, err := strconv.Atoi(maxBucketsStr); err == nil && mb > 0 {
			maxBuckets = mb
		}
	}

	// Determine if this is a paginated request (for response formatting)
	usePagination := maxBucketsStr != "" || continuationToken != ""

	// Call service layer
	result, err := s.svc.Buckets().ListBuckets(d.Ctx, &bucket.ListBucketsRequest{
		OwnerID:           d.S3Info.OwnerID,
		Prefix:            prefix,
		BucketRegion:      bucketRegion,
		MaxBuckets:        maxBuckets,
		ContinuationToken: continuationToken,
	})
	if err != nil {
		var bucketErr *bucket.Error
		if errors.As(err, &bucketErr) {
			writeXMLErrorResponse(w, d, bucketErr.ToS3Error())
			return
		}
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Convert to response format
	var buckets []s3types.BucketInfo
	for _, bi := range result.Buckets {
		b := s3types.BucketInfo{
			Name:         bi.Name,
			CreationDate: bi.CreationDate.UTC().Format("2006-01-02T15:04:05.000Z"),
		}
		// Include region if specified in response (AWS includes it in paginated responses)
		if usePagination && bi.Region != "" {
			b.Region = bi.Region
		}
		buckets = append(buckets, b)
	}

	// Get display name from identity
	displayName := d.S3Info.OwnerID
	if d.Identity != nil && d.Identity.Account != nil {
		displayName = d.Identity.Account.DisplayName
	}

	// Build response
	resp := s3types.ListAllMyBucketsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: s3types.BucketOwner{
			ID:          d.S3Info.OwnerID,
			DisplayName: displayName,
		},
		Buckets: s3types.BucketList{
			Buckets: buckets,
		},
	}

	// Add pagination fields
	if result.IsTruncated && result.NextContinuationToken != "" {
		resp.ContinuationToken = result.NextContinuationToken
	}
	// Echo back prefix if provided (AWS spec requirement)
	if prefix != "" {
		resp.Prefix = prefix
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(resp)
}

// HeadBucketHandler checks if a bucket exists and the caller has permission.
// HEAD /{bucket}
func (s *MetadataServer) HeadBucketHandler(d *data.Data, w http.ResponseWriter) {
	// Check if service layer is available
	if s.svc == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if d.S3Info.Bucket == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Call service layer
	result, err := s.svc.Buckets().HeadBucket(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var bucketErr *bucket.Error
		if errors.As(err, &bucketErr) {
			if bucketErr.Code == bucket.ErrCodeNoSuchBucket {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Check expected bucket owner header if provided
	if errCode := checkExpectedBucketOwner(d.Req, result.OwnerID); errCode != nil {
		writeXMLErrorResponse(w, d, *errCode)
		return
	}

	// Set region header if available
	if result.Location != "" {
		w.Header().Set("x-amz-bucket-region", result.Location)
	}

	w.WriteHeader(http.StatusOK)
}

// GetBucketLocationHandler returns the region where the bucket resides.
// GET /{bucket}?location
func (s *MetadataServer) GetBucketLocationHandler(d *data.Data, w http.ResponseWriter) {
	// Check if service layer is available
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	if d.S3Info.Bucket == "" {
		writeXMLErrorResponse(w, d, s3err.ErrNoSuchBucket)
		return
	}

	// Call service layer
	result, err := s.svc.Buckets().GetBucketLocation(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var bucketErr *bucket.Error
		if errors.As(err, &bucketErr) {
			writeXMLErrorResponse(w, d, bucketErr.ToS3Error())
			return
		}
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Note: AWS returns empty string for us-east-1 (legacy behavior)
	resp := s3types.LocationConstraint{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Location: result.Location,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(resp)
}
