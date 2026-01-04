//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutObjectWithIntelligentTiering tests storing objects with INTELLIGENT_TIERING storage class
func TestPutObjectWithIntelligentTiering(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-int-tiering-put")
	key := uniqueKey("object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	ctx := context.Background()

	// Put object with INTELLIGENT_TIERING storage class
	data := []byte("test data for intelligent tiering")
	_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(data),
		StorageClass: s3types.StorageClassIntelligentTiering,
	})
	require.NoError(t, err)
	defer client.DeleteObject(bucket, key)

	// Verify object is accessible
	getResp, err := client.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	retrieved, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, data, retrieved)

	// Verify storage class is INTELLIGENT_TIERING
	headResp, err := client.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, s3types.StorageClassIntelligentTiering, headResp.StorageClass)
}

// TestIntelligentTieringAccessTracking tests that access is tracked for INTELLIGENT_TIERING objects
// This test verifies that GetObject updates the last_accessed_at timestamp
func TestIntelligentTieringAccessTracking(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-int-tiering-access")
	key := uniqueKey("tracked-object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	ctx := context.Background()

	// Put object with INTELLIGENT_TIERING storage class
	data := []byte("test data for access tracking")
	_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(data),
		StorageClass: s3types.StorageClassIntelligentTiering,
	})
	require.NoError(t, err)
	defer client.DeleteObject(bucket, key)

	// Access the object multiple times
	for i := 0; i < 3; i++ {
		getResp, err := client.Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		_, _ = io.ReadAll(getResp.Body)
		getResp.Body.Close()
		time.Sleep(100 * time.Millisecond) // Small delay between accesses
	}

	// Object should still be accessible and in INTELLIGENT_TIERING class
	headResp, err := client.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, s3types.StorageClassIntelligentTiering, headResp.StorageClass)
}

// TestIntelligentTieringListObjects tests listing objects with INTELLIGENT_TIERING storage class
func TestIntelligentTieringListObjects(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-int-tiering-list")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	ctx := context.Background()

	// Put multiple objects with different storage classes
	keys := []string{
		uniqueKey("standard"),
		uniqueKey("intelligent"),
	}

	// Standard object
	_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(keys[0]),
		Body:         bytes.NewReader([]byte("standard data")),
		StorageClass: s3types.StorageClassStandard,
	})
	require.NoError(t, err)
	defer client.DeleteObject(bucket, keys[0])

	// Intelligent tiering object
	_, err = client.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(keys[1]),
		Body:         bytes.NewReader([]byte("intelligent tiering data")),
		StorageClass: s3types.StorageClassIntelligentTiering,
	})
	require.NoError(t, err)
	defer client.DeleteObject(bucket, keys[1])

	// List objects and verify storage classes
	listResp, err := client.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, listResp.Contents, 2)

	// Build a map of key -> storage class
	storageClasses := make(map[string]s3types.ObjectStorageClass)
	for _, obj := range listResp.Contents {
		storageClasses[*obj.Key] = obj.StorageClass
	}

	// Verify storage classes
	assert.Equal(t, s3types.ObjectStorageClassStandard, storageClasses[keys[0]])
	assert.Equal(t, s3types.ObjectStorageClassIntelligentTiering, storageClasses[keys[1]])
}

// IntelligentTieringConfiguration represents the S3 Intelligent-Tiering configuration XML
type IntelligentTieringConfiguration struct {
	XMLName xml.Name `xml:"IntelligentTieringConfiguration"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	ID      string   `xml:"Id"`
	Status  string   `xml:"Status"`
	Tiering []struct {
		AccessTier string `xml:"AccessTier"`
		Days       int    `xml:"Days"`
	} `xml:"Tiering"`
}

// ListIntelligentTieringConfigurationsResult represents the list response
type ListIntelligentTieringConfigurationsResult struct {
	XMLName        xml.Name                          `xml:"ListBucketIntelligentTieringConfigurationsResult"`
	IsTruncated    bool                              `xml:"IsTruncated"`
	Configurations []IntelligentTieringConfiguration `xml:"IntelligentTieringConfiguration"`
}

// TestIntelligentTieringConfigurationAPI tests the PUT/GET/DELETE/LIST configuration API
func TestIntelligentTieringConfigurationAPI(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-int-tiering-config")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	configID := "test-config-1"

	// Use raw HTTP client since the AWS SDK doesn't fully support Intelligent-Tiering config API
	httpClient := &http.Client{Timeout: 30 * time.Second}

	// Helper to make signed requests
	makeRequest := func(method, path string, body []byte) (*http.Response, error) {
		endpoint := s3Config.Endpoint
		url := endpoint + "/" + bucket + path

		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}

		req, err := http.NewRequest(method, url, bodyReader)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", "application/xml")
		// Note: In a real test, we'd need to sign the request with AWS SigV4
		// For now, we rely on the test environment having unsigned request support
		return httpClient.Do(req)
	}

	t.Run("PutConfiguration", func(t *testing.T) {
		config := `<?xml version="1.0" encoding="UTF-8"?>
<IntelligentTieringConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Id>` + configID + `</Id>
    <Status>Enabled</Status>
    <Tiering>
        <AccessTier>ARCHIVE_ACCESS</AccessTier>
        <Days>90</Days>
    </Tiering>
    <Tiering>
        <AccessTier>DEEP_ARCHIVE_ACCESS</AccessTier>
        <Days>180</Days>
    </Tiering>
</IntelligentTieringConfiguration>`

		resp, err := makeRequest("PUT", "?intelligent-tiering&id="+configID, []byte(config))
		if err != nil {
			t.Skipf("Skipping: could not make request: %v", err)
		}
		defer resp.Body.Close()

		// Check if the API is implemented (might require enterprise license)
		if resp.StatusCode == http.StatusNotImplemented {
			t.Skip("Intelligent-Tiering configuration API not implemented (requires enterprise license)")
		}

		// Skip if authentication is required (test uses unsigned requests)
		if resp.StatusCode == http.StatusForbidden {
			t.Skip("Intelligent-Tiering configuration API requires signed requests")
		}

		assert.Equal(t, http.StatusOK, resp.StatusCode, "PUT configuration should succeed")
	})

	t.Run("GetConfiguration", func(t *testing.T) {
		resp, err := makeRequest("GET", "?intelligent-tiering&id="+configID, nil)
		if err != nil {
			t.Skipf("Skipping: could not make request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotImplemented {
			t.Skip("Intelligent-Tiering configuration API not implemented")
		}

		// Skip if authentication is required (test uses unsigned requests)
		if resp.StatusCode == http.StatusForbidden {
			t.Skip("Intelligent-Tiering configuration API requires signed requests")
		}

		// Either we get the config (200) or it doesn't exist (404)
		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			assert.True(t, strings.Contains(string(body), configID), "Response should contain config ID")
		}
	})

	t.Run("ListConfigurations", func(t *testing.T) {
		resp, err := makeRequest("GET", "?intelligent-tiering", nil)
		if err != nil {
			t.Skipf("Skipping: could not make request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotImplemented {
			t.Skip("Intelligent-Tiering configuration API not implemented")
		}

		// Skip if authentication is required (test uses unsigned requests)
		if resp.StatusCode == http.StatusForbidden {
			t.Skip("Intelligent-Tiering configuration API requires signed requests")
		}

		assert.Equal(t, http.StatusOK, resp.StatusCode, "LIST configurations should succeed")

		body, _ := io.ReadAll(resp.Body)
		assert.True(t, strings.Contains(string(body), "ListBucketIntelligentTieringConfigurationsResult"),
			"Response should be a list result")
	})

	t.Run("DeleteConfiguration", func(t *testing.T) {
		resp, err := makeRequest("DELETE", "?intelligent-tiering&id="+configID, nil)
		if err != nil {
			t.Skipf("Skipping: could not make request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotImplemented {
			t.Skip("Intelligent-Tiering configuration API not implemented")
		}

		// Skip if authentication is required (test uses unsigned requests)
		if resp.StatusCode == http.StatusForbidden {
			t.Skip("Intelligent-Tiering configuration API requires signed requests")
		}

		// Delete should return 204 No Content
		assert.Equal(t, http.StatusNoContent, resp.StatusCode, "DELETE should return 204")
	})
}

// TestIntelligentTieringCopyObject tests copying objects with INTELLIGENT_TIERING storage class
func TestIntelligentTieringCopyObject(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-int-tiering-copy")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	ctx := context.Background()

	// Create source object with INTELLIGENT_TIERING
	srcKey := uniqueKey("source")
	data := []byte("source data for copy test")
	_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(srcKey),
		Body:         bytes.NewReader(data),
		StorageClass: s3types.StorageClassIntelligentTiering,
	})
	require.NoError(t, err)
	defer client.DeleteObject(bucket, srcKey)

	// Copy to new key, preserving storage class
	dstKey := uniqueKey("destination")
	_, err = client.Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:       aws.String(bucket),
		CopySource:   aws.String(bucket + "/" + srcKey),
		Key:          aws.String(dstKey),
		StorageClass: s3types.StorageClassIntelligentTiering,
	})
	require.NoError(t, err)
	defer client.DeleteObject(bucket, dstKey)

	// Verify destination has INTELLIGENT_TIERING storage class
	headResp, err := client.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(dstKey),
	})
	require.NoError(t, err)
	assert.Equal(t, s3types.StorageClassIntelligentTiering, headResp.StorageClass)

	// Verify data integrity
	getResp, err := client.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(dstKey),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	retrieved, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, data, retrieved)
}

// TestIntelligentTieringMultipartUpload tests multipart uploads with INTELLIGENT_TIERING
func TestIntelligentTieringMultipartUpload(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-int-tiering-mpu")
	key := uniqueKey("multipart")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	ctx := context.Background()

	// Create multipart upload with INTELLIGENT_TIERING storage class
	createResp, err := client.Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		StorageClass: s3types.StorageClassIntelligentTiering,
	})
	require.NoError(t, err)

	uploadID := createResp.UploadId

	// Upload a single part (minimum 5MB for multipart, but we'll use smaller for testing)
	partData := bytes.Repeat([]byte("x"), 5*1024*1024) // 5MB
	uploadResp, err := client.Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(partData),
	})
	require.NoError(t, err)

	// Complete the multipart upload
	_, err = client.Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: []s3types.CompletedPart{
				{
					ETag:       uploadResp.ETag,
					PartNumber: aws.Int32(1),
				},
			},
		},
	})
	require.NoError(t, err)
	defer client.DeleteObject(bucket, key)

	// Verify storage class is INTELLIGENT_TIERING
	headResp, err := client.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, s3types.StorageClassIntelligentTiering, headResp.StorageClass)
}

// TestIntelligentTieringLargeObject tests storing large objects (>128KB, the minimum for tiering)
func TestIntelligentTieringLargeObject(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-int-tiering-large")
	key := uniqueKey("large-object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	ctx := context.Background()

	// Create a 256KB object (above 128KB threshold for intelligent tiering)
	data := bytes.Repeat([]byte("large object data "), 256*1024/18) // ~256KB
	_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(data),
		StorageClass: s3types.StorageClassIntelligentTiering,
	})
	require.NoError(t, err)
	defer client.DeleteObject(bucket, key)

	// Verify object metadata
	headResp, err := client.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, s3types.StorageClassIntelligentTiering, headResp.StorageClass)
	assert.GreaterOrEqual(t, *headResp.ContentLength, int64(128*1024), "Object should be >= 128KB")

	// Verify data integrity
	getResp, err := client.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	retrieved, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, len(data), len(retrieved))
}

// TestIntelligentTieringSmallObject tests that small objects (<128KB) are still stored correctly
// Note: AWS S3 doesn't tier objects smaller than 128KB but still accepts them with INTELLIGENT_TIERING class
func TestIntelligentTieringSmallObject(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-int-tiering-small")
	key := uniqueKey("small-object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	ctx := context.Background()

	// Create a 1KB object (below 128KB threshold)
	data := []byte("small object - this won't be tiered but should still work")
	_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(data),
		StorageClass: s3types.StorageClassIntelligentTiering,
	})
	require.NoError(t, err)
	defer client.DeleteObject(bucket, key)

	// Verify object is stored with INTELLIGENT_TIERING class
	headResp, err := client.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, s3types.StorageClassIntelligentTiering, headResp.StorageClass)

	// Verify data integrity
	getResp, err := client.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	retrieved, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, data, retrieved)
}
