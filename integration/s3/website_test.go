//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBucketWebsiteConfiguration tests the S3 bucket website configuration API.
func TestBucketWebsiteConfiguration(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("website-config")

	// Setup: create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	t.Run("PutBucketWebsite", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Client.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{
			Bucket: aws.String(bucket),
			WebsiteConfiguration: &s3types.WebsiteConfiguration{
				IndexDocument: &s3types.IndexDocument{
					Suffix: aws.String("index.html"),
				},
				ErrorDocument: &s3types.ErrorDocument{
					Key: aws.String("error.html"),
				},
			},
		})
		require.NoError(t, err, "PutBucketWebsite should succeed")
	})

	t.Run("GetBucketWebsite", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.Client.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err, "GetBucketWebsite should succeed")

		require.NotNil(t, resp.IndexDocument, "IndexDocument should be set")
		assert.Equal(t, "index.html", aws.ToString(resp.IndexDocument.Suffix))

		require.NotNil(t, resp.ErrorDocument, "ErrorDocument should be set")
		assert.Equal(t, "error.html", aws.ToString(resp.ErrorDocument.Key))
	})

	t.Run("DeleteBucketWebsite", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Client.DeleteBucketWebsite(ctx, &s3.DeleteBucketWebsiteInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err, "DeleteBucketWebsite should succeed")

		// Verify website config is gone
		_, err = client.Client.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{
			Bucket: aws.String(bucket),
		})
		assert.Error(t, err, "GetBucketWebsite should fail after deletion")
	})
}

// TestWebsiteHosting tests actual website hosting functionality.
// Requires WEBSITE_ENDPOINT env var to be set (e.g., "http://localhost:8082").
// The metadata server must be configured with website_domains that matches the Host header.
func TestWebsiteHosting(t *testing.T) {
	websiteEndpoint := os.Getenv("WEBSITE_ENDPOINT")
	if websiteEndpoint == "" {
		t.Skip("WEBSITE_ENDPOINT not set, skipping website hosting tests")
	}

	// Parse the website domain from the endpoint
	// e.g., "http://localhost:8082" -> we'll send Host header like "mybucket.s3-website.localhost"
	websiteDomain := os.Getenv("WEBSITE_DOMAIN")
	if websiteDomain == "" {
		websiteDomain = "s3-website.localhost" // default
	}

	client := newS3Client(t)
	bucket := uniqueBucket("website-test")

	// Setup: create bucket and configure website
	client.CreateBucket(bucket)
	defer func() {
		// Cleanup objects
		client.DeleteObject(bucket, "index.html")
		client.DeleteObject(bucket, "error.html")
		client.DeleteObject(bucket, "subdir/index.html")
		client.DeleteBucket(bucket)
	}()

	// Configure website
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.Client.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{
		Bucket: aws.String(bucket),
		WebsiteConfiguration: &s3types.WebsiteConfiguration{
			IndexDocument: &s3types.IndexDocument{
				Suffix: aws.String("index.html"),
			},
			ErrorDocument: &s3types.ErrorDocument{
				Key: aws.String("error.html"),
			},
		},
	})
	require.NoError(t, err)

	// Upload test content
	indexContent := []byte("<!DOCTYPE html><html><body>Welcome!</body></html>")
	errorContent := []byte("<!DOCTYPE html><html><body>Not Found</body></html>")
	subdirContent := []byte("<!DOCTYPE html><html><body>Subdir Index</body></html>")

	client.PutObject(bucket, "index.html", indexContent)
	client.PutObject(bucket, "error.html", errorContent)
	client.PutObject(bucket, "subdir/index.html", subdirContent)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	websiteHost := bucket + "." + websiteDomain

	t.Run("GET root returns index document", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, websiteEndpoint+"/", nil)
		require.NoError(t, err)
		req.Host = websiteHost

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, string(indexContent), string(body))
	})

	t.Run("GET specific file", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, websiteEndpoint+"/index.html", nil)
		require.NoError(t, err)
		req.Host = websiteHost

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, string(indexContent), string(body))
	})

	t.Run("GET subdir with trailing slash returns index", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, websiteEndpoint+"/subdir/", nil)
		require.NoError(t, err)
		req.Host = websiteHost

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, string(subdirContent), string(body))
	})

	t.Run("GET nonexistent returns error document with 404", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, websiteEndpoint+"/nonexistent.html", nil)
		require.NoError(t, err)
		req.Host = websiteHost

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusNotFound, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, string(errorContent), string(body))
	})

	t.Run("HEAD returns headers without body", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodHead, websiteEndpoint+"/index.html", nil)
		require.NoError(t, err)
		req.Host = websiteHost

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.NotEmpty(t, resp.Header.Get("Content-Length"))
		assert.NotEmpty(t, resp.Header.Get("ETag"))

		// HEAD should have no body
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Empty(t, body)
	})

	t.Run("PUT is rejected on website endpoint", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPut, websiteEndpoint+"/newfile.txt", strings.NewReader("test"))
		require.NoError(t, err)
		req.Host = websiteHost

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should return an error (501 Not Implemented for Unknown action)
		assert.True(t, resp.StatusCode >= 400, "PUT should be rejected on website endpoint")
	})

	t.Run("DELETE is rejected on website endpoint", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodDelete, websiteEndpoint+"/index.html", nil)
		require.NoError(t, err)
		req.Host = websiteHost

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should return an error (501 Not Implemented for Unknown action)
		assert.True(t, resp.StatusCode >= 400, "DELETE should be rejected on website endpoint")
	})

	t.Run("POST is rejected on website endpoint", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, websiteEndpoint+"/upload", strings.NewReader("test"))
		require.NoError(t, err)
		req.Host = websiteHost

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should return an error (501 Not Implemented for Unknown action)
		assert.True(t, resp.StatusCode >= 400, "POST should be rejected on website endpoint")
	})
}

// TestWebsiteHostingHTMLErrors tests that website requests return HTML error pages.
func TestWebsiteHostingHTMLErrors(t *testing.T) {
	websiteEndpoint := os.Getenv("WEBSITE_ENDPOINT")
	if websiteEndpoint == "" {
		t.Skip("WEBSITE_ENDPOINT not set, skipping website hosting tests")
	}

	websiteDomain := os.Getenv("WEBSITE_DOMAIN")
	if websiteDomain == "" {
		websiteDomain = "s3-website.localhost"
	}

	client := newS3Client(t)
	bucket := uniqueBucket("website-html-err")

	// Create bucket but DON'T configure website
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	websiteHost := bucket + "." + websiteDomain

	t.Run("GET without website config returns HTML error", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, websiteEndpoint+"/", nil)
		require.NoError(t, err)
		req.Host = websiteHost

		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should return error for no website configuration
		assert.True(t, resp.StatusCode >= 400)

		// Content-Type should be text/html for website requests
		contentType := resp.Header.Get("Content-Type")
		assert.True(t, strings.HasPrefix(contentType, "text/html"), "website errors should return HTML, got: %s", contentType)
	})
}
