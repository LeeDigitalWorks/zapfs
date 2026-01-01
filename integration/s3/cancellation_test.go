//go:build integration

package s3

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Cancellation Tests
// =============================================================================
// These tests verify that the server handles client cancellations gracefully,
// including mid-flight PUT/GET operations and multipart uploads.

// slowReader wraps an io.Reader and introduces delays to simulate slow uploads
type slowReader struct {
	r            io.Reader
	bytesRead    int64
	delay        time.Duration
	cancelAt     int64       // Cancel context after this many bytes
	cancelFunc   func()      // Context cancel function
	cancelled    atomic.Bool // Track if we cancelled
	bytesPerRead int         // Bytes to read per Read call
}

func (s *slowReader) Read(p []byte) (n int, err error) {
	// Check if we should cancel
	if s.cancelAt > 0 && atomic.LoadInt64(&s.bytesRead) >= s.cancelAt && !s.cancelled.Load() {
		s.cancelled.Store(true)
		if s.cancelFunc != nil {
			s.cancelFunc()
		}
		return 0, context.Canceled
	}

	// Add delay to simulate slow upload
	if s.delay > 0 {
		time.Sleep(s.delay)
	}

	// Limit read size to simulate chunked uploads
	readSize := len(p)
	if s.bytesPerRead > 0 && readSize > s.bytesPerRead {
		readSize = s.bytesPerRead
	}

	n, err = s.r.Read(p[:readSize])
	atomic.AddInt64(&s.bytesRead, int64(n))
	return n, err
}

func TestCancellation(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)

	t.Run("cancel PUT upload mid-flight", func(t *testing.T) {
		t.Parallel()

		bucket := uniqueBucket("test-cancel-put")
		key := uniqueKey("cancelled-upload")

		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		// Create a 5MB upload that we'll cancel after 1MB
		dataSize := 5 * 1024 * 1024
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create slow reader that cancels after 1MB
		slowR := &slowReader{
			r:            bytes.NewReader(data),
			delay:        1 * time.Millisecond,
			cancelAt:     1 * 1024 * 1024, // Cancel after 1MB
			cancelFunc:   cancel,
			bytesPerRead: 64 * 1024, // 64KB chunks
		}

		// Attempt upload - should fail due to cancellation
		_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			Body:          slowR,
			ContentLength: aws.Int64(int64(dataSize)),
		})

		// Should get a context cancelled error
		assert.Error(t, err, "PUT should fail when cancelled")
		t.Logf("PUT cancelled as expected after %d bytes: %v", atomic.LoadInt64(&slowR.bytesRead), err)

		// Verify the object was NOT created (partial upload should not persist)
		// Use a fresh context for verification
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer verifyCancel()

		_, err = client.Client.HeadObject(verifyCtx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		assert.Error(t, err, "Object should not exist after cancelled upload")
		t.Logf("Verified object does not exist after cancelled upload")
	})

	t.Run("cancel GET download mid-flight", func(t *testing.T) {
		t.Parallel()

		bucket := uniqueBucket("test-cancel-get")
		key := uniqueKey("download-object")

		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		// Upload a 5MB object first
		dataSize := 5 * 1024 * 1024
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		uploadCtx, uploadCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer uploadCancel()

		_, err := client.Client.PutObject(uploadCtx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err, "Upload should succeed")

		// Now download with a context that we'll cancel mid-flight
		downloadCtx, downloadCancel := context.WithTimeout(context.Background(), 30*time.Second)

		getResp, err := client.Client.GetObject(downloadCtx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "GetObject should start")
		defer getResp.Body.Close()

		// Read 1MB then cancel
		partialData := make([]byte, 1*1024*1024)
		n, err := io.ReadFull(getResp.Body, partialData)
		require.NoError(t, err, "Should read first 1MB")
		assert.Equal(t, 1*1024*1024, n, "Should have read 1MB")

		// Cancel the context
		downloadCancel()

		// Try to read more - should fail or return less data
		moreData := make([]byte, 1*1024*1024)
		_, err = io.ReadFull(getResp.Body, moreData)
		// Either context.Canceled or we might get some buffered data
		// The important thing is that the server handles this gracefully
		t.Logf("Read after cancel: err=%v", err)

		// Verify the object still exists and is intact
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer verifyCancel()

		getResp2, err := client.Client.GetObject(verifyCtx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "Object should still be accessible")
		defer getResp2.Body.Close()

		retrieved, err := io.ReadAll(getResp2.Body)
		require.NoError(t, err, "Should read full object")
		assert.Equal(t, data, retrieved, "Object data should be intact")
		t.Logf("Verified object is intact after cancelled download")

		// Cleanup
		client.DeleteObject(bucket, key)
	})

	t.Run("cancel multipart part upload mid-flight", func(t *testing.T) {
		t.Parallel()

		bucket := uniqueBucket("test-cancel-mpu")
		key := uniqueKey("cancelled-mpu")

		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		// Create multipart upload
		createCtx, createCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer createCancel()

		createResp, err := client.Client.CreateMultipartUpload(createCtx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		uploadID := *createResp.UploadId
		t.Logf("Created multipart upload: %s", uploadID)

		// Upload part 1 successfully
		part1Data := make([]byte, 5*1024*1024)
		for i := range part1Data {
			part1Data[i] = byte(i % 256)
		}

		part1Ctx, part1Cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer part1Cancel()

		part1Resp, err := client.Client.UploadPart(part1Ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(part1Data),
		})
		require.NoError(t, err, "Part 1 should upload successfully")
		t.Logf("Part 1 uploaded: ETag=%s", *part1Resp.ETag)

		// Attempt to upload part 2 but cancel it
		part2Data := make([]byte, 5*1024*1024)
		for i := range part2Data {
			part2Data[i] = byte((i + 100) % 256)
		}

		part2Ctx, part2Cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer part2Cancel()

		slowR := &slowReader{
			r:            bytes.NewReader(part2Data),
			delay:        1 * time.Millisecond,
			cancelAt:     1 * 1024 * 1024,
			cancelFunc:   part2Cancel,
			bytesPerRead: 64 * 1024,
		}

		_, err = client.Client.UploadPart(part2Ctx, &s3.UploadPartInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(2),
			Body:          slowR,
			ContentLength: aws.Int64(int64(len(part2Data))),
		})
		assert.Error(t, err, "Part 2 upload should fail when cancelled")
		t.Logf("Part 2 cancelled as expected: %v", err)

		// List parts - only part 1 should exist
		listCtx, listCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer listCancel()

		listResp, err := client.Client.ListParts(listCtx, &s3.ListPartsInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
		require.NoError(t, err)
		assert.Len(t, listResp.Parts, 1, "Only part 1 should be listed")
		if len(listResp.Parts) > 0 {
			assert.Equal(t, int32(1), *listResp.Parts[0].PartNumber)
		}

		// Abort the upload to clean up
		abortCtx, abortCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer abortCancel()

		_, err = client.Client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
		require.NoError(t, err, "Abort should succeed")
		t.Logf("Multipart upload aborted successfully")
	})

	t.Run("rapid cancel during upload start", func(t *testing.T) {
		t.Parallel()

		bucket := uniqueBucket("test-rapid-cancel")
		key := uniqueKey("rapid-cancel")

		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		// Create upload data
		data := make([]byte, 1*1024*1024)
		for i := range data {
			data[i] = byte(i % 256)
		}

		// Cancel immediately
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel before even starting

		_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		assert.Error(t, err, "Should fail with already cancelled context")
		t.Logf("Immediate cancel handled: %v", err)

		// Verify object doesn't exist
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer verifyCancel()

		_, err = client.Client.HeadObject(verifyCtx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		assert.Error(t, err, "Object should not exist")
	})

	t.Run("timeout during large upload", func(t *testing.T) {
		t.Parallel()

		bucket := uniqueBucket("test-timeout-upload")
		key := uniqueKey("timeout-object")

		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		// Create a 10MB upload with a very short timeout
		dataSize := 10 * 1024 * 1024
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		// Very slow reader with short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		slowR := &slowReader{
			r:            bytes.NewReader(data),
			delay:        10 * time.Millisecond,
			bytesPerRead: 32 * 1024,
		}

		_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			Body:          slowR,
			ContentLength: aws.Int64(int64(dataSize)),
		})
		assert.Error(t, err, "Should timeout during slow upload")
		t.Logf("Timeout handled after %d bytes: %v", atomic.LoadInt64(&slowR.bytesRead), err)

		// Verify object doesn't exist
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer verifyCancel()

		_, err = client.Client.HeadObject(verifyCtx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		assert.Error(t, err, "Object should not exist after timeout")
	})
}

// TestConnectionDropMidFlight tests server handling when client drops connection
// Uses SDK with custom transport to simulate connection drops
func TestConnectionDropMidFlight(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)

	t.Run("connection drop during large download", func(t *testing.T) {
		t.Parallel()

		bucket := uniqueBucket("test-drop-download")
		key := uniqueKey("drop-download")

		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		// Upload a 10MB object
		dataSize := 10 * 1024 * 1024
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		uploadCtx, uploadCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer uploadCancel()

		_, err := client.Client.PutObject(uploadCtx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		// Download and drop connection mid-flight by closing the body early
		downloadCtx, downloadCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer downloadCancel()

		getResp, err := client.Client.GetObject(downloadCtx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		// Read only 1MB then close the body (simulating connection drop)
		partialData := make([]byte, 1*1024*1024)
		n, err := io.ReadFull(getResp.Body, partialData)
		require.NoError(t, err)
		assert.Equal(t, 1*1024*1024, n)

		// Close body early - this simulates client disconnect
		getResp.Body.Close()
		t.Logf("Closed response body after reading %d bytes (simulating connection drop)", n)

		// Give server time to notice the disconnect and clean up
		time.Sleep(100 * time.Millisecond)

		// Verify object still exists and is fully intact
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer verifyCancel()

		getResp2, err := client.Client.GetObject(verifyCtx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getResp2.Body.Close()

		retrieved, err := io.ReadAll(getResp2.Body)
		require.NoError(t, err)
		assert.Equal(t, data, retrieved, "Object should be fully intact after partial read")
		t.Logf("Verified object is intact after connection drop during download")

		// Cleanup
		client.DeleteObject(bucket, key)
	})

	t.Run("multiple rapid connection drops", func(t *testing.T) {
		t.Parallel()

		bucket := uniqueBucket("test-rapid-drops")
		key := uniqueKey("rapid-drops")

		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		// Upload a 5MB object
		dataSize := 5 * 1024 * 1024
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		uploadCtx, uploadCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer uploadCancel()

		_, err := client.Client.PutObject(uploadCtx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		// Perform 5 rapid partial downloads, each closing early
		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			getResp, err := client.Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.NoError(t, err)

			// Read a small amount and close
			smallBuf := make([]byte, 64*1024)
			_, _ = getResp.Body.Read(smallBuf)
			getResp.Body.Close()
			cancel()
		}
		t.Logf("Completed 5 rapid partial downloads with early closes")

		// Verify object is still intact
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer verifyCancel()

		getResp, err := client.Client.GetObject(verifyCtx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		retrieved, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		assert.Equal(t, data, retrieved, "Object should be intact after rapid connection drops")
		t.Logf("Verified object is intact after multiple rapid connection drops")

		// Cleanup
		client.DeleteObject(bucket, key)
	})
}
