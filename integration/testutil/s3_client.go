//go:build integration

package testutil

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// S3Client wraps the AWS S3 client with test helpers
type S3Client struct {
	*s3.Client
	t        *testing.T
	endpoint string
}

// S3Config holds configuration for S3 client
type S3Config struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	UsePathStyle    bool
}

// DefaultS3Config returns default S3 configuration for local testing.
func DefaultS3Config() S3Config {
	return S3Config{
		Endpoint:        GetEnv("S3_ENDPOINT", "http://localhost:8082"),
		Region:          GetEnv("S3_REGION", "us-east-1"),
		AccessKeyID:     GetEnv("S3_ACCESS_KEY_ID", "AKIAADMINKEY00001"),
		SecretAccessKey: GetEnv("S3_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE1"),
		UsePathStyle:    true, // Always use path-style for local testing
	}
}

// NewS3Client creates an S3 client for testing
func NewS3Client(t *testing.T, cfg S3Config) *S3Client {
	t.Helper()

	ctx := context.Background()

	// Create custom resolver for local endpoint
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               cfg.Endpoint,
			HostnameImmutable: true,
			SigningRegion:     cfg.Region,
		}, nil
	})

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	require.NoError(t, err, "failed to create AWS config")

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.UsePathStyle
	})

	return &S3Client{
		Client:   client,
		t:        t,
		endpoint: cfg.Endpoint,
	}
}

// CreateBucket creates a bucket
func (c *S3Client) CreateBucket(bucket string) {
	c.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	_, err := c.Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(c.t, err, "failed to create bucket %s", bucket)
}

// DeleteBucket deletes a bucket
func (c *S3Client) DeleteBucket(bucket string) {
	c.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	_, err := c.Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(c.t, err, "failed to delete bucket %s", bucket)
}

// ListBuckets lists all buckets
func (c *S3Client) ListBuckets() *s3.ListBucketsOutput {
	c.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := c.Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(c.t, err, "failed to list buckets")
	return resp
}

// PutObject uploads an object
func (c *S3Client) PutObject(bucket, key string, data []byte) *s3.PutObjectOutput {
	return c.PutObjectWithOptions(bucket, key, data)
}

// PutObjectWithOptions uploads an object with additional options
func (c *S3Client) PutObjectWithOptions(bucket, key string, data []byte, opts ...PutObjectOption) *s3.PutObjectOutput {
	c.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(input)
		}
	}

	resp, err := c.Client.PutObject(ctx, input)
	require.NoError(c.t, err, "failed to put object %s/%s", bucket, key)
	return resp
}

// PutObjectOption modifies PutObjectInput
type PutObjectOption func(*s3.PutObjectInput)

// WithSSECustomerKey sets SSE-C encryption headers
func WithSSECustomerKey(key []byte) PutObjectOption {
	return func(i *s3.PutObjectInput) {
		// Base64 encode the key
		keyBase64 := base64.StdEncoding.EncodeToString(key)
		// Compute MD5 of the key
		keyMD5Hash := md5.Sum(key)
		keyMD5 := base64.StdEncoding.EncodeToString(keyMD5Hash[:])

		i.SSECustomerAlgorithm = aws.String("AES256")
		i.SSECustomerKey = aws.String(keyBase64)
		i.SSECustomerKeyMD5 = aws.String(keyMD5)
	}
}

// GetObject retrieves an object
func (c *S3Client) GetObject(bucket, key string) []byte {
	return c.GetObjectWithOptions(bucket, key)
}

// GetObjectWithOptions retrieves an object with additional options
func (c *S3Client) GetObjectWithOptions(bucket, key string, opts ...GetObjectOption) []byte {
	c.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(input)
		}
	}

	resp, err := c.Client.GetObject(ctx, input)
	require.NoError(c.t, err, "failed to get object %s/%s", bucket, key)
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err, "failed to read object body")
	return data
}

// GetObjectOption modifies GetObjectInput
type GetObjectOption func(*s3.GetObjectInput)

// WithSSECustomerKeyForGet sets SSE-C decryption headers for GetObject
func WithSSECustomerKeyForGet(key []byte) GetObjectOption {
	return func(i *s3.GetObjectInput) {
		keyBase64 := base64.StdEncoding.EncodeToString(key)
		keyMD5Hash := md5.Sum(key)
		keyMD5 := base64.StdEncoding.EncodeToString(keyMD5Hash[:])

		i.SSECustomerAlgorithm = aws.String("AES256")
		i.SSECustomerKey = aws.String(keyBase64)
		i.SSECustomerKeyMD5 = aws.String(keyMD5)
	}
}

// DeleteObject deletes an object
func (c *S3Client) DeleteObject(bucket, key string) {
	c.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	_, err := c.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(c.t, err, "failed to delete object %s/%s", bucket, key)
}

// HeadObject checks if an object exists and returns metadata
func (c *S3Client) HeadObject(bucket, key string) *s3.HeadObjectOutput {
	return c.HeadObjectWithOptions(bucket, key)
}

// HeadObjectWithOptions checks if an object exists with additional options
func (c *S3Client) HeadObjectWithOptions(bucket, key string, opts ...HeadObjectOption) *s3.HeadObjectOutput {
	c.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(input)
		}
	}

	resp, err := c.Client.HeadObject(ctx, input)
	require.NoError(c.t, err, "failed to head object %s/%s", bucket, key)
	return resp
}

// HeadObjectOption modifies HeadObjectInput
type HeadObjectOption func(*s3.HeadObjectInput)

// WithSSECustomerKeyForHead sets SSE-C decryption headers for HeadObject
func WithSSECustomerKeyForHead(key []byte) HeadObjectOption {
	return func(i *s3.HeadObjectInput) {
		keyBase64 := base64.StdEncoding.EncodeToString(key)
		keyMD5Hash := md5.Sum(key)
		keyMD5 := base64.StdEncoding.EncodeToString(keyMD5Hash[:])

		i.SSECustomerAlgorithm = aws.String("AES256")
		i.SSECustomerKey = aws.String(keyBase64)
		i.SSECustomerKeyMD5 = aws.String(keyMD5)
	}
}

// ListObjects lists objects in a bucket
func (c *S3Client) ListObjects(bucket string, opts ...ListObjectsS3Option) *s3.ListObjectsV2Output {
	c.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	for _, opt := range opts {
		opt(input)
	}

	resp, err := c.Client.ListObjectsV2(ctx, input)
	require.NoError(c.t, err, "failed to list objects in %s", bucket)
	return resp
}

// ListObjectsS3Option modifies ListObjectsV2Input
type ListObjectsS3Option func(*s3.ListObjectsV2Input)

// WithS3Prefix sets the prefix filter
func WithS3Prefix(prefix string) ListObjectsS3Option {
	return func(i *s3.ListObjectsV2Input) {
		i.Prefix = aws.String(prefix)
	}
}

// WithS3MaxKeys sets the max keys
func WithS3MaxKeys(max int32) ListObjectsS3Option {
	return func(i *s3.ListObjectsV2Input) {
		i.MaxKeys = aws.Int32(max)
	}
}

// WithS3Delimiter sets the delimiter for hierarchical listing
func WithS3Delimiter(delimiter string) ListObjectsS3Option {
	return func(i *s3.ListObjectsV2Input) {
		i.Delimiter = aws.String(delimiter)
	}
}
