//go:build integration

// Package testutil provides shared utilities for integration tests.
package testutil

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DefaultTimeout is the default timeout for test operations
const DefaultTimeout = 30 * time.Second

// ShortTimeout is a shorter timeout for simple operations
const ShortTimeout = 5 * time.Second

// GetEnv returns the environment variable value or a default
func GetEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// GenerateTestData creates random test data of the specified size
func GenerateTestData(t *testing.T, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err, "failed to generate random data")
	return data
}

// ComputeETag computes the MD5 hash (ETag) of data
func ComputeETag(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// NewGRPCConn creates a gRPC client connection with automatic cleanup
func NewGRPCConn(t *testing.T, addr string, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()

	defaultOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	allOpts := append(defaultOpts, opts...)

	conn, err := grpc.NewClient(addr, allOpts...)
	require.NoError(t, err, "failed to create gRPC connection to %s", addr)

	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}

// WithTimeout creates a context with the default timeout
func WithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, DefaultTimeout)
}

// WithShortTimeout creates a context with a short timeout
func WithShortTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, ShortTimeout)
}

// UniqueID generates a unique ID for test objects using timestamp
func UniqueID(prefix string) string {
	return prefix + "-" + time.Now().Format("20060102-150405.000000000")
}

// SkipIfShort skips the test if running in short mode
func SkipIfShort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
}
