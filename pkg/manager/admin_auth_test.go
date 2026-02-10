// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestAdminAuthInterceptor_ValidToken(t *testing.T) {
	interceptor := NewAdminAuthInterceptor("secret-token", []string{"/manager.ManagerService/RaftAddServer"})

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer secret-token"))

	result, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/manager.ManagerService/RaftAddServer",
	}, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "ok", result)
}

func TestAdminAuthInterceptor_MissingToken(t *testing.T) {
	interceptor := NewAdminAuthInterceptor("secret-token", []string{"/manager.ManagerService/RaftAddServer"})

	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/manager.ManagerService/RaftAddServer",
	}, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing credentials")
}

func TestAdminAuthInterceptor_InvalidToken(t *testing.T) {
	interceptor := NewAdminAuthInterceptor("secret-token", []string{"/manager.ManagerService/RaftAddServer"})

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer wrong-token"))

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/manager.ManagerService/RaftAddServer",
	}, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid admin token")
}

func TestAdminAuthInterceptor_NonProtectedMethod(t *testing.T) {
	interceptor := NewAdminAuthInterceptor("secret-token", []string{"/manager.ManagerService/RaftAddServer"})

	// Heartbeat is NOT protected - should pass without token
	result, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/manager.ManagerService/Heartbeat",
	}, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "ok", result)
}

func TestAdminAuthInterceptor_EmptyToken(t *testing.T) {
	// Empty token = auth disabled
	interceptor := NewAdminAuthInterceptor("", []string{"/manager.ManagerService/RaftAddServer"})

	result, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/manager.ManagerService/RaftAddServer",
	}, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "ok", result)
}

func TestAdminAuthInterceptor_InvalidFormat(t *testing.T) {
	interceptor := NewAdminAuthInterceptor("secret-token", []string{"/manager.ManagerService/RaftAddServer"})

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Basic secret-token"))

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/manager.ManagerService/RaftAddServer",
	}, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid authorization format")
}

func TestAdminAuthInterceptor_MetadataNoAuth(t *testing.T) {
	interceptor := NewAdminAuthInterceptor("secret-token", []string{"/manager.ManagerService/RaftAddServer"})

	// Context has metadata but no authorization header
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("other-header", "value"))

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/manager.ManagerService/RaftAddServer",
	}, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing authorization header")
}

func TestAdminProtectedMethods(t *testing.T) {
	methods := AdminProtectedMethods()
	assert.Len(t, methods, 4)
	assert.Contains(t, methods, "/manager.ManagerService/RaftAddServer")
	assert.Contains(t, methods, "/manager.ManagerService/RaftRemoveServer")
	assert.Contains(t, methods, "/manager.ManagerService/RaftListClusterServers")
	assert.Contains(t, methods, "/manager.ManagerService/RecoverCollections")

	// Ensure it returns a copy (modifying the returned slice doesn't affect the original)
	methods[0] = "modified"
	original := AdminProtectedMethods()
	assert.Equal(t, "/manager.ManagerService/RaftAddServer", original[0])
}
