// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"crypto/subtle"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// adminProtectedMethods lists gRPC methods that require admin auth.
var adminProtectedMethods = []string{
	"/manager.ManagerService/RaftAddServer",
	"/manager.ManagerService/RaftRemoveServer",
	"/manager.ManagerService/RaftListClusterServers",
	"/manager.ManagerService/RecoverCollections",
}

// AdminProtectedMethods returns a copy of the protected methods list.
func AdminProtectedMethods() []string {
	return append([]string{}, adminProtectedMethods...)
}

// NewAdminAuthInterceptor creates a gRPC unary interceptor that requires
// a bearer token for admin-level RPCs. Non-admin RPCs pass through.
// If token is empty, all requests pass through (auth disabled).
func NewAdminAuthInterceptor(token string, protectedMethods []string) grpc.UnaryServerInterceptor {
	protected := make(map[string]bool, len(protectedMethods))
	for _, m := range protectedMethods {
		protected[m] = true
	}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token == "" {
			return handler(ctx, req)
		}

		if !protected[info.FullMethod] {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			logger.Warn().Str("method", info.FullMethod).Msg("Admin RPC rejected: no metadata")
			return nil, status.Error(codes.Unauthenticated, "missing credentials")
		}

		authValues := md.Get("authorization")
		if len(authValues) == 0 {
			logger.Warn().Str("method", info.FullMethod).Msg("Admin RPC rejected: no authorization header")
			return nil, status.Error(codes.Unauthenticated, "missing authorization header")
		}

		authHeader := authValues[0]
		if !strings.HasPrefix(authHeader, "Bearer ") {
			return nil, status.Error(codes.Unauthenticated, "invalid authorization format")
		}

		provided := strings.TrimPrefix(authHeader, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			logger.Warn().Str("method", info.FullMethod).Msg("Admin RPC rejected: invalid token")
			return nil, status.Error(codes.Unauthenticated, "invalid admin token")
		}

		return handler(ctx, req)
	}
}
