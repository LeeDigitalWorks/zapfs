// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"sync/atomic"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/signature"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	FilterTypeAuthentication = "AuthenticationFilter"

	// Metric label values for auth results
	authResultSuccess           = "success"
	authResultAccessDenied      = "access_denied"
	authResultInvalidAccessKey  = "invalid_access_key"
	authResultSignatureMismatch = "signature_mismatch"
	authResultUnsupportedAuth   = "unsupported_auth"
	authResultAnonymousDenied   = "anonymous_denied"
)

type AuthenticationFilter struct {
	v4Verifier *signature.V4Verifier
	v2Verifier *signature.V2Verifier
	iamManager *iam.Manager
	allowAnon  bool // Allow anonymous access (for public buckets)

	// Metrics
	authAttempts atomic.Uint64
	authErrors   atomic.Uint64

	metricAuthTotal   *prometheus.CounterVec
	metricAuthLatency prometheus.Histogram
}

type AuthFilterOption func(*AuthenticationFilter)

func WithAllowAnonymous(allow bool) AuthFilterOption {
	return func(f *AuthenticationFilter) {
		f.allowAnon = allow
	}
}

func NewAuthenticationFilter(iamManager *iam.Manager, opts ...AuthFilterOption) *AuthenticationFilter {
	f := &AuthenticationFilter{
		v4Verifier: signature.NewV4Verifier(iamManager),
		v2Verifier: signature.NewV2Verifier(iamManager),
		iamManager: iamManager,
		allowAnon:  false,
		metricAuthTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "auth_filter_total",
			Help: "Total authentication attempts by result",
		}, []string{"result", "auth_type"}),
		metricAuthLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "auth_filter_latency_seconds",
			Help:    "Authentication verification latency in seconds",
			Buckets: prometheus.DefBuckets,
		}),
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// GetIAMManager returns the IAM manager for policy verification
func (f *AuthenticationFilter) GetIAMManager() *iam.Manager {
	return f.iamManager
}

// Metrics returns the Prometheus collectors for registration
func (f *AuthenticationFilter) Metrics() []prometheus.Collector {
	return []prometheus.Collector{f.metricAuthTotal, f.metricAuthLatency}
}

// Stats returns current auth statistics
func (f *AuthenticationFilter) Stats() (attempts, errors uint64) {
	return f.authAttempts.Load(), f.authErrors.Load()
}

func (f *AuthenticationFilter) recordResult(result, authType string) {
	f.authAttempts.Add(1)
	if result != authResultSuccess {
		f.authErrors.Add(1)
	}
	f.metricAuthTotal.WithLabelValues(result, authType).Inc()
}

func (f *AuthenticationFilter) errorToResult(errCode s3err.ErrorCode) string {
	switch errCode {
	case s3err.ErrAccessDenied:
		return authResultAccessDenied
	case s3err.ErrInvalidAccessKeyID:
		return authResultInvalidAccessKey
	case s3err.ErrSignatureDoesNotMatch:
		return authResultSignatureMismatch
	case s3err.ErrSignatureVersionNotSupported:
		return authResultUnsupportedAuth
	default:
		return authResultAccessDenied
	}
}

func (f *AuthenticationFilter) Type() string {
	return FilterTypeAuthentication
}

func (f *AuthenticationFilter) Run(d *data.Data) (Response, error) {
	if d.Ctx.Err() != nil {
		return nil, d.Ctx.Err()
	}

	// Get the auth type
	authType := signature.GetAuthType(d)
	authTypeStr := authType.String()

	// Reject unsupported auth types
	if authType == signature.AuthTypeNone {
		f.recordResult(authResultUnsupportedAuth, authTypeStr)
		return nil, s3err.ErrSignatureVersionNotSupported
	}

	// For anonymous requests, check if allowed
	if authType == signature.AuthTypeAnonymous {
		if f.allowAnon {
			f.recordResult(authResultSuccess, authTypeStr)
			return Next{}, nil
		}
		f.recordResult(authResultAnonymousDenied, authTypeStr)
		return nil, s3err.ErrAccessDenied
	}

	// Verify signature based on auth type
	switch authType {
	case signature.AuthTypeV4, signature.AuthTypePresignedV4:
		// Standard AWS Signature V4 - header-based or presigned URL
		identity, errCode := f.v4Verifier.VerifyRequest(d.Req)
		if errCode != s3err.ErrNone {
			f.recordResult(f.errorToResult(errCode), authTypeStr)
			return nil, errCode
		}
		d.Identity = identity
		f.recordResult(authResultSuccess, authTypeStr)

	case signature.AuthTypeStreamingSigned:
		// AWS Signature V4 with chunked transfer encoding (aws-chunked)
		// Each chunk has its own signature that must be verified.
		//
		// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
		streamResult, errCode := f.v4Verifier.VerifyStreamingRequest(d.Req)
		if errCode != s3err.ErrNone {
			f.recordResult(f.errorToResult(errCode), authTypeStr)
			return nil, errCode
		}
		d.Identity = streamResult.Identity

		// Create ChunkReader to verify each chunk's signature as data is read
		chunkReader := signature.NewVerifyingChunkReader(signature.ChunkReaderConfig{
			Body:          d.Req.Body,
			SigningKey:    streamResult.SigningKey,
			SeedSignature: streamResult.SeedSignature,
			Timestamp:     streamResult.Timestamp,
			Region:        streamResult.Region,
			Service:       streamResult.Service,
		})
		d.VerifiedBody = chunkReader
		f.recordResult(authResultSuccess, authTypeStr)

	case signature.AuthTypeStreamingSignedTrailer:
		// AWS Signature V4 with chunked encoding AND trailing checksums
		// Each chunk has its own signature, plus trailing headers (checksums)
		// with their own signature at the end.
		//
		// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
		streamResult, errCode := f.v4Verifier.VerifyStreamingRequest(d.Req)
		if errCode != s3err.ErrNone {
			f.recordResult(f.errorToResult(errCode), authTypeStr)
			return nil, errCode
		}
		d.Identity = streamResult.Identity

		// Create TrailerChunkReader to verify chunk signatures AND trailing checksums
		chunkReader := signature.NewTrailerChunkReader(signature.ChunkReaderConfig{
			Body:          d.Req.Body,
			SigningKey:    streamResult.SigningKey,
			SeedSignature: streamResult.SeedSignature,
			Timestamp:     streamResult.Timestamp,
			Region:        streamResult.Region,
			Service:       streamResult.Service,
		})
		d.VerifiedBody = chunkReader
		f.recordResult(authResultSuccess, authTypeStr)

	case signature.AuthTypeV2, signature.AuthTypePresignedV2:
		// AWS Signature V2 (deprecated but some old clients still use it)
		// Supported for compatibility with legacy SDKs and tools like s3cmd
		identity, errCode := f.v2Verifier.VerifyRequest(d.Req)
		if errCode != s3err.ErrNone {
			f.recordResult(f.errorToResult(errCode), authTypeStr)
			return nil, errCode
		}
		d.Identity = identity
		f.recordResult(authResultSuccess, authTypeStr)

	case signature.AuthTypePostPolicy:
		// Browser-based uploads using POST with policy document.
		// Policy verification is handled in PostObjectHandler since we need
		// to parse the multipart form first to extract the policy and signature.
		//
		// See: pkg/metadata/api/object.go - PostObjectHandler
		// See: pkg/s3api/signature/post_policy.go - PostPolicyVerifier
		f.recordResult(authResultSuccess, authTypeStr)
		return Next{}, nil

	case signature.AuthTypeStreamingUnsignedTrailer:
		// Unsigned payload (x-amz-content-sha256: UNSIGNED-PAYLOAD)
		//
		// AWS allows UNSIGNED-PAYLOAD when:
		// - Request headers are signed with V4 (provides authentication)
		// - Only the body is unsigned (useful for large uploads where SHA256 is expensive)
		// - TLS is used (provides transport-level integrity)
		//
		// We verify the request signature treating UNSIGNED-PAYLOAD as the content hash.
		// The signature covers all signed headers including x-amz-content-sha256.
		identity, errCode := f.v4Verifier.VerifyRequest(d.Req)
		if errCode != s3err.ErrNone {
			f.recordResult(f.errorToResult(errCode), authTypeStr)
			return nil, errCode
		}
		d.Identity = identity
		f.recordResult(authResultSuccess, authTypeStr)
	}

	// Populate S3Info.OwnerID from authenticated identity
	if d.Identity != nil && d.Identity.Account != nil {
		d.S3Info.OwnerID = d.Identity.Account.ID
	}

	return Next{}, nil
}
