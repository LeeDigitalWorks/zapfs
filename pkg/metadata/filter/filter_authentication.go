package filter

import (
	"sync/atomic"

	"zapfs/pkg/iam"
	"zapfs/pkg/metadata/data"
	"zapfs/pkg/s3api/s3err"
	"zapfs/pkg/s3api/signature"

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
		// TODO: Implement streaming signature verification:
		// 1. Verify initial request signature (same as V4)
		// 2. Create a ChunkVerifier that wraps the request body
		// 3. For each chunk, verify: chunk-signature = HMAC-SHA256(signing-key, string-to-sign)
		//    where string-to-sign = "AWS4-HMAC-SHA256-PAYLOAD" + "\n" +
		//                           date + "\n" + credential-scope + "\n" +
		//                           previous-signature + "\n" +
		//                           hash("") + "\n" + hash(chunk-data)
		// 4. Store ChunkVerifier in data.Data for handler to use
		//
		// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
		identity, errCode := f.v4Verifier.VerifyRequest(d.Req)
		if errCode != s3err.ErrNone {
			f.recordResult(f.errorToResult(errCode), authTypeStr)
			return nil, errCode
		}
		d.Identity = identity
		f.recordResult(authResultSuccess, authTypeStr)
		// TODO: d.ChunkVerifier = signature.NewChunkVerifier(...)

	case signature.AuthTypeStreamingSignedTrailer:
		// AWS Signature V4 with chunked encoding AND trailing checksums
		//
		// TODO: Implement streaming with trailer verification:
		// 1. Same as StreamingSigned above
		// 2. Additionally verify trailing headers (x-amz-checksum-*)
		// 3. Trailer signature covers: previous-signature + trailing-headers
		//
		// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
		identity, errCode := f.v4Verifier.VerifyRequest(d.Req)
		if errCode != s3err.ErrNone {
			f.recordResult(f.errorToResult(errCode), authTypeStr)
			return nil, errCode
		}
		d.Identity = identity
		f.recordResult(authResultSuccess, authTypeStr)

	case signature.AuthTypeV2, signature.AuthTypePresignedV2:
		// AWS Signature V2 (deprecated but some old clients still use it)
		//
		// TODO: Implement V2 signature verification if needed:
		// 1. Parse Authorization header: "AWS AWSAccessKeyId:Signature"
		// 2. Build StringToSign = HTTP-Verb + "\n" + Content-MD5 + "\n" +
		//                         Content-Type + "\n" + Date + "\n" +
		//                         CanonicalizedAmzHeaders + CanonicalizedResource
		// 3. Calculate: Base64(HMAC-SHA1(SecretKey, StringToSign))
		// 4. Compare with provided signature
		//
		// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html
		f.recordResult(authResultUnsupportedAuth, authTypeStr)
		return nil, s3err.ErrSignatureVersionNotSupported

	case signature.AuthTypePostPolicy:
		// Browser-based uploads using POST with policy document
		//
		// TODO: Implement POST policy verification:
		// 1. Parse multipart form to extract: policy, x-amz-signature, x-amz-credential
		// 2. Base64-decode and parse JSON policy document
		// 3. Verify policy conditions (bucket, key prefix, content-type, size limits, etc.)
		// 4. Verify expiration timestamp
		// 5. Calculate signature over base64-encoded policy
		// 6. Compare with provided x-amz-signature
		//
		// Note: This is typically handled in the POST object handler itself
		// since we need to parse the multipart form first.
		//
		// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-post-example.html
		f.recordResult(authResultSuccess, authTypeStr)
		return Next{}, nil

	case signature.AuthTypeStreamingUnsignedTrailer:
		// Unsigned payload with trailing checksums (x-amz-content-sha256: UNSIGNED-PAYLOAD)
		//
		// TODO: Decide policy for unsigned payloads:
		// - Some operations allow UNSIGNED-PAYLOAD (e.g., when using HTTPS)
		// - Still need to verify trailing checksum if present
		// - May want to allow based on bucket policy or server config
		//
		// For now, reject unsigned payloads for security
		f.recordResult(authResultAccessDenied, authTypeStr)
		return nil, s3err.ErrAccessDenied
	}

	// Populate S3Info.OwnerID from authenticated identity
	if d.Identity != nil && d.Identity.Account != nil {
		d.S3Info.OwnerID = d.Identity.Account.ID
	}

	return Next{}, nil
}
