package filter

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3action"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// AuthorizationFilter checks if the authenticated identity is allowed to perform
// the requested action on the target resource. It evaluates multiple policy sources
// in a specific order following AWS's authorization logic.
//
// Authorization evaluation order (first explicit deny wins, then explicit allow):
// 1. Bucket policies - attached to buckets, can grant cross-account access
// 2. IAM policies - attached to users/groups/roles
// 3. ACLs (Access Control Lists) - legacy, per-object/bucket permissions
//
// For anonymous requests (no identity), only bucket policies and ACLs are checked.
type AuthorizationFilter struct {
	policyStore     PolicyStore
	aclStore        ACLStore
	iamEvaluator    *iam.PolicyEvaluator
	bucketOwnerFunc BucketOwnerFunc // Optional: function to check bucket ownership
}

// PolicyStore retrieves bucket policies
type PolicyStore interface {
	// GetBucketPolicy returns the bucket policy, or nil if none exists
	GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, bool)
}

// ACLStore retrieves ACLs for buckets and objects
type ACLStore interface {
	// GetBucketACL returns the ACL for a bucket
	GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, bool)
	// GetObjectACL returns the ACL for an object
	GetObjectACL(ctx context.Context, bucket, key string) (*s3types.AccessControlList, bool)
}

// BucketOwnerFunc checks if an identity owns a bucket
type BucketOwnerFunc func(ctx context.Context, bucket string, identity *iam.Identity) bool

// AuthorizationConfig holds configuration for the authorization filter
type AuthorizationConfig struct {
	PolicyStore     PolicyStore
	ACLStore        ACLStore
	IAMEvaluator    *iam.PolicyEvaluator
	BucketOwnerFunc BucketOwnerFunc
}

func NewAuthorizationFilter(config AuthorizationConfig) *AuthorizationFilter {
	return &AuthorizationFilter{
		policyStore:     config.PolicyStore,
		aclStore:        config.ACLStore,
		iamEvaluator:    config.IAMEvaluator,
		bucketOwnerFunc: config.BucketOwnerFunc,
	}
}

func (f *AuthorizationFilter) Type() string {
	return "authorization"
}

func (f *AuthorizationFilter) Run(d *data.Data) (Response, error) {
	if d.Ctx.Err() != nil {
		return nil, d.Ctx.Err()
	}

	bucket := d.S3Info.Bucket
	key := d.S3Info.Key
	action := d.S3Info.Action
	identity := d.Identity // May be nil for anonymous requests

	// Build evaluation context for bucket policy
	evalCtx := f.buildEvalContext(d, identity)

	// Step 1: Check bucket policy (if exists)
	if f.policyStore != nil && bucket != "" {
		decision, err := f.evaluateBucketPolicy(d.Ctx, bucket, key, action, evalCtx)
		if err != nil {
			return nil, s3err.ErrInternalError
		}
		if decision == PolicyDeny {
			return nil, s3err.ErrAccessDenied
		}
		if decision == PolicyAllow {
			return Next{}, nil
		}
		// PolicyNotApplicable - continue to next check
	}

	// Step 2: Check IAM policies (only for authenticated users)
	if identity != nil {
		decision := f.evaluateIAMPolicies(d.Ctx, action, bucket, key, identity)
		if decision == PolicyDeny {
			return nil, s3err.ErrAccessDenied
		}
		if decision == PolicyAllow {
			return Next{}, nil
		}
	}

	// Step 3: Check ACLs (legacy but still supported)
	if f.aclStore != nil {
		allowed, err := f.evaluateACL(d.Ctx, bucket, key, action, identity)
		if err != nil {
			return nil, s3err.ErrInternalError
		}
		if allowed {
			return Next{}, nil
		}
	}

	// Step 4: Check if bucket owner (implicit full access)
	if identity != nil && bucket != "" {
		if f.isBucketOwner(d.Ctx, bucket, identity) {
			return Next{}, nil
		}
	}

	// No policy granted access - deny by default
	return nil, s3err.ErrAccessDenied
}

// PolicyDecision represents the result of policy evaluation
type PolicyDecision int

const (
	PolicyNotApplicable PolicyDecision = iota // Policy doesn't apply to this request
	PolicyAllow                               // Explicit allow
	PolicyDeny                                // Explicit deny
)

// buildEvalContext creates a PolicyEvaluationContext from request data
func (f *AuthorizationFilter) buildEvalContext(d *data.Data, identity *iam.Identity) *s3types.PolicyEvaluationContext {
	ctx := &s3types.PolicyEvaluationContext{
		Bucket: d.S3Info.Bucket,
		Key:    d.S3Info.Key,
	}

	// Extract request info
	if d.Req != nil {
		ctx.IsHTTPS = d.Req.TLS != nil
		ctx.UserAgent = d.Req.UserAgent()
		ctx.Referer = d.Req.Referer()

		// Parse source IP
		if ip := extractClientIP(d.Req); ip != "" {
			ctx.SourceIP = net.ParseIP(ip)
		}
	}

	// Set principal info
	if identity != nil {
		if identity.Account != nil {
			ctx.AccountID = identity.Account.ID
		}
		ctx.UserARN = buildUserARN(identity)
	}

	return ctx
}

// evaluateBucketPolicy checks if the bucket policy allows or denies the action
func (f *AuthorizationFilter) evaluateBucketPolicy(
	ctx context.Context,
	bucket, key string,
	action s3action.Action,
	evalCtx *s3types.PolicyEvaluationContext,
) (PolicyDecision, error) {
	policy, exists := f.policyStore.GetBucketPolicy(ctx, bucket)
	if !exists || policy == nil {
		return PolicyNotApplicable, nil
	}

	// Convert s3action.Action to s3types.S3Action
	s3Action := s3types.S3Action("s3:" + action.String())

	// Use the policy's built-in Evaluate method
	result := policy.Evaluate(s3Action, evalCtx)

	switch result {
	case s3types.EffectDeny:
		return PolicyDeny, nil
	case s3types.EffectAllow:
		return PolicyAllow, nil
	default:
		return PolicyNotApplicable, nil
	}
}

// evaluateIAMPolicies checks the user's attached IAM policies
func (f *AuthorizationFilter) evaluateIAMPolicies(
	ctx context.Context,
	action s3action.Action,
	bucket, key string,
	identity *iam.Identity,
) PolicyDecision {
	if f.iamEvaluator == nil {
		return PolicyNotApplicable
	}

	// Build IAM evaluation context
	evalCtx := &iam.PolicyEvaluationContext{
		Action:   action.String(),
		Bucket:   bucket,
		Key:      key,
		Resource: iam.BuildResourceARN(bucket, key),
	}

	// Add identity info
	if identity != nil {
		evalCtx.UserName = identity.Name
		evalCtx.UserARN = buildUserARN(identity)
		if identity.Account != nil {
			evalCtx.AccountID = identity.Account.ID
		}
	}

	// Evaluate IAM policies
	decision := f.iamEvaluator.Evaluate(ctx, identity, evalCtx)

	switch decision {
	case iam.DecisionDeny:
		return PolicyDeny
	case iam.DecisionAllow:
		return PolicyAllow
	default:
		return PolicyNotApplicable
	}
}

// evaluateACL checks bucket or object ACL for access
func (f *AuthorizationFilter) evaluateACL(
	ctx context.Context,
	bucket, key string,
	action s3action.Action,
	identity *iam.Identity,
) (bool, error) {
	var acl *s3types.AccessControlList
	var exists bool

	// Get the appropriate ACL
	if key != "" {
		acl, exists = f.aclStore.GetObjectACL(ctx, bucket, key)
	} else {
		acl, exists = f.aclStore.GetBucketACL(ctx, bucket)
	}

	if !exists || acl == nil {
		return false, s3err.ErrAccessDenied
	}

	// Get account ID and auth status
	var accountID string
	isAuthenticated := identity != nil
	if identity != nil && identity.Account != nil {
		accountID = identity.Account.ID
	}

	// Map S3 action to required ACL permission
	requiredPerm := actionToACLPermission(action)
	if requiredPerm == "" {
		return false, s3err.ErrAccessDenied // Actions not covered by ACLs need bucket/IAM policies
	}

	return acl.CheckAccess(accountID, isAuthenticated, requiredPerm), nil
}

// actionToACLPermission maps S3 actions to ACL permissions
func actionToACLPermission(action s3action.Action) s3types.Permission {
	switch action {
	case s3action.GetObject, s3action.GetObjectAcl, s3action.GetObjectTagging,
		s3action.ListObjects, s3action.ListObjectsV2, s3action.ListObjectVersions,
		s3action.HeadObject, s3action.HeadBucket:
		return s3types.PermissionRead
	case s3action.PutObject, s3action.DeleteObject, s3action.DeleteObjects,
		s3action.PutObjectTagging, s3action.DeleteObjectTagging:
		return s3types.PermissionWrite
	case s3action.GetBucketAcl:
		return s3types.PermissionReadACP
	case s3action.PutBucketAcl, s3action.PutObjectAcl:
		return s3types.PermissionWriteACP
	default:
		return "" // Actions not covered by ACLs need bucket/IAM policies
	}
}

// isBucketOwner checks if the identity owns the bucket
func (f *AuthorizationFilter) isBucketOwner(ctx context.Context, bucket string, identity *iam.Identity) bool {
	if f.bucketOwnerFunc != nil {
		return f.bucketOwnerFunc(ctx, bucket, identity)
	}
	// Default: no ownership check (rely on policies)
	return false
}

// buildUserARN constructs an ARN for the user
func buildUserARN(identity *iam.Identity) string {
	if identity == nil {
		return ""
	}
	// Format: arn:aws:iam::account-id:user/user-name
	accountID := "000000000000"
	if identity.Account != nil && identity.Account.ID != "" {
		accountID = identity.Account.ID
	}
	return "arn:aws:iam::" + accountID + ":user/" + identity.Name
}

// extractClientIP gets client IP from request headers or RemoteAddr
func extractClientIP(r *http.Request) string {
	// Check X-Forwarded-For first (may contain multiple IPs, take first)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if idx := strings.Index(xff, ","); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}

	// Check X-Real-IP
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}

	// Fall back to RemoteAddr
	if r.RemoteAddr != "" {
		if idx := strings.LastIndex(r.RemoteAddr, ":"); idx != -1 {
			return r.RemoteAddr[:idx]
		}
		return r.RemoteAddr
	}

	return ""
}
