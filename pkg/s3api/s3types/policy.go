// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import (
	"encoding/json"
	"net"
	"strings"
	"time"
)

// S3Action represents an S3 action that can be performed
// Follows AWS S3 action naming: s3:GetObject, s3:PutObject, etc.
type S3Action string

const (
	// Object actions
	S3ActionGetObject                S3Action = "s3:GetObject"
	S3ActionGetObjectVersion         S3Action = "s3:GetObjectVersion"
	S3ActionPutObject                S3Action = "s3:PutObject"
	S3ActionDeleteObject             S3Action = "s3:DeleteObject"
	S3ActionDeleteObjectVersion      S3Action = "s3:DeleteObjectVersion"
	S3ActionCopyObject               S3Action = "s3:CopyObject"
	S3ActionHeadObject               S3Action = "s3:HeadObject"
	S3ActionGetObjectTagging         S3Action = "s3:GetObjectTagging"
	S3ActionPutObjectTagging         S3Action = "s3:PutObjectTagging"
	S3ActionDeleteObjectTagging      S3Action = "s3:DeleteObjectTagging"
	S3ActionGetObjectAcl             S3Action = "s3:GetObjectAcl"
	S3ActionPutObjectAcl             S3Action = "s3:PutObjectAcl"
	S3ActionRestoreObject            S3Action = "s3:RestoreObject"
	S3ActionAbortMultipartUpload     S3Action = "s3:AbortMultipartUpload"
	S3ActionListMultipartUploadParts S3Action = "s3:ListMultipartUploadParts"

	// Bucket actions
	S3ActionListBucket                 S3Action = "s3:ListBucket"
	S3ActionListBucketVersions         S3Action = "s3:ListBucketVersions"
	S3ActionListBucketMultipartUploads S3Action = "s3:ListBucketMultipartUploads"
	S3ActionCreateBucket               S3Action = "s3:CreateBucket"
	S3ActionDeleteBucket               S3Action = "s3:DeleteBucket"
	S3ActionGetBucketLocation          S3Action = "s3:GetBucketLocation"
	S3ActionGetBucketPolicy            S3Action = "s3:GetBucketPolicy"
	S3ActionPutBucketPolicy            S3Action = "s3:PutBucketPolicy"
	S3ActionDeleteBucketPolicy         S3Action = "s3:DeleteBucketPolicy"
	S3ActionGetBucketAcl               S3Action = "s3:GetBucketAcl"
	S3ActionPutBucketAcl               S3Action = "s3:PutBucketAcl"
	S3ActionGetBucketVersioning        S3Action = "s3:GetBucketVersioning"
	S3ActionPutBucketVersioning        S3Action = "s3:PutBucketVersioning"
	S3ActionGetBucketLifecycle         S3Action = "s3:GetLifecycleConfiguration"
	S3ActionPutBucketLifecycle         S3Action = "s3:PutLifecycleConfiguration"
	S3ActionGetBucketTagging           S3Action = "s3:GetBucketTagging"
	S3ActionPutBucketTagging           S3Action = "s3:PutBucketTagging"

	// Service actions
	S3ActionListAllMyBuckets S3Action = "s3:ListAllMyBuckets"

	// Wildcards
	S3ActionAll       S3Action = "s3:*"
	S3ActionAllGet    S3Action = "s3:Get*"
	S3ActionAllPut    S3Action = "s3:Put*"
	S3ActionAllDelete S3Action = "s3:Delete*"
	S3ActionAllList   S3Action = "s3:List*"
)

// Effect determines whether a statement allows or denies access
type Effect string

const (
	EffectAllow Effect = "Allow"
	EffectDeny  Effect = "Deny"
)

// Principal represents who the policy applies to
type Principal struct {
	AWS       []string `json:"AWS,omitempty"`       // AWS account IDs or ARNs
	Service   []string `json:"Service,omitempty"`   // AWS services
	Federated []string `json:"Federated,omitempty"` // Federated identity providers
}

// UnmarshalJSON handles both string "*" and object forms of Principal
func (p *Principal) UnmarshalJSON(data []byte) error {
	// Try string first (for "*" wildcard)
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		if str == "*" {
			p.AWS = []string{"*"}
		}
		return nil
	}

	// Try object form
	type alias Principal
	return json.Unmarshal(data, (*alias)(p))
}

// IsPublic returns true if the principal allows public access
func (p *Principal) IsPublic() bool {
	for _, aws := range p.AWS {
		if aws == "*" {
			return true
		}
	}
	return false
}

// Statement represents a single permission statement in a bucket policy
type Statement struct {
	Sid          string               `json:"Sid,omitempty"`
	Effect       Effect               `json:"Effect"`
	Principal    *Principal           `json:"Principal,omitempty"`
	NotPrincipal *Principal           `json:"NotPrincipal,omitempty"`
	Actions      []S3Action           `json:"-"` // Custom unmarshal handles Action field
	NotActions   []S3Action           `json:"-"` // Custom unmarshal handles NotAction field
	Resources    []string             `json:"-"` // Custom unmarshal handles Resource field
	NotResources []string             `json:"-"` // Custom unmarshal handles NotResource field
	Condition    map[string]Condition `json:"Condition,omitempty"`
}

// UnmarshalJSON handles both string and array forms of Action and Resource.
// AWS S3 policy JSON allows: "Action": "s3:GetObject" or "Action": ["s3:GetObject"]
func (s *Statement) UnmarshalJSON(data []byte) error {
	// Use an alias to avoid infinite recursion
	type Alias Statement
	aux := &struct {
		Action      interface{} `json:"Action"`
		NotAction   interface{} `json:"NotAction"`
		Resource    interface{} `json:"Resource"`
		NotResource interface{} `json:"NotResource"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Parse Action (can be string or []string)
	if aux.Action != nil {
		actions, err := parseStringOrArray(aux.Action)
		if err != nil {
			return err
		}
		for _, a := range actions {
			s.Actions = append(s.Actions, S3Action(a))
		}
	}

	// Parse NotAction
	if aux.NotAction != nil {
		notActions, err := parseStringOrArray(aux.NotAction)
		if err != nil {
			return err
		}
		for _, a := range notActions {
			s.NotActions = append(s.NotActions, S3Action(a))
		}
	}

	// Parse Resource (can be string or []string)
	if aux.Resource != nil {
		resources, err := parseStringOrArray(aux.Resource)
		if err != nil {
			return err
		}
		s.Resources = resources
	}

	// Parse NotResource
	if aux.NotResource != nil {
		notResources, err := parseStringOrArray(aux.NotResource)
		if err != nil {
			return err
		}
		s.NotResources = notResources
	}

	return nil
}

// MarshalJSON outputs Action/Resource as arrays (canonical form)
func (s Statement) MarshalJSON() ([]byte, error) {
	type Alias Statement
	return json.Marshal(&struct {
		Action      []S3Action `json:"Action,omitempty"`
		NotAction   []S3Action `json:"NotAction,omitempty"`
		Resource    []string   `json:"Resource,omitempty"`
		NotResource []string   `json:"NotResource,omitempty"`
		*Alias
	}{
		Action:      s.Actions,
		NotAction:   s.NotActions,
		Resource:    s.Resources,
		NotResource: s.NotResources,
		Alias:       (*Alias)(&s),
	})
}

// parseStringOrArray converts a JSON value that could be string or []string to []string
func parseStringOrArray(v interface{}) ([]string, error) {
	switch val := v.(type) {
	case string:
		return []string{val}, nil
	case []interface{}:
		result := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result, nil
	default:
		return nil, nil
	}
}

// Condition represents conditional access rules
// Key is the condition operator (IpAddress, StringEquals, etc.)
type Condition map[string][]string

// BucketPolicy represents an S3 bucket policy document
type BucketPolicy struct {
	Version    string      `json:"Version"`
	ID         string      `json:"Id,omitempty"`
	Statements []Statement `json:"Statement"`
}

// PolicyEvaluationContext contains request metadata for policy evaluation
type PolicyEvaluationContext struct {
	// Request info
	SourceIP  net.IP
	IsHTTPS   bool
	Timestamp time.Time
	UserAgent string
	Referer   string

	// Principal info
	AccountID string // Canonical user ID making the request
	UserARN   string // ARN of the user/role

	// Resource info
	Bucket string
	Key    string
}

// EvaluationResult represents the outcome of policy evaluation
type EvaluationResult struct {
	Effect       Effect
	Matched      bool // True if any statement matched (explicit allow/deny)
	ExplicitDeny bool // True if there was an explicit Deny statement
}

// Evaluate checks if the policy allows the given action.
// Returns the effect and whether a statement explicitly matched.
func (bp *BucketPolicy) Evaluate(action S3Action, ctx *PolicyEvaluationContext) Effect {
	result := bp.EvaluateWithResult(action, ctx)
	return result.Effect
}

// EvaluateWithResult checks if the policy allows the given action and returns
// detailed information about the match. Use this when you need to distinguish
// between explicit deny and no-match (implicit deny).
func (bp *BucketPolicy) EvaluateWithResult(action S3Action, ctx *PolicyEvaluationContext) EvaluationResult {
	result := EvaluationResult{
		Effect:       EffectDeny, // Default deny
		Matched:      false,
		ExplicitDeny: false,
	}

	for _, stmt := range bp.Statements {
		if !stmt.matchesPrincipal(ctx) {
			continue
		}
		if !stmt.matchesAction(action) {
			continue
		}
		if !stmt.matchesResource(ctx.Bucket, ctx.Key) {
			continue
		}
		if !stmt.matchesCondition(ctx) {
			continue
		}

		result.Matched = true
		switch stmt.Effect {
		case EffectDeny:
			result.ExplicitDeny = true
			result.Effect = EffectDeny
			return result // Explicit deny always wins, return immediately
		case EffectAllow:
			result.Effect = EffectAllow
		}
	}

	return result
}

func (s *Statement) matchesPrincipal(ctx *PolicyEvaluationContext) bool {
	if s.Principal == nil && s.NotPrincipal == nil {
		return true // No principal restriction
	}

	if s.NotPrincipal != nil {
		// NotPrincipal: deny these principals
		for _, aws := range s.NotPrincipal.AWS {
			if aws == "*" || aws == ctx.AccountID || aws == ctx.UserARN {
				return false
			}
		}
		return true
	}

	// Principal: allow these principals
	for _, aws := range s.Principal.AWS {
		if aws == "*" || aws == ctx.AccountID || aws == ctx.UserARN {
			return true
		}
	}
	return false
}

func (s *Statement) matchesAction(action S3Action) bool {
	// Check NotAction first
	if len(s.NotActions) > 0 {
		for _, a := range s.NotActions {
			if matchActionPattern(a, action) {
				return false
			}
		}
		return true
	}

	// Check Action
	for _, a := range s.Actions {
		if matchActionPattern(a, action) {
			return true
		}
	}
	return false
}

func (s *Statement) matchesResource(bucket, key string) bool {
	resource := buildResourceARN(bucket, key)

	// Check NotResource first
	if len(s.NotResources) > 0 {
		for _, r := range s.NotResources {
			if matchResourcePattern(r, resource) {
				return false
			}
		}
		return true
	}

	// Check Resource
	for _, r := range s.Resources {
		if matchResourcePattern(r, resource) {
			return true
		}
	}
	return false
}

func (s *Statement) matchesCondition(ctx *PolicyEvaluationContext) bool {
	if len(s.Condition) == 0 {
		return true
	}

	for operator, conditions := range s.Condition {
		for condKey, values := range conditions {
			if !evaluateCondition(operator, condKey, values, ctx) {
				return false
			}
		}
	}
	return true
}

// Helper functions

func buildResourceARN(bucket, key string) string {
	if bucket == "" {
		return "arn:aws:s3:::*"
	}
	if key == "" {
		return "arn:aws:s3:::" + bucket
	}
	return "arn:aws:s3:::" + bucket + "/" + key
}

func matchActionPattern(pattern, action S3Action) bool {
	if pattern == S3ActionAll || pattern == action {
		return true
	}

	patternStr := string(pattern)
	actionStr := string(action)

	// Handle wildcards like s3:Get*
	if strings.HasSuffix(patternStr, "*") {
		prefix := strings.TrimSuffix(patternStr, "*")
		return strings.HasPrefix(actionStr, prefix)
	}

	return false
}

func matchResourcePattern(pattern, resource string) bool {
	if pattern == "*" || pattern == resource {
		return true
	}

	// Handle wildcards
	if strings.Contains(pattern, "*") {
		// Convert to simple glob matching
		parts := strings.Split(pattern, "*")
		if len(parts) == 2 {
			return strings.HasPrefix(resource, parts[0]) && strings.HasSuffix(resource, parts[1])
		}
		// Multiple wildcards - just check prefix
		return strings.HasPrefix(resource, parts[0])
	}

	return false
}

func matchIP(ip net.IP, cidrs []string) bool {
	if ip == nil {
		return false
	}
	for _, cidr := range cidrs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			// Try as single IP
			if net.ParseIP(cidr).Equal(ip) {
				return true
			}
			continue
		}
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func evaluateCondition(operator, condKey string, values []string, ctx *PolicyEvaluationContext) bool {
	switch operator {
	case "IpAddress":
		if condKey == "aws:SourceIp" {
			return matchIP(ctx.SourceIP, values)
		}
	case "NotIpAddress":
		if condKey == "aws:SourceIp" {
			return !matchIP(ctx.SourceIP, values)
		}
	case "Bool":
		if condKey == "aws:SecureTransport" {
			for _, v := range values {
				if v == "true" && ctx.IsHTTPS {
					return true
				}
				if v == "false" && !ctx.IsHTTPS {
					return true
				}
			}
			return false
		}
	case "StringEquals":
		val := getConditionValue(condKey, ctx)
		for _, v := range values {
			if v == val {
				return true
			}
		}
		return false
	case "StringNotEquals":
		val := getConditionValue(condKey, ctx)
		for _, v := range values {
			if v == val {
				return false
			}
		}
		return true
	case "StringLike":
		val := getConditionValue(condKey, ctx)
		for _, pattern := range values {
			if matchStringLike(pattern, val) {
				return true
			}
		}
		return false
	}

	return true // Unknown condition - allow by default
}

func getConditionValue(key string, ctx *PolicyEvaluationContext) string {
	switch key {
	case "aws:UserAgent":
		return ctx.UserAgent
	case "aws:Referer":
		return ctx.Referer
	case "aws:SourceIp":
		if ctx.SourceIP != nil {
			return ctx.SourceIP.String()
		}
	case "aws:PrincipalArn":
		return ctx.UserARN
	case "aws:userid":
		return ctx.AccountID
	}
	return ""
}

// matchStringLike implements glob pattern matching for IAM/S3 policy conditions.
// Supports:
//   - * matches zero or more characters
//   - ? matches exactly one character
//
// Examples:
//   - "*.example.com" matches "foo.example.com"
//   - "user-*" matches "user-123"
//   - "file?.txt" matches "file1.txt" but not "file10.txt"
//   - "prefix-*-suffix" matches "prefix-middle-suffix"
func matchStringLike(pattern, value string) bool {
	// Empty pattern only matches empty value
	if pattern == "" {
		return value == ""
	}

	// * matches everything
	if pattern == "*" {
		return true
	}

	// No wildcards - exact match
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		return pattern == value
	}

	// Use dynamic programming for proper glob matching
	return matchGlob(pattern, value)
}

// matchGlob implements glob matching using dynamic programming.
// Time: O(m*n), Space: O(n) where m=len(pattern), n=len(value)
func matchGlob(pattern, value string) bool {
	m, n := len(pattern), len(value)

	// dp[j] = true if pattern[0:i] matches value[0:j]
	// We use rolling array to optimize space from O(m*n) to O(n)
	dp := make([]bool, n+1)
	dp[0] = true

	// Handle leading * characters (they can match empty string)
	for i := 0; i < m && pattern[i] == '*'; i++ {
		dp[0] = true
	}

	for i := 0; i < m; i++ {
		c := pattern[i]

		switch c {
		case '*':
			// * can match empty string (keep dp[j] as is) or extend match
			// dp[j] = dp[j] (match zero chars) || dp[j-1] (match one more char)
			for j := 1; j <= n; j++ {
				dp[j] = dp[j] || dp[j-1]
			}
		case '?':
			// ? must match exactly one character
			// Traverse right to left to avoid using updated values
			for j := n; j >= 1; j-- {
				dp[j] = dp[j-1]
			}
			dp[0] = false
		default:
			// Regular character - must match exactly
			for j := n; j >= 1; j-- {
				dp[j] = dp[j-1] && value[j-1] == c
			}
			dp[0] = false
		}
	}

	return dp[n]
}
