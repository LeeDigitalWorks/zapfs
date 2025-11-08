package iam

import (
	"encoding/json"
	"net"
	"strings"
	"time"
)

// Policy represents an IAM policy document that can be attached to users, groups, or roles.
// The structure mirrors AWS IAM policies for compatibility.
//
// Example policy:
//
//	{
//	  "Version": "2012-10-17",
//	  "Statement": [{
//	    "Effect": "Allow",
//	    "Action": ["s3:GetObject", "s3:ListBucket"],
//	    "Resource": ["arn:aws:s3:::mybucket", "arn:aws:s3:::mybucket/*"]
//	  }]
//	}
type Policy struct {
	Version    string            `json:"Version"`
	ID         string            `json:"Id,omitempty"`
	Statements []PolicyStatement `json:"Statement"`
}

// ToJSON serializes the policy to JSON string
func (p *Policy) ToJSON() (string, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// FromJSON parses a JSON policy document
func PolicyFromJSON(jsonDoc string) (*Policy, error) {
	var p Policy
	if err := json.Unmarshal([]byte(jsonDoc), &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// PolicyStatement represents a single permission statement in an IAM policy
type PolicyStatement struct {
	Sid          string               `json:"Sid,omitempty"`
	Effect       PolicyEffect         `json:"Effect"`
	Actions      StringOrSlice        `json:"Action"`
	NotActions   StringOrSlice        `json:"NotAction,omitempty"`
	Resources    StringOrSlice        `json:"Resource"`
	NotResources StringOrSlice        `json:"NotResource,omitempty"`
	Condition    map[string]Condition `json:"Condition,omitempty"`
}

// PolicyEffect determines whether a statement allows or denies access
type PolicyEffect string

const (
	EffectAllow PolicyEffect = "Allow"
	EffectDeny  PolicyEffect = "Deny"
)

// Condition represents conditional access rules
// Key is the condition key (aws:SourceIp, s3:prefix, etc.)
type Condition map[string]StringOrSlice

// StringOrSlice handles JSON fields that can be either a string or []string
type StringOrSlice []string

func (s *StringOrSlice) UnmarshalJSON(data []byte) error {
	// Try string first
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		*s = []string{str}
		return nil
	}
	// Try array
	var arr []string
	if err := json.Unmarshal(data, &arr); err != nil {
		return err
	}
	*s = arr
	return nil
}

func (s StringOrSlice) MarshalJSON() ([]byte, error) {
	if len(s) == 1 {
		return json.Marshal(s[0])
	}
	return json.Marshal([]string(s))
}

// PolicyEvaluationContext contains all information needed to evaluate a policy
type PolicyEvaluationContext struct {
	// Principal info
	AccountID   string   // Account ID of the requester
	UserName    string   // User name
	UserARN     string   // Full ARN of the user
	Groups      []string // Groups the user belongs to
	AssumedRole string   // Role ARN if using assumed role

	// Request info
	Action    string // The S3 action being performed (e.g., "s3:GetObject")
	Resource  string // The resource ARN being accessed
	SourceIP  net.IP // Client IP address
	IsHTTPS   bool   // Whether request is over HTTPS
	Timestamp time.Time
	UserAgent string
	Referer   string

	// S3-specific context keys
	Bucket string            // Bucket name
	Key    string            // Object key
	Prefix string            // List prefix (for s3:prefix condition)
	Tags   map[string]string // Object/request tags
}

// PolicyDecision represents the result of policy evaluation
type PolicyDecision int

const (
	DecisionNotApplicable PolicyDecision = iota // Policy doesn't apply
	DecisionAllow                               // Explicit allow
	DecisionDeny                                // Explicit deny
)

func (d PolicyDecision) String() string {
	switch d {
	case DecisionAllow:
		return "Allow"
	case DecisionDeny:
		return "Deny"
	default:
		return "NotApplicable"
	}
}

// Evaluate checks if this policy allows or denies the action in the given context.
// Returns DecisionDeny if any statement explicitly denies (deny always wins).
// Returns DecisionAllow if any statement explicitly allows and none deny.
// Returns DecisionNotApplicable if no statements match.
func (p *Policy) Evaluate(ctx *PolicyEvaluationContext) PolicyDecision {
	var hasAllow bool

	for _, stmt := range p.Statements {
		decision := stmt.Evaluate(ctx)
		switch decision {
		case DecisionDeny:
			return DecisionDeny // Explicit deny always wins
		case DecisionAllow:
			hasAllow = true
		}
	}

	if hasAllow {
		return DecisionAllow
	}
	return DecisionNotApplicable
}

// Evaluate checks if this statement applies to the context and what effect it has
func (s *PolicyStatement) Evaluate(ctx *PolicyEvaluationContext) PolicyDecision {
	// Check if action matches
	if !s.matchesAction(ctx.Action) {
		return DecisionNotApplicable
	}

	// Check if resource matches
	if !s.matchesResource(ctx.Resource) {
		return DecisionNotApplicable
	}

	// Check conditions
	if !s.matchesConditions(ctx) {
		return DecisionNotApplicable
	}

	// Statement applies - return its effect
	if s.Effect == EffectDeny {
		return DecisionDeny
	}
	return DecisionAllow
}

// matchesAction checks if the statement's Action/NotAction matches the requested action
func (s *PolicyStatement) matchesAction(action string) bool {
	// Handle NotAction (inverse matching)
	if len(s.NotActions) > 0 {
		for _, pattern := range s.NotActions {
			if matchWildcard(string(pattern), action) {
				return false // NotAction matched, statement doesn't apply
			}
		}
		return true // None of the NotActions matched
	}

	// Handle Action
	for _, pattern := range s.Actions {
		if matchWildcard(string(pattern), action) {
			return true
		}
	}
	return false
}

// matchesResource checks if the statement's Resource/NotResource matches the requested resource
func (s *PolicyStatement) matchesResource(resource string) bool {
	// Handle NotResource (inverse matching)
	if len(s.NotResources) > 0 {
		for _, pattern := range s.NotResources {
			if matchWildcard(pattern, resource) {
				return false // NotResource matched, statement doesn't apply
			}
		}
		return true // None of the NotResources matched
	}

	// Handle Resource
	for _, pattern := range s.Resources {
		if matchWildcard(pattern, resource) {
			return true
		}
	}
	return false
}

// matchesConditions evaluates all conditions in the statement
func (s *PolicyStatement) matchesConditions(ctx *PolicyEvaluationContext) bool {
	if len(s.Condition) == 0 {
		return true // No conditions means always matches
	}

	for operator, conditions := range s.Condition {
		for condKey, condValues := range conditions {
			if !evaluateCondition(operator, condKey, condValues, ctx) {
				return false // All conditions must match
			}
		}
	}
	return true
}

// evaluateCondition evaluates a single condition
func evaluateCondition(operator, key string, values []string, ctx *PolicyEvaluationContext) bool {
	// Get the actual value for the condition key
	actualValue := getConditionValue(key, ctx)

	switch operator {
	case "StringEquals", "StringEqualsIfExists":
		if operator == "StringEqualsIfExists" && actualValue == "" {
			return true
		}
		return stringEquals(actualValue, values)

	case "StringNotEquals":
		return !stringEquals(actualValue, values)

	case "StringLike", "StringLikeIfExists":
		if operator == "StringLikeIfExists" && actualValue == "" {
			return true
		}
		return stringLike(actualValue, values)

	case "StringNotLike":
		return !stringLike(actualValue, values)

	case "IpAddress", "IpAddressIfExists":
		if operator == "IpAddressIfExists" && ctx.SourceIP == nil {
			return true
		}
		return ipAddressMatch(ctx.SourceIP, values)

	case "NotIpAddress":
		return !ipAddressMatch(ctx.SourceIP, values)

	case "Bool":
		return boolMatch(actualValue, values)

	case "Null":
		isNull := actualValue == ""
		for _, v := range values {
			if (v == "true" && isNull) || (v == "false" && !isNull) {
				return true
			}
		}
		return false

	default:
		// Unknown operator - fail closed
		return false
	}
}

// getConditionValue retrieves the value for a condition key from the context
func getConditionValue(key string, ctx *PolicyEvaluationContext) string {
	switch key {
	// AWS global condition keys
	case "aws:SourceIp":
		if ctx.SourceIP != nil {
			return ctx.SourceIP.String()
		}
		return ""
	case "aws:SecureTransport":
		if ctx.IsHTTPS {
			return "true"
		}
		return "false"
	case "aws:UserAgent":
		return ctx.UserAgent
	case "aws:Referer":
		return ctx.Referer
	case "aws:CurrentTime":
		return ctx.Timestamp.Format(time.RFC3339)
	case "aws:username":
		return ctx.UserName
	case "aws:userid":
		return ctx.AccountID
	case "aws:PrincipalArn":
		return ctx.UserARN

	// S3-specific condition keys
	case "s3:prefix":
		return ctx.Prefix
	case "s3:x-amz-acl":
		// Would need to be passed in context
		return ""
	case "s3:ExistingObjectTag/<key>":
		// Would need tag lookup
		return ""

	default:
		// Check for tag-based conditions
		if strings.HasPrefix(key, "aws:RequestTag/") {
			tagKey := strings.TrimPrefix(key, "aws:RequestTag/")
			if ctx.Tags != nil {
				return ctx.Tags[tagKey]
			}
		}
		return ""
	}
}

// Helper functions for condition evaluation

func stringEquals(actual string, expected []string) bool {
	for _, v := range expected {
		if actual == v {
			return true
		}
	}
	return false
}

func stringLike(actual string, patterns []string) bool {
	for _, pattern := range patterns {
		if matchWildcard(pattern, actual) {
			return true
		}
	}
	return false
}

func ipAddressMatch(ip net.IP, cidrs []string) bool {
	if ip == nil {
		return false
	}
	for _, cidr := range cidrs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			// Try as plain IP
			if matchIP := net.ParseIP(cidr); matchIP != nil {
				if ip.Equal(matchIP) {
					return true
				}
			}
			continue
		}
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func boolMatch(actual string, values []string) bool {
	for _, v := range values {
		if strings.EqualFold(actual, v) {
			return true
		}
	}
	return false
}

// matchWildcard performs simple wildcard matching with * and ?
func matchWildcard(pattern, value string) bool {
	// Handle exact match
	if pattern == value {
		return true
	}

	// Handle full wildcard
	if pattern == "*" {
		return true
	}

	// Simple prefix/suffix wildcard (most common cases)
	if strings.HasSuffix(pattern, "*") && !strings.Contains(pattern[:len(pattern)-1], "*") {
		return strings.HasPrefix(value, pattern[:len(pattern)-1])
	}
	if strings.HasPrefix(pattern, "*") && !strings.Contains(pattern[1:], "*") {
		return strings.HasSuffix(value, pattern[1:])
	}

	// Full wildcard matching for complex patterns
	return wildcardMatch(pattern, value)
}

// wildcardMatch performs full wildcard matching
func wildcardMatch(pattern, value string) bool {
	// Simple recursive implementation
	// For production, consider using a more efficient algorithm
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			// Try matching zero or more characters
			for i := 0; i <= len(value); i++ {
				if wildcardMatch(pattern[1:], value[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(value) == 0 {
				return false
			}
			pattern = pattern[1:]
			value = value[1:]
		default:
			if len(value) == 0 || pattern[0] != value[0] {
				return false
			}
			pattern = pattern[1:]
			value = value[1:]
		}
	}
	return len(value) == 0
}
