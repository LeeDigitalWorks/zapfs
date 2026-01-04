// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"net"
	"testing"
)

// BenchmarkPolicyEvaluate benchmarks full policy evaluation
func BenchmarkPolicyEvaluate(b *testing.B) {
	// Create a realistic policy with multiple statements
	policy := &Policy{
		Version: "2012-10-17",
		Statements: []PolicyStatement{
			{
				Sid:       "AllowGetObject",
				Effect:    EffectAllow,
				Actions:   StringOrSlice{"s3:GetObject", "s3:GetObjectVersion"},
				Resources: StringOrSlice{"arn:aws:s3:::mybucket/*"},
			},
			{
				Sid:       "AllowListBucket",
				Effect:    EffectAllow,
				Actions:   StringOrSlice{"s3:ListBucket"},
				Resources: StringOrSlice{"arn:aws:s3:::mybucket"},
				Condition: map[string]Condition{
					"StringLike": {"s3:prefix": StringOrSlice{"public/*"}},
				},
			},
			{
				Sid:       "DenyDelete",
				Effect:    EffectDeny,
				Actions:   StringOrSlice{"s3:DeleteObject", "s3:DeleteBucket"},
				Resources: StringOrSlice{"arn:aws:s3:::mybucket", "arn:aws:s3:::mybucket/*"},
			},
		},
	}

	ctx := &PolicyEvaluationContext{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/folder/subfolder/document.pdf",
		Bucket:   "mybucket",
		Key:      "folder/subfolder/document.pdf",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		policy.Evaluate(ctx)
	}
}

// BenchmarkPolicyStatementEvaluate benchmarks single statement evaluation
func BenchmarkPolicyStatementEvaluate(b *testing.B) {
	stmt := PolicyStatement{
		Effect:    EffectAllow,
		Actions:   StringOrSlice{"s3:*"},
		Resources: StringOrSlice{"arn:aws:s3:::*", "arn:aws:s3:::*/*"},
	}

	ctx := &PolicyEvaluationContext{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/key.txt",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stmt.Evaluate(ctx)
	}
}

// BenchmarkWildcardMatch benchmarks the wildcard matching function
func BenchmarkWildcardMatch(b *testing.B) {
	cases := []struct {
		name    string
		pattern string
		value   string
	}{
		{"exact_match", "s3:GetObject", "s3:GetObject"},
		{"star_wildcard", "s3:*", "s3:GetObject"},
		{"prefix_wildcard", "s3:Get*", "s3:GetObjectVersion"},
		{"suffix_wildcard", "*Object", "s3:GetObject"},
		{"s3_arn_wildcard", "arn:aws:s3:::mybucket/*", "arn:aws:s3:::mybucket/folder/subfolder/file.txt"},
		{"complex_pattern", "arn:aws:s3:::*-prod/*-backup/*.json", "arn:aws:s3:::myapp-prod/db-backup/data.json"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				matchWildcard(tc.pattern, tc.value)
			}
		})
	}
}

// BenchmarkConditionEvaluation benchmarks condition evaluation
func BenchmarkConditionEvaluation(b *testing.B) {
	stmt := PolicyStatement{
		Effect:    EffectAllow,
		Actions:   StringOrSlice{"s3:GetObject"},
		Resources: StringOrSlice{"arn:aws:s3:::mybucket/*"},
		Condition: map[string]Condition{
			"IpAddress":    {"aws:SourceIp": StringOrSlice{"192.168.1.0/24", "10.0.0.0/8"}},
			"StringEquals": {"aws:SecureTransport": StringOrSlice{"true"}},
			"StringLike":   {"s3:prefix": StringOrSlice{"public/*", "shared/*"}},
		},
	}

	ctx := &PolicyEvaluationContext{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::mybucket/public/file.txt",
		SourceIP: net.ParseIP("192.168.1.50"),
		IsHTTPS:  true,
		Prefix:   "public/",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stmt.Evaluate(ctx)
	}
}

// BenchmarkPolicyEvaluator benchmarks the full evaluator with multiple policies
func BenchmarkPolicyEvaluator(b *testing.B) {
	// Create mock store with multiple policies
	store := NewMemoryPolicyStore()

	// Add several policies for the user
	policies := []*Policy{
		{
			Version: "2012-10-17",
			Statements: []PolicyStatement{
				{Effect: EffectAllow, Actions: StringOrSlice{"s3:Get*"}, Resources: StringOrSlice{"arn:aws:s3:::bucket1/*"}},
			},
		},
		{
			Version: "2012-10-17",
			Statements: []PolicyStatement{
				{Effect: EffectAllow, Actions: StringOrSlice{"s3:List*"}, Resources: StringOrSlice{"arn:aws:s3:::bucket1"}},
			},
		},
		{
			Version: "2012-10-17",
			Statements: []PolicyStatement{
				{Effect: EffectDeny, Actions: StringOrSlice{"s3:Delete*"}, Resources: StringOrSlice{"arn:aws:s3:::*"}},
			},
		},
	}
	store.SetUserPolicies("testuser", policies)

	evaluator := NewPolicyEvaluator(store, nil)
	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket1", "folder/file.txt")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		evaluator.Evaluate(ctx, identity, evalCtx)
	}
}

// BenchmarkIPAddressMatch benchmarks IP address condition matching
func BenchmarkIPAddressMatch(b *testing.B) {
	ip := net.ParseIP("192.168.1.100")
	cidrs := []string{
		"192.168.0.0/16",
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.1.0/24",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ipAddressMatch(ip, cidrs)
	}
}

// BenchmarkManyStatements benchmarks policy with many statements (realistic enterprise scenario)
func BenchmarkManyStatements(b *testing.B) {
	// Build a policy with 50 statements (common in enterprise)
	statements := make([]PolicyStatement, 50)
	for i := 0; i < 50; i++ {
		statements[i] = PolicyStatement{
			Sid:       "Statement" + string(rune('A'+i%26)),
			Effect:    EffectAllow,
			Actions:   StringOrSlice{"s3:GetObject", "s3:PutObject"},
			Resources: StringOrSlice{"arn:aws:s3:::bucket-" + string(rune('a'+i%26)) + "/*"},
		}
	}
	// Add a matching statement near the end
	statements[48] = PolicyStatement{
		Effect:    EffectAllow,
		Actions:   StringOrSlice{"s3:*"},
		Resources: StringOrSlice{"arn:aws:s3:::target-bucket/*"},
	}

	policy := &Policy{Version: "2012-10-17", Statements: statements}

	ctx := &PolicyEvaluationContext{
		Action:   "s3:GetObject",
		Resource: "arn:aws:s3:::target-bucket/important-file.txt",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		policy.Evaluate(ctx)
	}
}

// BenchmarkParallelEvaluation benchmarks concurrent policy evaluation
func BenchmarkParallelEvaluation(b *testing.B) {
	policy := FullAccessPolicy()
	ctx := BuildEvaluationContext("s3:GetObject", "mybucket", "file.txt")

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			policy.Evaluate(ctx)
		}
	})
}
