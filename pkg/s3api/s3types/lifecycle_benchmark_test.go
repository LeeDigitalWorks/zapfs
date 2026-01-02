// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import (
	"testing"
	"time"
)

// BenchmarkLifecycleEval benchmarks the full lifecycle evaluation
func BenchmarkLifecycleEval(b *testing.B) {
	days30 := int64(30)
	days90 := int64(90)
	prefix := "logs/"
	glacierClass := "GLACIER"
	ruleID := "expire-logs"

	lc := &Lifecycle{
		Rules: []LifecycleRule{
			{
				ID:     &ruleID,
				Status: LifecycleStatusEnabled,
				Filter: &LifecycleFilter{
					Prefix: &prefix,
				},
				Expiration: &LifecycleExpiration{
					Days: &days90,
				},
				Transitions: []*LifecycleTransition{
					{
						Days:         &days30,
						StorageClass: &glacierClass,
					},
				},
			},
		},
	}

	evaluator := NewEvaluator(lc)
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	obj := ObjectState{
		Name:     "logs/2024/01/access.log",
		Size:     1024 * 1024,
		ModTime:  now.AddDate(0, -2, 0), // 2 months old
		IsLatest: true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		evaluator.Eval(obj, now)
	}
}

// BenchmarkLifecycleRuleMatching benchmarks rule filter matching
func BenchmarkLifecycleRuleMatching(b *testing.B) {
	prefix := "data/"
	minSize := int64(1024)
	maxSize := int64(1024 * 1024 * 100)
	days90 := int64(90)

	lc := &Lifecycle{
		Rules: []LifecycleRule{
			{
				Status: LifecycleStatusEnabled,
				Filter: &LifecycleFilter{
					And: &LifecycleRuleAndOperator{
						Prefix: &prefix,
						Tags: []*Tag{
							{Key: "environment", Value: "production"},
							{Key: "department", Value: "engineering"},
						},
						ObjectSizeGreaterThan: &minSize,
						ObjectSizeLessThan:    &maxSize,
					},
				},
				Expiration: &LifecycleExpiration{
					Days: &days90,
				},
			},
		},
	}

	evaluator := NewEvaluator(lc)
	now := time.Now().UTC()
	obj := ObjectState{
		Name:     "data/project/report.pdf",
		Size:     50 * 1024, // 50KB - matches size filter
		ModTime:  now.AddDate(0, -6, 0),
		IsLatest: true,
		UserTags: "environment=production&department=engineering",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		evaluator.Eval(obj, now)
	}
}

// BenchmarkLifecycleManyRules benchmarks evaluation with many rules
func BenchmarkLifecycleManyRules(b *testing.B) {
	rules := make([]LifecycleRule, 50)
	for i := 0; i < 50; i++ {
		days := int64((i + 1) * 30)
		prefix := "prefix" + string(rune('a'+i%26)) + "/"
		id := "rule-" + string(rune('0'+i%10))
		rules[i] = LifecycleRule{
			ID:     &id,
			Status: LifecycleStatusEnabled,
			Filter: &LifecycleFilter{
				Prefix: &prefix,
			},
			Expiration: &LifecycleExpiration{
				Days: &days,
			},
		}
	}

	// Add a matching rule for our test object
	matchPrefix := "target/"
	matchDays := int64(60)
	matchID := "target-rule"
	rules[48] = LifecycleRule{
		ID:     &matchID,
		Status: LifecycleStatusEnabled,
		Filter: &LifecycleFilter{
			Prefix: &matchPrefix,
		},
		Expiration: &LifecycleExpiration{
			Days: &matchDays,
		},
	}

	lc := &Lifecycle{Rules: rules}
	evaluator := NewEvaluator(lc)
	now := time.Now().UTC()
	obj := ObjectState{
		Name:     "target/important/file.txt",
		Size:     1024,
		ModTime:  now.AddDate(0, -3, 0), // 3 months old
		IsLatest: true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		evaluator.Eval(obj, now)
	}
}

// BenchmarkLifecycleNoncurrentVersions benchmarks noncurrent version evaluation
func BenchmarkLifecycleNoncurrentVersions(b *testing.B) {
	days30 := int64(30)
	id := "expire-old-versions"

	lc := &Lifecycle{
		Rules: []LifecycleRule{
			{
				ID:     &id,
				Status: LifecycleStatusEnabled,
				NoncurrentVersionExpiration: &LifecycleNoncurrentVersionExpiration{
					Days: &days30,
				},
			},
		},
	}

	evaluator := NewEvaluator(lc)
	now := time.Now().UTC()
	obj := ObjectState{
		Name:             "documents/report.pdf",
		Size:             1024,
		ModTime:          now.AddDate(0, -6, 0),
		IsLatest:         false,
		VersionID:        "version-123",
		SuccessorModTime: now.AddDate(0, -2, 0), // Became noncurrent 2 months ago
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		evaluator.Eval(obj, now)
	}
}

// BenchmarkLifecycleNoMatch benchmarks evaluation when no rules match
func BenchmarkLifecycleNoMatch(b *testing.B) {
	prefix := "archived/"
	days90 := int64(90)

	lc := &Lifecycle{
		Rules: []LifecycleRule{
			{
				Status: LifecycleStatusEnabled,
				Filter: &LifecycleFilter{
					Prefix: &prefix,
				},
				Expiration: &LifecycleExpiration{
					Days: &days90,
				},
			},
		},
	}

	evaluator := NewEvaluator(lc)
	now := time.Now().UTC()
	obj := ObjectState{
		Name:     "current/active/document.txt", // Doesn't match "archived/" prefix
		Size:     1024,
		ModTime:  now.AddDate(0, -1, 0),
		IsLatest: true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		evaluator.Eval(obj, now)
	}
}

// BenchmarkLifecycleTagParsing benchmarks tag parsing during evaluation
func BenchmarkLifecycleTagParsing(b *testing.B) {
	days90 := int64(90)

	lc := &Lifecycle{
		Rules: []LifecycleRule{
			{
				Status: LifecycleStatusEnabled,
				Filter: &LifecycleFilter{
					Tag: &Tag{Key: "status", Value: "archived"},
				},
				Expiration: &LifecycleExpiration{
					Days: &days90,
				},
			},
		},
	}

	evaluator := NewEvaluator(lc)
	now := time.Now().UTC()
	obj := ObjectState{
		Name:     "documents/old-file.pdf",
		Size:     1024,
		ModTime:  now.AddDate(0, -6, 0),
		IsLatest: true,
		UserTags: "environment=production&status=archived&owner=engineering&project=alpha&version=1.0",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		evaluator.Eval(obj, now)
	}
}

// BenchmarkHasActiveRules benchmarks the HasActiveRules check
func BenchmarkHasActiveRules(b *testing.B) {
	prefix1 := "logs/"
	prefix2 := "archive/"
	prefix3 := "temp/"
	days90 := int64(90)

	lc := &Lifecycle{
		Rules: []LifecycleRule{
			{Status: LifecycleStatusEnabled, Filter: &LifecycleFilter{Prefix: &prefix1}, Expiration: &LifecycleExpiration{Days: &days90}},
			{Status: LifecycleStatusEnabled, Filter: &LifecycleFilter{Prefix: &prefix2}, Expiration: &LifecycleExpiration{Days: &days90}},
			{Status: LifecycleStatusEnabled, Filter: &LifecycleFilter{Prefix: &prefix3}, Expiration: &LifecycleExpiration{Days: &days90}},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		lc.HasActiveRules("logs/2024/")
	}
}

// BenchmarkParallelLifecycleEval benchmarks concurrent lifecycle evaluation
func BenchmarkParallelLifecycleEval(b *testing.B) {
	prefix := "data/"
	days90 := int64(90)

	lc := &Lifecycle{
		Rules: []LifecycleRule{
			{
				Status:     LifecycleStatusEnabled,
				Filter:     &LifecycleFilter{Prefix: &prefix},
				Expiration: &LifecycleExpiration{Days: &days90},
			},
		},
	}

	evaluator := NewEvaluator(lc)
	now := time.Now().UTC()
	obj := ObjectState{
		Name:     "data/file.txt",
		Size:     1024,
		ModTime:  now.AddDate(0, -6, 0),
		IsLatest: true,
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			evaluator.Eval(obj, now)
		}
	})
}
