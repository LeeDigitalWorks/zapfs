// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatchStringLike(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pattern  string
		value    string
		expected bool
	}{
		// Exact match cases
		{
			name:     "exact match",
			pattern:  "hello",
			value:    "hello",
			expected: true,
		},
		{
			name:     "exact match fail",
			pattern:  "hello",
			value:    "world",
			expected: false,
		},
		{
			name:     "empty pattern empty value",
			pattern:  "",
			value:    "",
			expected: true,
		},
		{
			name:     "empty pattern non-empty value",
			pattern:  "",
			value:    "hello",
			expected: false,
		},

		// Single * wildcard
		{
			name:     "wildcard * matches everything",
			pattern:  "*",
			value:    "anything",
			expected: true,
		},
		{
			name:     "wildcard * matches empty",
			pattern:  "*",
			value:    "",
			expected: true,
		},
		{
			name:     "prefix wildcard",
			pattern:  "*suffix",
			value:    "some-suffix",
			expected: true,
		},
		{
			name:     "prefix wildcard fail",
			pattern:  "*suffix",
			value:    "suffix-not",
			expected: false,
		},
		{
			name:     "suffix wildcard",
			pattern:  "prefix*",
			value:    "prefix-something",
			expected: true,
		},
		{
			name:     "suffix wildcard fail",
			pattern:  "prefix*",
			value:    "not-prefix",
			expected: false,
		},
		{
			name:     "middle wildcard",
			pattern:  "pre*suf",
			value:    "pre-middle-suf",
			expected: true,
		},
		{
			name:     "middle wildcard empty match",
			pattern:  "pre*suf",
			value:    "presuf",
			expected: true,
		},
		{
			name:     "middle wildcard fail",
			pattern:  "pre*suf",
			value:    "pre-middle-suffix",
			expected: false,
		},

		// Multiple * wildcards
		{
			name:     "multiple wildcards",
			pattern:  "a*b*c",
			value:    "a123b456c",
			expected: true,
		},
		{
			name:     "multiple wildcards empty matches",
			pattern:  "a*b*c",
			value:    "abc",
			expected: true,
		},
		{
			name:     "multiple wildcards fail",
			pattern:  "a*b*c",
			value:    "axyz",
			expected: false,
		},
		{
			name:     "complex multiple wildcards",
			pattern:  "*foo*bar*baz*",
			value:    "prefoo123bar456bazend",
			expected: true,
		},
		{
			name:     "consecutive wildcards",
			pattern:  "a**b",
			value:    "a123b",
			expected: true,
		},

		// ? single character wildcard
		{
			name:     "single ? matches one char",
			pattern:  "file?.txt",
			value:    "file1.txt",
			expected: true,
		},
		{
			name:     "single ? fails on empty",
			pattern:  "file?.txt",
			value:    "file.txt",
			expected: false,
		},
		{
			name:     "single ? fails on multiple chars",
			pattern:  "file?.txt",
			value:    "file10.txt",
			expected: false,
		},
		{
			name:     "multiple ?",
			pattern:  "file???.txt",
			value:    "file123.txt",
			expected: true,
		},
		{
			name:     "multiple ? wrong count",
			pattern:  "file???.txt",
			value:    "file12.txt",
			expected: false,
		},

		// Mixed * and ?
		{
			name:     "mixed * and ?",
			pattern:  "user-?-*",
			value:    "user-a-admin",
			expected: true,
		},
		{
			name:     "mixed * and ? fail",
			pattern:  "user-?-*",
			value:    "user-ab-admin",
			expected: false,
		},
		{
			name:     "? followed by *",
			pattern:  "?*",
			value:    "x",
			expected: true,
		},
		{
			name:     "? followed by * empty value",
			pattern:  "?*",
			value:    "",
			expected: false,
		},

		// Real-world IAM policy patterns
		{
			name:     "S3 ARN pattern",
			pattern:  "arn:aws:s3:::bucket/*",
			value:    "arn:aws:s3:::bucket/folder/file.txt",
			expected: true,
		},
		{
			name:     "S3 ARN pattern with prefix",
			pattern:  "arn:aws:s3:::bucket/logs/*",
			value:    "arn:aws:s3:::bucket/logs/2024/app.log",
			expected: true,
		},
		{
			name:     "IAM user ARN",
			pattern:  "arn:aws:iam::123456789012:user/*",
			value:    "arn:aws:iam::123456789012:user/johndoe",
			expected: true,
		},
		{
			name:     "Domain pattern",
			pattern:  "*.example.com",
			value:    "api.example.com",
			expected: true,
		},
		{
			name:     "Domain pattern fail",
			pattern:  "*.example.com",
			value:    "example.com",
			expected: false,
		},
		{
			name:     "Subdomain pattern",
			pattern:  "*.*.example.com",
			value:    "api.prod.example.com",
			expected: true,
		},
		{
			name:     "IP range-like pattern",
			pattern:  "10.0.*.*",
			value:    "10.0.1.100",
			expected: true,
		},
		{
			name:     "Version pattern",
			pattern:  "v?.?.?",
			value:    "v1.2.3",
			expected: true,
		},
		{
			name:     "Version pattern fail",
			pattern:  "v?.?.?",
			value:    "v10.2.3",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := matchStringLike(tc.pattern, tc.value)
			assert.Equal(t, tc.expected, result, "pattern=%q value=%q", tc.pattern, tc.value)
		})
	}
}

func TestMatchGlob(t *testing.T) {
	t.Parallel()

	// Additional edge cases for the underlying glob function
	tests := []struct {
		name     string
		pattern  string
		value    string
		expected bool
	}{
		{
			name:     "only wildcards",
			pattern:  "***",
			value:    "anything",
			expected: true,
		},
		{
			name:     "pattern longer than value",
			pattern:  "abcdef",
			value:    "abc",
			expected: false,
		},
		{
			name:     "value longer than pattern",
			pattern:  "abc",
			value:    "abcdef",
			expected: false,
		},
		{
			name:     "trailing star matches empty",
			pattern:  "abc*",
			value:    "abc",
			expected: true,
		},
		{
			name:     "leading star matches empty",
			pattern:  "*abc",
			value:    "abc",
			expected: true,
		},
		{
			name:     "star between same chars",
			pattern:  "a*a",
			value:    "aa",
			expected: true,
		},
		{
			name:     "star between same chars with content",
			pattern:  "a*a",
			value:    "aba",
			expected: true,
		},
		{
			name:     "multiple stars same chars",
			pattern:  "a*a*a",
			value:    "aXaYa",
			expected: true,
		},
		{
			name:     "question mark at end",
			pattern:  "abc?",
			value:    "abcd",
			expected: true,
		},
		{
			name:     "question mark at start",
			pattern:  "?abc",
			value:    "xabc",
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := matchGlob(tc.pattern, tc.value)
			assert.Equal(t, tc.expected, result, "pattern=%q value=%q", tc.pattern, tc.value)
		})
	}
}

func BenchmarkMatchGlob(b *testing.B) {
	patterns := []struct {
		name    string
		pattern string
		value   string
	}{
		{"simple", "hello", "hello"},
		{"prefix_star", "prefix*", "prefix-something-long"},
		{"suffix_star", "*suffix", "something-long-suffix"},
		{"middle_star", "pre*suf", "pre-middle-content-suf"},
		{"multiple_stars", "a*b*c*d", "a123b456c789d"},
		{"question_marks", "file???.txt", "file123.txt"},
		{"mixed", "user-?-*-admin", "user-a-very-long-value-admin"},
		{"s3_arn", "arn:aws:s3:::bucket/*", "arn:aws:s3:::bucket/folder/subfolder/file.txt"},
	}

	for _, p := range patterns {
		b.Run(p.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				matchStringLike(p.pattern, p.value)
			}
		})
	}
}
