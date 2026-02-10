// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWildcardMatch(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		expect  bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"foo", "foo", true},
		{"foo", "bar", false},
		{"foo*", "foobar", true},
		{"foo*", "foo", true},
		{"*bar", "foobar", true},
		{"f?o", "foo", true},
		{"f?o", "fao", true},
		{"f?o", "fo", false},
		{"f*o", "fo", true},
		{"f*o", "fxxxo", true},
		{"f*o", "fxxxx", false},
		{"a*b*c", "aXXbYYc", true},
		{"a*b*c", "aXXbYY", false},
		{"arn:aws:s3:::*", "arn:aws:s3:::my-bucket", true},
		{"arn:aws:s3:::my-*", "arn:aws:s3:::my-bucket", true},
		{"arn:aws:s3:::my-*", "arn:aws:s3:::other-bucket", false},
	}
	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.value, func(t *testing.T) {
			assert.Equal(t, tt.expect, wildcardMatch(tt.pattern, tt.value))
		})
	}
}

func TestWildcardMatch_NoReDoS(t *testing.T) {
	// This pattern causes O(2^n) backtracking with naive recursive implementation
	pattern := strings.Repeat("*", 20) + "b"
	value := strings.Repeat("a", 25)

	done := make(chan bool, 1)
	go func() {
		_ = wildcardMatch(pattern, value)
		done <- true
	}()

	select {
	case <-done:
		// OK
	case <-time.After(1 * time.Second):
		t.Fatal("wildcardMatch took >1s — ReDoS vulnerability")
	}
}
