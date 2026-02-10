// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsTruthy(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		expect bool
	}{
		{"true", true, true},
		{"false", false, false},
		{"int zero", int(0), false},
		{"int nonzero", int(42), true},
		{"int64 zero", int64(0), false},
		{"int64 nonzero", int64(42), true},
		{"int32 zero", int32(0), false},
		{"int32 nonzero", int32(1), true},
		{"float64 zero", float64(0), false},
		{"float64 nonzero", float64(3.14), true},
		{"empty string", "", false},
		{"nonempty string", "hello", true},
		{"nil", nil, false},
		{"uint64 zero", uint64(0), false},
		{"uint64 nonzero", uint64(1), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, isTruthy(tt.value))
		})
	}
}
