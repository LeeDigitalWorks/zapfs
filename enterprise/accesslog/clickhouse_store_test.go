// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

//go:build enterprise

package accesslog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaskDSN(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL-style DSN with password",
			input:    "clickhouse://user:secret123@localhost:9000/default",
			expected: "clickhouse://user:***@localhost:9000/default",
		},
		{
			name:     "URL-style DSN without password",
			input:    "clickhouse://user@localhost:9000/default",
			expected: "clickhouse://user@localhost:9000/default",
		},
		{
			name:     "TCP DSN with password query param",
			input:    "tcp://localhost:9000?username=user&password=secret123&database=default",
			expected: "tcp://localhost:9000?username=user&password=***&database=default",
		},
		{
			name:     "TCP DSN with password at end",
			input:    "tcp://localhost:9000?username=user&password=secret123",
			expected: "tcp://localhost:9000?username=user&password=***",
		},
		{
			name:     "DSN without password",
			input:    "tcp://localhost:9000?username=user&database=default",
			expected: "tcp://localhost:9000?username=user&database=default",
		},
		{
			name:     "Empty DSN",
			input:    "",
			expected: "",
		},
		{
			name:     "Complex password with special chars",
			input:    "clickhouse://admin:P@ss!w0rd@localhost:9000/db",
			expected: "clickhouse://admin:***@localhost:9000/db",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := maskDSN(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}
