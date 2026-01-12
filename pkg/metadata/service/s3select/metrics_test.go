// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetInputFormat(t *testing.T) {
	assert.Equal(t, "csv", GetInputFormat(true, false, false))
	assert.Equal(t, "json", GetInputFormat(false, true, false))
	assert.Equal(t, "parquet", GetInputFormat(false, false, true))
	assert.Equal(t, "unknown", GetInputFormat(false, false, false))
}

func TestRecordRequest(t *testing.T) {
	// Test success case - verify it doesn't panic
	RecordRequest("csv", time.Second, 1000, 500, 10, nil)

	// Test error case - verify it doesn't panic
	RecordRequest("json", time.Millisecond*500, 2000, 100, 5, errors.New("test error"))
}

func TestRecordError(t *testing.T) {
	// Just verify it doesn't panic
	RecordError("json", "InvalidQuery")
	RecordError("csv", "InternalError")
	RecordError("parquet", "UnsupportedFormat")
}
