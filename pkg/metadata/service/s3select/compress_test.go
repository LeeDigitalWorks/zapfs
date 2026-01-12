// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testData = []byte("Hello, World! This is test data for compression testing.")

func TestWrapReader_None(t *testing.T) {
	// Test with empty compression type
	r, err := WrapReader(bytes.NewReader(testData), "")
	require.NoError(t, err)
	defer r.Close()

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestWrapReader_NoneExplicit(t *testing.T) {
	// Test with explicit NONE compression type
	r, err := WrapReader(bytes.NewReader(testData), "NONE")
	require.NoError(t, err)
	defer r.Close()

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestWrapReader_Gzip(t *testing.T) {
	// Compress test data
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(testData)
	require.NoError(t, err)
	require.NoError(t, gw.Close())

	// Decompress using WrapReader
	r, err := WrapReader(bytes.NewReader(buf.Bytes()), "GZIP")
	require.NoError(t, err)
	defer r.Close()

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestWrapReader_Bzip2(t *testing.T) {
	// bzip2 doesn't have a standard Go writer, so we use pre-compressed data
	// Generated using: echo -n "Hello" | bzip2 -9 | xxd -i
	bzip2Data := []byte{
		0x42, 0x5a, 0x68, 0x39, 0x31, 0x41, 0x59, 0x26, 0x53, 0x59, 0x1a, 0x54,
		0x64, 0x92, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x40, 0x02, 0x04, 0xa0,
		0x00, 0x21, 0x9a, 0x68, 0x33, 0x4d, 0x13, 0x33, 0x8b, 0xb9, 0x22, 0x9c,
		0x28, 0x48, 0x0d, 0x2a, 0x32, 0x49, 0x00,
	}
	expected := []byte("Hello")

	r, err := WrapReader(bytes.NewReader(bzip2Data), "BZIP2")
	require.NoError(t, err)
	defer r.Close()

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, expected, data)
}

func TestWrapReader_Zstd(t *testing.T) {
	// Compress test data
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = zw.Write(testData)
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	// Decompress using WrapReader
	r, err := WrapReader(bytes.NewReader(buf.Bytes()), "ZSTD")
	require.NoError(t, err)
	defer r.Close()

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestWrapReader_LZ4(t *testing.T) {
	// Compress test data
	var buf bytes.Buffer
	lw := lz4.NewWriter(&buf)
	_, err := lw.Write(testData)
	require.NoError(t, err)
	require.NoError(t, lw.Close())

	// Decompress using WrapReader
	r, err := WrapReader(bytes.NewReader(buf.Bytes()), "LZ4")
	require.NoError(t, err)
	defer r.Close()

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestWrapReader_S2(t *testing.T) {
	// Compress test data
	var buf bytes.Buffer
	sw := s2.NewWriter(&buf)
	_, err := sw.Write(testData)
	require.NoError(t, err)
	require.NoError(t, sw.Close())

	// Decompress using WrapReader
	r, err := WrapReader(bytes.NewReader(buf.Bytes()), "S2")
	require.NoError(t, err)
	defer r.Close()

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestWrapReader_Snappy(t *testing.T) {
	// Compress test data using snappy stream format
	var buf bytes.Buffer
	sw := snappy.NewBufferedWriter(&buf)
	_, err := sw.Write(testData)
	require.NoError(t, err)
	require.NoError(t, sw.Close())

	// Decompress using WrapReader
	r, err := WrapReader(bytes.NewReader(buf.Bytes()), "SNAPPY")
	require.NoError(t, err)
	defer r.Close()

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestWrapReader_InvalidCompressionType(t *testing.T) {
	_, err := WrapReader(bytes.NewReader(testData), "INVALID")
	require.Error(t, err)

	selectErr, ok := err.(*SelectError)
	require.True(t, ok, "expected *SelectError, got %T", err)
	assert.Equal(t, "InvalidCompressionFormat", selectErr.Code)
	assert.Contains(t, selectErr.Message, "unsupported compression type")
}

func TestWrapReader_CaseInsensitive(t *testing.T) {
	testCases := []struct {
		name            string
		compressionType string
		compress        func([]byte) []byte
	}{
		{
			name:            "gzip lowercase",
			compressionType: "gzip",
			compress: func(data []byte) []byte {
				var buf bytes.Buffer
				gw := gzip.NewWriter(&buf)
				gw.Write(data)
				gw.Close()
				return buf.Bytes()
			},
		},
		{
			name:            "Gzip mixed case",
			compressionType: "Gzip",
			compress: func(data []byte) []byte {
				var buf bytes.Buffer
				gw := gzip.NewWriter(&buf)
				gw.Write(data)
				gw.Close()
				return buf.Bytes()
			},
		},
		{
			name:            "zstd lowercase",
			compressionType: "zstd",
			compress: func(data []byte) []byte {
				var buf bytes.Buffer
				zw, _ := zstd.NewWriter(&buf)
				zw.Write(data)
				zw.Close()
				return buf.Bytes()
			},
		},
		{
			name:            "lz4 lowercase",
			compressionType: "lz4",
			compress: func(data []byte) []byte {
				var buf bytes.Buffer
				lw := lz4.NewWriter(&buf)
				lw.Write(data)
				lw.Close()
				return buf.Bytes()
			},
		},
		{
			name:            "s2 lowercase",
			compressionType: "s2",
			compress: func(data []byte) []byte {
				var buf bytes.Buffer
				sw := s2.NewWriter(&buf)
				sw.Write(data)
				sw.Close()
				return buf.Bytes()
			},
		},
		{
			name:            "snappy lowercase",
			compressionType: "snappy",
			compress: func(data []byte) []byte {
				var buf bytes.Buffer
				sw := snappy.NewBufferedWriter(&buf)
				sw.Write(data)
				sw.Close()
				return buf.Bytes()
			},
		},
		{
			name:            "none lowercase",
			compressionType: "none",
			compress: func(data []byte) []byte {
				return data
			},
		},
		{
			name:            "NONE with whitespace",
			compressionType: "  NONE  ",
			compress: func(data []byte) []byte {
				return data
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressed := tc.compress(testData)

			r, err := WrapReader(bytes.NewReader(compressed), tc.compressionType)
			require.NoError(t, err)
			defer r.Close()

			data, err := io.ReadAll(r)
			require.NoError(t, err)
			assert.Equal(t, testData, data)
		})
	}
}

func TestWrapReader_InvalidGzipData(t *testing.T) {
	// Invalid gzip data should return an error
	_, err := WrapReader(bytes.NewReader([]byte("not gzip data")), "GZIP")
	require.Error(t, err)

	selectErr, ok := err.(*SelectError)
	require.True(t, ok, "expected *SelectError, got %T", err)
	assert.Equal(t, "InvalidCompressionFormat", selectErr.Code)
}

func TestWrapReader_ClosesUnderlyingReader(t *testing.T) {
	// Test that Close() closes the underlying reader
	var closed bool
	underlying := &closeTracker{
		Reader:   bytes.NewReader(testData),
		onClose:  func() { closed = true },
		closable: true,
	}

	r, err := WrapReader(underlying, "NONE")
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)
	assert.True(t, closed, "underlying reader should be closed")
}

func TestWrapReader_ClosesUnderlyingReaderWithCompression(t *testing.T) {
	// Test with gzip compression
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write(testData)
	gw.Close()

	var closed bool
	underlying := &closeTracker{
		Reader:   bytes.NewReader(buf.Bytes()),
		onClose:  func() { closed = true },
		closable: true,
	}

	r, err := WrapReader(underlying, "GZIP")
	require.NoError(t, err)

	// Read all data first
	_, err = io.ReadAll(r)
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)
	assert.True(t, closed, "underlying reader should be closed")
}

// closeTracker tracks whether Close was called
type closeTracker struct {
	io.Reader
	onClose  func()
	closable bool
}

func (c *closeTracker) Close() error {
	if c.closable {
		c.onClose()
	}
	return nil
}
