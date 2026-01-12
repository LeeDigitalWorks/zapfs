// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// CompressionType constants for supported compression formats.
const (
	CompressionNone   = "NONE"
	CompressionGzip   = "GZIP"
	CompressionBzip2  = "BZIP2"
	CompressionZstd   = "ZSTD"
	CompressionLZ4    = "LZ4"
	CompressionS2     = "S2"
	CompressionSnappy = "SNAPPY"
)

// WrapReader wraps the given reader with a decompression reader based on the
// compression type. The compression type is case-insensitive.
//
// Supported compression types:
//   - NONE (or empty): No decompression
//   - GZIP: Standard gzip compression
//   - BZIP2: Bzip2 compression
//   - ZSTD: Zstandard compression
//   - LZ4: LZ4 block compression
//   - S2: S2 compression (Snappy-compatible)
//   - SNAPPY: Snappy compression
//
// The returned ReadCloser must be closed by the caller. Closing it will also
// close the underlying reader if it implements io.Closer.
func WrapReader(r io.Reader, compressionType string) (io.ReadCloser, error) {
	// Normalize compression type to uppercase for case-insensitive matching
	ct := strings.ToUpper(strings.TrimSpace(compressionType))

	switch ct {
	case "", CompressionNone:
		return wrapNopCloser(r), nil

	case CompressionGzip:
		return wrapGzip(r)

	case CompressionBzip2:
		return wrapBzip2(r), nil

	case CompressionZstd:
		return wrapZstd(r)

	case CompressionLZ4:
		return wrapLZ4(r), nil

	case CompressionS2:
		return wrapS2(r), nil

	case CompressionSnappy:
		return wrapSnappy(r), nil

	default:
		return nil, &SelectError{
			Code:    "InvalidCompressionFormat",
			Message: fmt.Sprintf("unsupported compression type: %s", compressionType),
		}
	}
}

// wrapNopCloser wraps a reader with io.NopCloser, preserving any existing
// Close method on the underlying reader.
func wrapNopCloser(r io.Reader) io.ReadCloser {
	if rc, ok := r.(io.ReadCloser); ok {
		return rc
	}
	return io.NopCloser(r)
}

// wrapGzip creates a gzip decompression reader.
func wrapGzip(r io.Reader) (io.ReadCloser, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, &SelectError{
			Code:    "InvalidCompressionFormat",
			Message: fmt.Sprintf("failed to create gzip reader: %v", err),
		}
	}
	return &wrappedReader{Reader: gr, underlying: r}, nil
}

// wrapBzip2 creates a bzip2 decompression reader.
func wrapBzip2(r io.Reader) io.ReadCloser {
	br := bzip2.NewReader(r)
	return &wrappedReader{Reader: br, underlying: r}
}

// wrapZstd creates a zstd decompression reader.
func wrapZstd(r io.Reader) (io.ReadCloser, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, &SelectError{
			Code:    "InvalidCompressionFormat",
			Message: fmt.Sprintf("failed to create zstd reader: %v", err),
		}
	}
	return &zstdReader{decoder: zr, underlying: r}, nil
}

// wrapLZ4 creates an LZ4 decompression reader.
func wrapLZ4(r io.Reader) io.ReadCloser {
	lr := lz4.NewReader(r)
	return &wrappedReader{Reader: lr, underlying: r}
}

// wrapS2 creates an S2 decompression reader.
func wrapS2(r io.Reader) io.ReadCloser {
	sr := s2.NewReader(r)
	return &wrappedReader{Reader: sr, underlying: r}
}

// wrapSnappy creates a Snappy decompression reader.
func wrapSnappy(r io.Reader) io.ReadCloser {
	sr := snappy.NewReader(r)
	return &wrappedReader{Reader: sr, underlying: r}
}

// wrappedReader wraps a decompression reader and ensures the underlying
// reader is closed when Close is called.
type wrappedReader struct {
	io.Reader
	underlying io.Reader
}

func (w *wrappedReader) Close() error {
	// Close the decompression reader if it implements io.Closer
	if closer, ok := w.Reader.(io.Closer); ok {
		closer.Close()
	}
	// Close the underlying reader if it implements io.Closer
	if closer, ok := w.underlying.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// zstdReader is a specialized wrapper for zstd.Decoder which requires
// calling Close() to release resources.
type zstdReader struct {
	decoder    *zstd.Decoder
	underlying io.Reader
}

func (z *zstdReader) Read(p []byte) (n int, err error) {
	return z.decoder.Read(p)
}

func (z *zstdReader) Close() error {
	z.decoder.Close()
	if closer, ok := z.underlying.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
