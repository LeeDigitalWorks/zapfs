// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"io"
)

// Compress compresses data using the specified algorithm.
// Returns the original data unchanged if algo is None or empty.
func Compress(algo Algorithm, data []byte) ([]byte, error) {
	switch algo {
	case None, "":
		return data, nil
	case LZ4:
		return compressLZ4(data)
	case ZSTD:
		return compressZSTD(data)
	case S2:
		return compressS2(data)
	default:
		return data, nil
	}
}

// Decompress decompresses data using the specified algorithm.
// Returns the original data unchanged if algo is None or empty.
func Decompress(algo Algorithm, data []byte) ([]byte, error) {
	switch algo {
	case None, "":
		return data, nil
	case LZ4:
		return decompressLZ4(data)
	case ZSTD:
		return decompressZSTD(data)
	case S2:
		return decompressS2(data)
	default:
		return data, nil
	}
}

// CompressReader wraps a reader to compress data as it's read.
// The returned ReadCloser must be closed when done.
func CompressReader(algo Algorithm, r io.Reader) (io.ReadCloser, error) {
	switch algo {
	case None, "":
		return io.NopCloser(r), nil
	case LZ4:
		return newLZ4CompressReader(r), nil
	case ZSTD:
		return newZSTDCompressReader(r), nil
	case S2:
		return newS2CompressReader(r), nil
	default:
		return io.NopCloser(r), nil
	}
}

// DecompressReader wraps a reader to decompress data as it's read.
// The returned ReadCloser must be closed when done.
func DecompressReader(algo Algorithm, r io.Reader) (io.ReadCloser, error) {
	switch algo {
	case None, "":
		return io.NopCloser(r), nil
	case LZ4:
		return newLZ4DecompressReader(r), nil
	case ZSTD:
		return newZSTDDecompressReader(r)
	case S2:
		return newS2DecompressReader(r), nil
	default:
		return io.NopCloser(r), nil
	}
}

// CompressWriter wraps a writer to compress data as it's written.
// The returned WriteCloser must be closed when done to flush remaining data.
func CompressWriter(algo Algorithm, w io.Writer) (io.WriteCloser, error) {
	switch algo {
	case None, "":
		return &nopWriteCloser{w}, nil
	case LZ4:
		return newLZ4CompressWriter(w), nil
	case ZSTD:
		return newZSTDCompressWriter(w), nil
	case S2:
		return newS2CompressWriter(w), nil
	default:
		return &nopWriteCloser{w}, nil
	}
}

// CompressIfBeneficial compresses data and returns the compressed version
// only if it's smaller than the original. Otherwise returns the original data
// and None algorithm.
func CompressIfBeneficial(algo Algorithm, data []byte) ([]byte, Algorithm, error) {
	if algo == None || algo == "" {
		return data, None, nil
	}

	compressed, err := Compress(algo, data)
	if err != nil {
		return nil, None, err
	}

	// Only use compression if it actually saves space
	if len(compressed) >= len(data) {
		return data, None, nil
	}

	return compressed, algo, nil
}

// CompressionRatio calculates the compression ratio (original / compressed).
// Returns 1.0 if compressed size is zero or larger than original.
func CompressionRatio(originalSize, compressedSize int) float64 {
	if compressedSize <= 0 || compressedSize >= originalSize {
		return 1.0
	}
	return float64(originalSize) / float64(compressedSize)
}

// nopWriteCloser wraps a Writer to add a no-op Close method
type nopWriteCloser struct {
	io.Writer
}

func (w *nopWriteCloser) Close() error {
	return nil
}
