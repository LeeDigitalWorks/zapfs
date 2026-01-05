// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Encoder/decoder pools for algorithms that benefit from reuse
var (
	zstdEncoderPool sync.Pool
	zstdDecoderPool sync.Pool
	lz4WriterPool   sync.Pool
)

func init() {
	zstdEncoderPool = sync.Pool{
		New: func() interface{} {
			enc, _ := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.SpeedDefault),
				zstd.WithEncoderConcurrency(1),
			)
			return enc
		},
	}
	zstdDecoderPool = sync.Pool{
		New: func() interface{} {
			dec, _ := zstd.NewReader(nil,
				zstd.WithDecoderConcurrency(1),
			)
			return dec
		},
	}
	lz4WriterPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewWriter(nil)
		},
	}
}

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
	case Snappy:
		return compressSnappy(data)
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
	case Snappy:
		return decompressSnappy(data)
	default:
		return data, nil
	}
}

// compressLZ4 compresses data using LZ4 block format
func compressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(lz4.CompressBlockBound(len(data)))

	w := lz4WriterPool.Get().(*lz4.Writer)
	w.Reset(&buf)
	defer func() {
		w.Reset(nil)
		lz4WriterPool.Put(w)
	}()

	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("lz4 write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("lz4 close: %w", err)
	}

	return buf.Bytes(), nil
}

// decompressLZ4 decompresses LZ4 compressed data
func decompressLZ4(data []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	decompressed, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("lz4 decompress: %w", err)
	}
	return decompressed, nil
}

// compressZSTD compresses data using Zstandard
func compressZSTD(data []byte) ([]byte, error) {
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	defer zstdEncoderPool.Put(enc)

	return enc.EncodeAll(data, nil), nil
}

// decompressZSTD decompresses Zstandard compressed data
func decompressZSTD(data []byte) ([]byte, error) {
	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(dec)

	decompressed, err := dec.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress: %w", err)
	}
	return decompressed, nil
}

// compressSnappy compresses data using Snappy
func compressSnappy(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

// decompressSnappy decompresses Snappy compressed data
func decompressSnappy(data []byte) ([]byte, error) {
	decompressed, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("snappy decompress: %w", err)
	}
	return decompressed, nil
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
