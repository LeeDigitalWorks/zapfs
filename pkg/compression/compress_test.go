// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"bytes"
	"crypto/rand"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlgorithmIsValid(t *testing.T) {
	tests := []struct {
		algo  Algorithm
		valid bool
	}{
		{None, true},
		{LZ4, true},
		{ZSTD, true},
		{S2, true},
		{"", false},
		{"invalid", false},
		{"gzip", false},
		{"snappy", false}, // snappy no longer supported
	}

	for _, tt := range tests {
		t.Run(string(tt.algo), func(t *testing.T) {
			assert.Equal(t, tt.valid, tt.algo.IsValid())
		})
	}
}

func TestParseAlgorithm(t *testing.T) {
	tests := []struct {
		input    string
		expected Algorithm
	}{
		{"none", None},
		{"lz4", LZ4},
		{"zstd", ZSTD},
		{"s2", S2},
		{"", None},
		{"invalid", None},
		{"ZSTD", None},   // case sensitive
		{"snappy", None}, // snappy no longer supported
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, ParseAlgorithm(tt.input))
		})
	}
}

func TestCompressDecompressRoundTrip(t *testing.T) {
	// Test data that compresses well
	compressibleData := []byte(strings.Repeat("hello world this is compressible data ", 100))

	algorithms := []Algorithm{None, LZ4, ZSTD, S2}

	for _, algo := range algorithms {
		t.Run(algo.String(), func(t *testing.T) {
			compressed, err := Compress(algo, compressibleData)
			require.NoError(t, err)

			decompressed, err := Decompress(algo, compressed)
			require.NoError(t, err)

			assert.Equal(t, compressibleData, decompressed)

			// For non-None algorithms, verify compression actually happened
			if algo != None {
				t.Logf("%s: %d -> %d bytes (%.2fx)",
					algo, len(compressibleData), len(compressed),
					float64(len(compressibleData))/float64(len(compressed)))
				assert.Less(t, len(compressed), len(compressibleData),
					"compressed data should be smaller for compressible input")
			}
		})
	}
}

func TestCompressEmptyData(t *testing.T) {
	algorithms := []Algorithm{None, LZ4, ZSTD, S2}

	for _, algo := range algorithms {
		t.Run(algo.String(), func(t *testing.T) {
			compressed, err := Compress(algo, []byte{})
			require.NoError(t, err)

			decompressed, err := Decompress(algo, compressed)
			require.NoError(t, err)

			// Handle nil vs empty slice - both represent empty data
			assert.Empty(t, decompressed)
		})
	}
}

func TestCompressRandomData(t *testing.T) {
	// Random data typically doesn't compress well
	randomData := make([]byte, 4096)
	_, err := rand.Read(randomData)
	require.NoError(t, err)

	algorithms := []Algorithm{LZ4, ZSTD, S2}

	for _, algo := range algorithms {
		t.Run(algo.String(), func(t *testing.T) {
			compressed, err := Compress(algo, randomData)
			require.NoError(t, err)

			decompressed, err := Decompress(algo, compressed)
			require.NoError(t, err)

			assert.Equal(t, randomData, decompressed)

			// Random data may not compress well, but should still round-trip
			t.Logf("%s: %d -> %d bytes", algo, len(randomData), len(compressed))
		})
	}
}

func TestCompressIfBeneficial(t *testing.T) {
	// Compressible data
	compressibleData := []byte(strings.Repeat("compress me please ", 100))

	compressed, usedAlgo, err := CompressIfBeneficial(ZSTD, compressibleData)
	require.NoError(t, err)
	assert.Equal(t, ZSTD, usedAlgo)
	assert.Less(t, len(compressed), len(compressibleData))

	// Verify round-trip
	decompressed, err := Decompress(usedAlgo, compressed)
	require.NoError(t, err)
	assert.Equal(t, compressibleData, decompressed)
}

func TestCompressIfBeneficialSkipsIncompressible(t *testing.T) {
	// Random data that won't compress well
	randomData := make([]byte, 1024)
	_, err := rand.Read(randomData)
	require.NoError(t, err)

	result, usedAlgo, err := CompressIfBeneficial(ZSTD, randomData)
	require.NoError(t, err)

	// If compression didn't help, should return original data and None algorithm
	if usedAlgo == None {
		assert.Equal(t, randomData, result)
	} else {
		// If it did compress, verify round-trip
		decompressed, err := Decompress(usedAlgo, result)
		require.NoError(t, err)
		assert.Equal(t, randomData, decompressed)
	}
}

func TestCompressIfBeneficialNone(t *testing.T) {
	data := []byte("test data")

	result, usedAlgo, err := CompressIfBeneficial(None, data)
	require.NoError(t, err)
	assert.Equal(t, None, usedAlgo)
	assert.Equal(t, data, result)
}

func TestCompressionRatio(t *testing.T) {
	tests := []struct {
		original   int
		compressed int
		expected   float64
	}{
		{1000, 500, 2.0},
		{1000, 250, 4.0},
		{1000, 1000, 1.0}, // No compression
		{1000, 1100, 1.0}, // Expansion
		{1000, 0, 1.0},    // Zero compressed (edge case)
		{0, 0, 1.0},       // Both zero
	}

	for _, tt := range tests {
		ratio := CompressionRatio(tt.original, tt.compressed)
		assert.Equal(t, tt.expected, ratio)
	}
}

func TestDecompressInvalidData(t *testing.T) {
	invalidData := []byte("this is not compressed data")

	// Each algorithm should return an error for invalid compressed data
	algorithms := []Algorithm{LZ4, ZSTD, S2}

	for _, algo := range algorithms {
		t.Run(algo.String(), func(t *testing.T) {
			_, err := Decompress(algo, invalidData)
			assert.Error(t, err, "decompressing invalid data should fail")
		})
	}
}

func TestCompressLargeData(t *testing.T) {
	// 1MB of compressible data
	largeData := bytes.Repeat([]byte("large data block "), 65536)

	algorithms := []Algorithm{LZ4, ZSTD, S2}

	for _, algo := range algorithms {
		t.Run(algo.String(), func(t *testing.T) {
			compressed, err := Compress(algo, largeData)
			require.NoError(t, err)

			decompressed, err := Decompress(algo, compressed)
			require.NoError(t, err)

			assert.Equal(t, largeData, decompressed)

			ratio := float64(len(largeData)) / float64(len(compressed))
			t.Logf("%s: %d -> %d bytes (%.2fx compression)",
				algo, len(largeData), len(compressed), ratio)
		})
	}
}

// ============================================================================
// Streaming API Tests
// ============================================================================

func TestStreamingCompressDecompressRoundTrip(t *testing.T) {
	// Test data that compresses well
	compressibleData := []byte(strings.Repeat("streaming compression test data ", 1000))

	algorithms := []Algorithm{None, LZ4, ZSTD, S2}

	for _, algo := range algorithms {
		t.Run(algo.String(), func(t *testing.T) {
			// Compress using streaming API
			var compressedBuf bytes.Buffer
			compressWriter, err := CompressWriter(algo, &compressedBuf)
			require.NoError(t, err)

			_, err = compressWriter.Write(compressibleData)
			require.NoError(t, err)
			err = compressWriter.Close()
			require.NoError(t, err)

			compressed := compressedBuf.Bytes()

			// Decompress using streaming API
			decompressReader, err := DecompressReader(algo, bytes.NewReader(compressed))
			require.NoError(t, err)

			decompressed, err := io.ReadAll(decompressReader)
			require.NoError(t, err)
			err = decompressReader.Close()
			require.NoError(t, err)

			assert.Equal(t, compressibleData, decompressed)

			if algo != None {
				t.Logf("%s streaming: %d -> %d bytes (%.2fx)",
					algo, len(compressibleData), len(compressed),
					float64(len(compressibleData))/float64(len(compressed)))
			}
		})
	}
}

func TestStreamingCompressReader(t *testing.T) {
	data := []byte(strings.Repeat("test data for compress reader ", 500))

	algorithms := []Algorithm{LZ4, ZSTD, S2}

	for _, algo := range algorithms {
		t.Run(algo.String(), func(t *testing.T) {
			// Create a compress reader
			reader, err := CompressReader(algo, bytes.NewReader(data))
			require.NoError(t, err)

			// Read all compressed data
			compressed, err := io.ReadAll(reader)
			require.NoError(t, err)
			reader.Close()

			// Decompress using streaming API (stream format differs from block format)
			decompressReader, err := DecompressReader(algo, bytes.NewReader(compressed))
			require.NoError(t, err)

			decompressed, err := io.ReadAll(decompressReader)
			require.NoError(t, err)
			decompressReader.Close()

			assert.Equal(t, data, decompressed)
		})
	}
}

func TestStreamingLargeData(t *testing.T) {
	// 5MB of data - tests streaming with larger payloads
	largeData := bytes.Repeat([]byte("streaming large data test "), 200000)

	algorithms := []Algorithm{LZ4, ZSTD, S2}

	for _, algo := range algorithms {
		t.Run(algo.String(), func(t *testing.T) {
			// Compress
			var buf bytes.Buffer
			w, err := CompressWriter(algo, &buf)
			require.NoError(t, err)

			// Write in chunks to simulate streaming
			chunkSize := 64 * 1024
			for i := 0; i < len(largeData); i += chunkSize {
				end := min(i+chunkSize, len(largeData))
				_, err := w.Write(largeData[i:end])
				require.NoError(t, err)
			}
			require.NoError(t, w.Close())

			compressed := buf.Bytes()

			// Decompress
			r, err := DecompressReader(algo, bytes.NewReader(compressed))
			require.NoError(t, err)

			decompressed, err := io.ReadAll(r)
			require.NoError(t, err)
			require.NoError(t, r.Close())

			assert.Equal(t, largeData, decompressed)

			ratio := float64(len(largeData)) / float64(len(compressed))
			t.Logf("%s: %d -> %d bytes (%.2fx)", algo, len(largeData), len(compressed), ratio)
		})
	}
}

func TestStreamingEmptyData(t *testing.T) {
	algorithms := []Algorithm{None, LZ4, ZSTD, S2}

	for _, algo := range algorithms {
		t.Run(algo.String(), func(t *testing.T) {
			var buf bytes.Buffer
			w, err := CompressWriter(algo, &buf)
			require.NoError(t, err)
			require.NoError(t, w.Close())

			r, err := DecompressReader(algo, bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)

			data, err := io.ReadAll(r)
			require.NoError(t, err)
			require.NoError(t, r.Close())

			assert.Empty(t, data)
		})
	}
}

// ============================================================================
// S2 specific tests
// ============================================================================

func TestS2Compression(t *testing.T) {
	// Test S2 compression with various data sizes
	sizes := []int{0, 100, 1024, 64 * 1024, 1024 * 1024}

	for _, size := range sizes {
		t.Run(string(rune(size)), func(t *testing.T) {
			var data []byte
			if size > 0 {
				data = bytes.Repeat([]byte("s2 compression test "), size/20+1)[:size]
			}

			compressed, err := Compress(S2, data)
			require.NoError(t, err)

			decompressed, err := Decompress(S2, compressed)
			require.NoError(t, err)

			assert.Equal(t, data, decompressed)

			if size > 100 {
				t.Logf("S2: %d -> %d bytes (%.2fx)", len(data), len(compressed),
					float64(len(data))/float64(len(compressed)))
			}
		})
	}
}

// ============================================================================
// Benchmarks
// ============================================================================

func BenchmarkCompressLZ4(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for compression "), 1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for b.Loop() {
		_, _ = Compress(LZ4, data)
	}
}

func BenchmarkCompressZSTD(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for compression "), 1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for b.Loop() {
		_, _ = Compress(ZSTD, data)
	}
}

func BenchmarkCompressS2(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for compression "), 1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for b.Loop() {
		_, _ = Compress(S2, data)
	}
}

func BenchmarkDecompressLZ4(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for compression "), 1024)
	compressed, _ := Compress(LZ4, data)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for b.Loop() {
		_, _ = Decompress(LZ4, compressed)
	}
}

func BenchmarkDecompressZSTD(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for compression "), 1024)
	compressed, _ := Compress(ZSTD, data)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for b.Loop() {
		_, _ = Decompress(ZSTD, compressed)
	}
}

func BenchmarkDecompressS2(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for compression "), 1024)
	compressed, _ := Compress(S2, data)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for b.Loop() {
		_, _ = Decompress(S2, compressed)
	}
}

// Streaming benchmarks

func BenchmarkStreamingCompressLZ4(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for compression "), 1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for b.Loop() {
		var buf bytes.Buffer
		w, _ := CompressWriter(LZ4, &buf)
		w.Write(data)
		w.Close()
	}
}

func BenchmarkStreamingCompressZSTD(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for compression "), 1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for b.Loop() {
		var buf bytes.Buffer
		w, _ := CompressWriter(ZSTD, &buf)
		w.Write(data)
		w.Close()
	}
}

func BenchmarkStreamingCompressS2(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for compression "), 1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for b.Loop() {
		var buf bytes.Buffer
		w, _ := CompressWriter(S2, &buf)
		w.Write(data)
		w.Close()
	}
}
