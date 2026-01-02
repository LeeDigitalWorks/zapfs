// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package signature

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test configuration for chunk reader tests
const (
	chunkTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	chunkTestRegion    = "us-east-1"
	chunkTestService   = "s3"
	chunkTestTimestamp = "20231215T000000Z"
	chunkTestDate      = "20231215"
)

// deriveChunkTestSigningKey creates a signing key for testing
func deriveChunkTestSigningKey() []byte {
	kDate := hmacSHA256([]byte("AWS4"+chunkTestSecretKey), []byte(chunkTestDate))
	kRegion := hmacSHA256(kDate, []byte(chunkTestRegion))
	kService := hmacSHA256(kRegion, []byte(chunkTestService))
	return hmacSHA256(kService, []byte("aws4_request"))
}

// calculateTestChunkSignature calculates a chunk signature for testing
func calculateTestChunkSignature(signingKey []byte, timestamp, credScope, prevSig string, chunkData []byte) string {
	// Hash chunk data
	h := sha256.New()
	h.Write(chunkData)
	chunkHash := hex.EncodeToString(h.Sum(nil))

	// Build string to sign
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		timestamp,
		credScope,
		prevSig,
		HashedEmptyPayload,
		chunkHash,
	}, "\n")

	// Calculate signature
	sig := hmac.New(sha256.New, signingKey)
	sig.Write([]byte(stringToSign))
	return hex.EncodeToString(sig.Sum(nil))
}

// buildChunkedBody creates a properly formatted chunked body for testing
func buildChunkedBody(signingKey []byte, seedSig string, chunks [][]byte) string {
	var buf bytes.Buffer
	timestamp := chunkTestTimestamp
	credScope := chunkTestDate + "/" + chunkTestRegion + "/" + chunkTestService + "/aws4_request"
	prevSig := seedSig

	for _, chunk := range chunks {
		// Calculate signature for this chunk
		chunkSig := calculateTestChunkSignature(signingKey, timestamp, credScope, prevSig, chunk)

		// Write chunk header
		buf.WriteString(fmt.Sprintf("%x;chunk-signature=%s\r\n", len(chunk), chunkSig))
		// Write chunk data
		buf.Write(chunk)
		buf.WriteString("\r\n")

		prevSig = chunkSig
	}

	// Write final chunk (size 0)
	finalSig := calculateTestChunkSignature(signingKey, timestamp, credScope, prevSig, []byte{})
	buf.WriteString(fmt.Sprintf("0;chunk-signature=%s\r\n\r\n", finalSig))

	return buf.String()
}

func TestChunkReader_SingleChunk(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunkData := []byte("Hello, World!")

	body := buildChunkedBody(signingKey, seedSig, [][]byte{chunkData})

	reader := NewChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	result, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, chunkData, result)
}

func TestChunkReader_MultipleChunks(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunks := [][]byte{
		[]byte("First chunk data"),
		[]byte("Second chunk data"),
		[]byte("Third chunk data"),
	}

	body := buildChunkedBody(signingKey, seedSig, chunks)

	reader := NewChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	result, err := io.ReadAll(reader)
	require.NoError(t, err)

	expected := bytes.Join(chunks, nil)
	assert.Equal(t, expected, result)
}

func TestChunkReader_EmptyChunks(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"

	// Just the final chunk (no data)
	body := buildChunkedBody(signingKey, seedSig, [][]byte{})

	reader := NewChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	result, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestVerifyingChunkReader_SingleChunk(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunkData := []byte("Hello, World!")

	body := buildChunkedBody(signingKey, seedSig, [][]byte{chunkData})

	reader := NewVerifyingChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	result, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, chunkData, result)
}

func TestVerifyingChunkReader_MultipleChunks(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunks := [][]byte{
		[]byte("First chunk data with some content"),
		[]byte("Second chunk data with different content"),
		[]byte("Third and final chunk"),
	}

	body := buildChunkedBody(signingKey, seedSig, chunks)

	reader := NewVerifyingChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	result, err := io.ReadAll(reader)
	require.NoError(t, err)

	expected := bytes.Join(chunks, nil)
	assert.Equal(t, expected, result)
}

func TestVerifyingChunkReader_InvalidSignature(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"

	// Create a body with an invalid signature
	body := "d;chunk-signature=invalidsignature0000000000000000000000000000000000000000\r\n" +
		"Hello, World!\r\n" +
		"0;chunk-signature=anotherbadsig00000000000000000000000000000000000000000000\r\n\r\n"

	reader := NewVerifyingChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	_, err := io.ReadAll(reader)
	assert.ErrorIs(t, err, ErrChunkSignatureMismatch)
}

func TestChunkReader_InvalidChunkFormat(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "missing semicolon",
			body: "d\r\nHello, World!\r\n",
		},
		{
			name: "missing chunk-signature prefix",
			body: "d;signature=abc\r\nHello, World!\r\n",
		},
		{
			name: "invalid hex size",
			body: "xyz;chunk-signature=abc\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signingKey := deriveChunkTestSigningKey()
			reader := NewChunkReader(ChunkReaderConfig{
				Body:          strings.NewReader(tt.body),
				SigningKey:    signingKey,
				SeedSignature: "seed",
				Timestamp:     chunkTestTimestamp,
				Region:        chunkTestRegion,
				Service:       chunkTestService,
			})

			_, err := io.ReadAll(reader)
			assert.Error(t, err)
		})
	}
}

func TestChunkReader_ChunkTooLarge(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()

	// Claim a chunk size larger than MaxChunkSize
	body := "10000000;chunk-signature=abc\r\n" // 256MB in hex

	reader := NewChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: "seed",
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	_, err := io.ReadAll(reader)
	assert.ErrorIs(t, err, ErrChunkTooLarge)
}

func TestVerifyingChunkReader_SmallReads(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunkData := []byte("Hello, World! This is a longer chunk of data for testing small reads.")

	body := buildChunkedBody(signingKey, seedSig, [][]byte{chunkData})

	reader := NewVerifyingChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	// Read in small chunks
	var result []byte
	buf := make([]byte, 5)
	for {
		n, err := reader.Read(buf)
		result = append(result, buf[:n]...)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, chunkData, result)
}

func TestChunkReaderConfig(t *testing.T) {
	cfg := ChunkReaderConfig{
		Body:          strings.NewReader("test"),
		SigningKey:    []byte("key"),
		SeedSignature: "seed",
		Timestamp:     "20231215T000000Z",
		Region:        "us-east-1",
		Service:       "s3",
	}

	reader := NewChunkReader(cfg)
	assert.NotNil(t, reader)
	assert.Equal(t, cfg.SigningKey, reader.signingKey)
	assert.Equal(t, cfg.SeedSignature, reader.prevSig)
	assert.Equal(t, cfg.Timestamp, reader.timestamp)
	assert.Equal(t, cfg.Region, reader.region)
	assert.Equal(t, cfg.Service, reader.service)
}

func BenchmarkVerifyingChunkReader(b *testing.B) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"

	// Create a 1MB chunk
	chunkData := make([]byte, 1024*1024)
	for i := range chunkData {
		chunkData[i] = byte(i % 256)
	}

	body := buildChunkedBody(signingKey, seedSig, [][]byte{chunkData})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := NewVerifyingChunkReader(ChunkReaderConfig{
			Body:          strings.NewReader(body),
			SigningKey:    signingKey,
			SeedSignature: seedSig,
			Timestamp:     chunkTestTimestamp,
			Region:        chunkTestRegion,
			Service:       chunkTestService,
		})

		_, _ = io.ReadAll(reader)
	}
}

// calculateTestTrailerSignature calculates a trailer signature for testing
func calculateTestTrailerSignature(signingKey []byte, timestamp, credScope, prevSig string, trailerHeaders []string) string {
	// Sort headers alphabetically
	sortedHeaders := make([]string, len(trailerHeaders))
	copy(sortedHeaders, trailerHeaders)
	for i := 0; i < len(sortedHeaders)-1; i++ {
		for j := i + 1; j < len(sortedHeaders); j++ {
			if sortedHeaders[i] > sortedHeaders[j] {
				sortedHeaders[i], sortedHeaders[j] = sortedHeaders[j], sortedHeaders[i]
			}
		}
	}

	// Build trailer string with newlines
	var trailerStr strings.Builder
	for _, h := range sortedHeaders {
		trailerStr.WriteString(h)
		trailerStr.WriteString("\n")
	}

	// Hash the trailing headers
	h := sha256.New()
	h.Write([]byte(trailerStr.String()))
	trailerHash := hex.EncodeToString(h.Sum(nil))

	// Build string to sign
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-TRAILER",
		timestamp,
		credScope,
		prevSig,
		trailerHash,
	}, "\n")

	// Calculate signature
	sig := hmac.New(sha256.New, signingKey)
	sig.Write([]byte(stringToSign))
	return hex.EncodeToString(sig.Sum(nil))
}

// buildChunkedBodyWithTrailers creates a properly formatted chunked body with trailers
func buildChunkedBodyWithTrailers(signingKey []byte, seedSig string, chunks [][]byte, trailers map[string]string) string {
	var buf bytes.Buffer
	timestamp := chunkTestTimestamp
	credScope := chunkTestDate + "/" + chunkTestRegion + "/" + chunkTestService + "/aws4_request"
	prevSig := seedSig

	// Write data chunks
	for _, chunk := range chunks {
		chunkSig := calculateTestChunkSignature(signingKey, timestamp, credScope, prevSig, chunk)
		buf.WriteString(fmt.Sprintf("%x;chunk-signature=%s\r\n", len(chunk), chunkSig))
		buf.Write(chunk)
		buf.WriteString("\r\n")
		prevSig = chunkSig
	}

	// Write final chunk (size 0)
	finalSig := calculateTestChunkSignature(signingKey, timestamp, credScope, prevSig, []byte{})
	buf.WriteString(fmt.Sprintf("0;chunk-signature=%s\r\n", finalSig))
	prevSig = finalSig

	// Build trailer headers list for signature
	var trailerHeaders []string
	for name, value := range trailers {
		trailerHeaders = append(trailerHeaders, name+":"+value)
	}

	// Write trailers
	for name, value := range trailers {
		buf.WriteString(name + ":" + value + "\r\n")
	}

	// Write trailer signature
	if len(trailerHeaders) > 0 {
		trailerSig := calculateTestTrailerSignature(signingKey, timestamp, credScope, prevSig, trailerHeaders)
		buf.WriteString("x-amz-trailer-signature:" + trailerSig + "\r\n")
	}

	// Final empty line
	buf.WriteString("\r\n")

	return buf.String()
}

func TestTrailerChunkReader_SingleChunkWithChecksum(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunkData := []byte("Hello, World!")

	trailers := map[string]string{
		"x-amz-checksum-crc32c": "sOO8/Q==",
	}

	body := buildChunkedBodyWithTrailers(signingKey, seedSig, [][]byte{chunkData}, trailers)

	reader := NewTrailerChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	result, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, chunkData, result)

	// Verify trailers were parsed
	assert.Equal(t, "sOO8/Q==", reader.GetTrailer("x-amz-checksum-crc32c"))
	assert.Equal(t, "sOO8/Q==", reader.Trailers()["x-amz-checksum-crc32c"])
}

func TestTrailerChunkReader_MultipleTrailers(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunkData := []byte("Test data with multiple trailers")

	trailers := map[string]string{
		"x-amz-checksum-sha256": "abc123hash==",
		"x-amz-checksum-crc32":  "xyz789==",
	}

	body := buildChunkedBodyWithTrailers(signingKey, seedSig, [][]byte{chunkData}, trailers)

	reader := NewTrailerChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	result, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, chunkData, result)

	// Verify both trailers were parsed
	assert.Equal(t, "abc123hash==", reader.GetTrailer("x-amz-checksum-sha256"))
	assert.Equal(t, "xyz789==", reader.GetTrailer("x-amz-checksum-crc32"))
}

func TestTrailerChunkReader_NoTrailers(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunkData := []byte("Data without trailers")

	// Use the non-trailer body builder
	body := buildChunkedBody(signingKey, seedSig, [][]byte{chunkData})

	reader := NewTrailerChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	result, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, chunkData, result)

	// Trailers should be empty but method should work
	assert.Empty(t, reader.Trailers())
	assert.Empty(t, reader.GetTrailer("x-amz-checksum-crc32c"))
}

func TestTrailerChunkReader_InvalidTrailerSignature(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunkData := []byte("Data")
	timestamp := chunkTestTimestamp
	credScope := chunkTestDate + "/" + chunkTestRegion + "/" + chunkTestService + "/aws4_request"

	// Build body with invalid trailer signature
	var buf bytes.Buffer
	prevSig := seedSig

	// Data chunk
	chunkSig := calculateTestChunkSignature(signingKey, timestamp, credScope, prevSig, chunkData)
	buf.WriteString(fmt.Sprintf("%x;chunk-signature=%s\r\n", len(chunkData), chunkSig))
	buf.Write(chunkData)
	buf.WriteString("\r\n")
	prevSig = chunkSig

	// Final chunk
	finalSig := calculateTestChunkSignature(signingKey, timestamp, credScope, prevSig, []byte{})
	buf.WriteString(fmt.Sprintf("0;chunk-signature=%s\r\n", finalSig))

	// Trailer with wrong signature
	buf.WriteString("x-amz-checksum-crc32c:sOO8/Q==\r\n")
	buf.WriteString("x-amz-trailer-signature:invalidsignature0000000000000000000000000000000000000000\r\n")
	buf.WriteString("\r\n")

	reader := NewTrailerChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(buf.String()),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	_, err := io.ReadAll(reader)
	assert.ErrorIs(t, err, ErrTrailerSignatureMismatch)
}

func TestTrailerChunkReader_TrailersWithoutSignature(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunkData := []byte("Data")
	timestamp := chunkTestTimestamp
	credScope := chunkTestDate + "/" + chunkTestRegion + "/" + chunkTestService + "/aws4_request"

	// Build body with trailers but no trailer signature
	var buf bytes.Buffer
	prevSig := seedSig

	// Data chunk
	chunkSig := calculateTestChunkSignature(signingKey, timestamp, credScope, prevSig, chunkData)
	buf.WriteString(fmt.Sprintf("%x;chunk-signature=%s\r\n", len(chunkData), chunkSig))
	buf.Write(chunkData)
	buf.WriteString("\r\n")
	prevSig = chunkSig

	// Final chunk
	finalSig := calculateTestChunkSignature(signingKey, timestamp, credScope, prevSig, []byte{})
	buf.WriteString(fmt.Sprintf("0;chunk-signature=%s\r\n", finalSig))

	// Trailer without signature
	buf.WriteString("x-amz-checksum-crc32c:sOO8/Q==\r\n")
	buf.WriteString("\r\n")

	reader := NewTrailerChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(buf.String()),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	_, err := io.ReadAll(reader)
	assert.ErrorIs(t, err, ErrInvalidTrailerFormat)
}

func TestTrailerChunkReader_MultipleChunksWithTrailers(t *testing.T) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"
	chunks := [][]byte{
		[]byte("First chunk of data"),
		[]byte("Second chunk of data"),
		[]byte("Third chunk"),
	}

	trailers := map[string]string{
		"x-amz-checksum-crc32c": "checksum123==",
	}

	body := buildChunkedBodyWithTrailers(signingKey, seedSig, chunks, trailers)

	reader := NewTrailerChunkReader(ChunkReaderConfig{
		Body:          strings.NewReader(body),
		SigningKey:    signingKey,
		SeedSignature: seedSig,
		Timestamp:     chunkTestTimestamp,
		Region:        chunkTestRegion,
		Service:       chunkTestService,
	})

	result, err := io.ReadAll(reader)
	require.NoError(t, err)

	expected := bytes.Join(chunks, nil)
	assert.Equal(t, expected, result)
	assert.Equal(t, "checksum123==", reader.GetTrailer("x-amz-checksum-crc32c"))
}

func BenchmarkTrailerChunkReader(b *testing.B) {
	signingKey := deriveChunkTestSigningKey()
	seedSig := "initialsignature0000000000000000000000000000000000000000000000000"

	// Create a 1MB chunk
	chunkData := make([]byte, 1024*1024)
	for i := range chunkData {
		chunkData[i] = byte(i % 256)
	}

	trailers := map[string]string{
		"x-amz-checksum-crc32c": "sOO8/Q==",
	}

	body := buildChunkedBodyWithTrailers(signingKey, seedSig, [][]byte{chunkData}, trailers)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := NewTrailerChunkReader(ChunkReaderConfig{
			Body:          strings.NewReader(body),
			SigningKey:    signingKey,
			SeedSignature: seedSig,
			Timestamp:     chunkTestTimestamp,
			Region:        chunkTestRegion,
			Service:       chunkTestService,
		})

		_, _ = io.ReadAll(reader)
	}
}
