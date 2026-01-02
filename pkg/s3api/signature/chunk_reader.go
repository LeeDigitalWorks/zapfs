// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package signature

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

// AWS chunked encoding format:
// <chunk-size-hex>;chunk-signature=<signature>\r\n
// <chunk-data>\r\n
// ... repeat ...
// 0;chunk-signature=<final-signature>\r\n
// \r\n
//
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html

var (
	ErrInvalidChunkFormat      = errors.New("invalid chunk format")
	ErrChunkSignatureMismatch  = errors.New("chunk signature mismatch")
	ErrChunkTooLarge           = errors.New("chunk size exceeds maximum")
	ErrTrailerSignatureMismatch = errors.New("trailer signature mismatch")
	ErrInvalidTrailerFormat    = errors.New("invalid trailer format")
)

const (
	// Maximum chunk size (64MB - same as AWS default)
	MaxChunkSize = 64 * 1024 * 1024

	// Chunk signature header prefix
	chunkSignaturePrefix = "chunk-signature="

	// Algorithm for chunk signing
	chunkSigningAlgorithm = "AWS4-HMAC-SHA256-PAYLOAD"

	// Algorithm for trailer signing
	trailerSigningAlgorithm = "AWS4-HMAC-SHA256-TRAILER"

	// Trailer signature header name
	trailerSignatureHeader = "x-amz-trailer-signature"
)

// ChunkReader wraps an io.Reader and parses AWS chunked transfer encoding.
// It implements io.Reader and returns only the actual data (stripping framing).
//
// NOTE: This basic reader does NOT verify chunk data signatures for performance.
// It only verifies the final (empty) chunk signature. For strict signature verification
// of all chunks, use VerifyingChunkReader instead.
//
// This reader is suitable for internal/trusted sources or when verification
// is handled at a different layer.
type ChunkReader struct {
	reader     *bufio.Reader
	signingKey []byte
	region     string
	service    string
	timestamp  string // ISO8601 format
	prevSig    string // Previous chunk signature (starts with seed signature)
	credScope  string // Credential scope for string-to-sign

	// Current chunk state
	chunkRemaining int64
	eof            bool
	err            error
}

// ChunkReaderConfig holds configuration for creating a ChunkReader
type ChunkReaderConfig struct {
	Body          io.Reader
	SigningKey    []byte // Derived signing key
	SeedSignature string // Initial request signature
	Timestamp     string // Request timestamp (ISO8601)
	Region        string
	Service       string
}

// NewChunkReader creates a new ChunkReader for verifying chunked uploads
func NewChunkReader(cfg ChunkReaderConfig) *ChunkReader {
	return &ChunkReader{
		reader:     bufio.NewReaderSize(cfg.Body, 64*1024), // 64KB buffer
		signingKey: cfg.SigningKey,
		prevSig:    cfg.SeedSignature,
		timestamp:  cfg.Timestamp,
		region:     cfg.Region,
		service:    cfg.Service,
		credScope:  fmt.Sprintf("%s/%s/%s/aws4_request", cfg.Timestamp[:8], cfg.Region, cfg.Service),
	}
}

// Read implements io.Reader, returning verified chunk data
func (c *ChunkReader) Read(p []byte) (n int, err error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.eof {
		return 0, io.EOF
	}

	// If we've exhausted current chunk, read next chunk header
	if c.chunkRemaining == 0 {
		if err := c.readChunkHeader(); err != nil {
			c.err = err
			return 0, err
		}

		// Check for final chunk (size 0)
		if c.chunkRemaining == 0 {
			c.eof = true
			// Read trailing CRLF after final chunk
			_, _ = c.reader.ReadString('\n')
			return 0, io.EOF
		}
	}

	// Read from current chunk
	toRead := min(int64(len(p)), c.chunkRemaining)

	n, err = c.reader.Read(p[:toRead])
	c.chunkRemaining -= int64(n)

	if err != nil && err != io.EOF {
		c.err = err
		return n, err
	}

	// If we've finished this chunk, consume the trailing CRLF
	if c.chunkRemaining == 0 {
		line, readErr := c.reader.ReadString('\n')
		if readErr != nil && readErr != io.EOF {
			c.err = readErr
			return n, readErr
		}
		// Should be just "\r\n"
		if strings.TrimSpace(line) != "" {
			c.err = ErrInvalidChunkFormat
			return n, c.err
		}
	}

	return n, nil
}

// readChunkHeader parses and verifies a chunk header
// Format: <hex-size>;chunk-signature=<signature>\r\n
func (c *ChunkReader) readChunkHeader() error {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading chunk header: %w", err)
	}

	// Remove trailing CRLF
	line = strings.TrimSuffix(line, "\r\n")
	line = strings.TrimSuffix(line, "\n")

	// Split by semicolon: size;chunk-signature=sig
	parts := strings.SplitN(line, ";", 2)
	if len(parts) != 2 {
		return fmt.Errorf("%w: missing chunk-signature", ErrInvalidChunkFormat)
	}

	// Parse chunk size (hex)
	chunkSize, err := strconv.ParseInt(parts[0], 16, 64)
	if err != nil {
		return fmt.Errorf("%w: invalid chunk size: %v", ErrInvalidChunkFormat, err)
	}

	if chunkSize > MaxChunkSize {
		return ErrChunkTooLarge
	}

	// Parse chunk signature
	sigPart := strings.TrimSpace(parts[1])
	if !strings.HasPrefix(sigPart, chunkSignaturePrefix) {
		return fmt.Errorf("%w: invalid signature prefix", ErrInvalidChunkFormat)
	}
	chunkSig := strings.TrimPrefix(sigPart, chunkSignaturePrefix)

	// Verify chunk signature
	if err := c.verifyChunkSignature(chunkSize, chunkSig); err != nil {
		return err
	}

	// Update state for next chunk
	c.prevSig = chunkSig
	c.chunkRemaining = chunkSize

	return nil
}

// verifyChunkSignature verifies the signature for a chunk.
// For the basic ChunkReader, only the final (empty) chunk is verified immediately.
// Non-empty chunks are not verified here - use VerifyingChunkReader for strict verification.
func (c *ChunkReader) verifyChunkSignature(chunkSize int64, providedSig string) error {
	// For final chunk (size 0), verify immediately
	if chunkSize == 0 {
		expectedSig := c.calculateChunkSignature([]byte{})
		if !constantTimeCompare(providedSig, expectedSig) {
			return ErrChunkSignatureMismatch
		}
		return nil
	}

	// Non-empty chunks: This basic reader does not verify chunk data signatures.
	// For strict verification that buffers and verifies before returning data,
	// use VerifyingChunkReader or TrailerChunkReader instead.
	return nil
}

// calculateChunkSignature calculates the expected signature for chunk data
func (c *ChunkReader) calculateChunkSignature(chunkData []byte) string {
	// Hash chunk data
	hasher := utils.Sha256PoolGetHasher()
	hasher.Write(chunkData)
	chunkHash := hex.EncodeToString(hasher.Sum(nil))
	utils.Sha256PoolPutHasher(hasher)

	// Build string to sign
	stringToSign := strings.Join([]string{
		chunkSigningAlgorithm,
		c.timestamp,
		c.credScope,
		c.prevSig,
		HashedEmptyPayload, // Empty headers hash
		chunkHash,
	}, "\n")

	// Calculate signature
	signature := hmacSHA256(c.signingKey, []byte(stringToSign))
	return hex.EncodeToString(signature)
}

// VerifyingChunkReader wraps ChunkReader and verifies each chunk's signature
// after reading its data. This provides strict verification at the cost of
// buffering chunk data.
type VerifyingChunkReader struct {
	*ChunkReader
	chunkBuffer bytes.Buffer
	chunkSig    string
}

// NewVerifyingChunkReader creates a ChunkReader with strict signature verification
func NewVerifyingChunkReader(cfg ChunkReaderConfig) *VerifyingChunkReader {
	return &VerifyingChunkReader{
		ChunkReader: NewChunkReader(cfg),
	}
}

// Read implements io.Reader with strict signature verification
func (v *VerifyingChunkReader) Read(p []byte) (n int, err error) {
	if v.err != nil {
		return 0, v.err
	}
	if v.eof {
		return 0, io.EOF
	}

	// If we have buffered verified data, serve from buffer first
	if v.chunkBuffer.Len() > 0 {
		return v.chunkBuffer.Read(p)
	}

	// Buffer is empty, read and verify next chunk
	if err := v.readAndBufferChunk(); err != nil {
		v.err = err
		return 0, err
	}
	if v.eof {
		return 0, io.EOF
	}

	return v.chunkBuffer.Read(p)
}

// readAndBufferChunk reads an entire chunk, verifies its signature, and buffers it
func (v *VerifyingChunkReader) readAndBufferChunk() error {
	// Read chunk header
	line, err := v.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading chunk header: %w", err)
	}

	line = strings.TrimSuffix(line, "\r\n")
	line = strings.TrimSuffix(line, "\n")

	parts := strings.SplitN(line, ";", 2)
	if len(parts) != 2 {
		return fmt.Errorf("%w: missing chunk-signature", ErrInvalidChunkFormat)
	}

	chunkSize, err := strconv.ParseInt(parts[0], 16, 64)
	if err != nil {
		return fmt.Errorf("%w: invalid chunk size", ErrInvalidChunkFormat)
	}

	if chunkSize > MaxChunkSize {
		return ErrChunkTooLarge
	}

	sigPart := strings.TrimSpace(parts[1])
	if !strings.HasPrefix(sigPart, chunkSignaturePrefix) {
		return fmt.Errorf("%w: invalid signature prefix", ErrInvalidChunkFormat)
	}
	v.chunkSig = strings.TrimPrefix(sigPart, chunkSignaturePrefix)

	// Handle final chunk
	if chunkSize == 0 {
		expectedSig := v.calculateChunkSignature([]byte{})
		if !constantTimeCompare(v.chunkSig, expectedSig) {
			return ErrChunkSignatureMismatch
		}
		v.eof = true
		_, _ = v.reader.ReadString('\n') // trailing CRLF
		return nil
	}

	// Read chunk data into buffer
	v.chunkBuffer.Reset()
	v.chunkBuffer.Grow(int(chunkSize))

	// Use pooled hash writer to compute hash while reading
	hasher := utils.Sha256PoolGetHasher()
	defer utils.Sha256PoolPutHasher(hasher)
	writer := io.MultiWriter(&v.chunkBuffer, hasher)

	n, err := io.CopyN(writer, v.reader, chunkSize)
	if err != nil {
		return fmt.Errorf("reading chunk data: %w", err)
	}
	if n != chunkSize {
		return fmt.Errorf("%w: incomplete chunk", ErrInvalidChunkFormat)
	}

	// Read trailing CRLF
	crlf := make([]byte, 2)
	if _, err := io.ReadFull(v.reader, crlf); err != nil {
		return fmt.Errorf("reading chunk trailer: %w", err)
	}

	// Verify chunk signature
	chunkHash := hex.EncodeToString(hasher.Sum(nil))
	stringToSign := strings.Join([]string{
		chunkSigningAlgorithm,
		v.timestamp,
		v.credScope,
		v.prevSig,
		HashedEmptyPayload,
		chunkHash,
	}, "\n")

	expectedSig := hex.EncodeToString(hmacSHA256(v.signingKey, []byte(stringToSign)))
	if !constantTimeCompare(v.chunkSig, expectedSig) {
		return ErrChunkSignatureMismatch
	}

	// Update previous signature for next chunk
	v.prevSig = v.chunkSig

	return nil
}

// TrailerChunkReader extends VerifyingChunkReader with support for trailing checksums.
// It parses and verifies trailer headers (e.g., x-amz-checksum-crc32c) and their signature.
//
// AWS chunked encoding with trailers format:
// <chunk-size-hex>;chunk-signature=<signature>\r\n
// <chunk-data>\r\n
// ... repeat ...
// 0;chunk-signature=<final-signature>\r\n
// x-amz-checksum-crc32c:<base64-checksum>\r\n
// x-amz-trailer-signature:<trailer-signature>\r\n
// \r\n
//
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
type TrailerChunkReader struct {
	*VerifyingChunkReader
	trailers       map[string]string // Parsed trailer headers (excluding signature)
	trailersRead   bool              // Whether trailers have been parsed
	trailerErr     error             // Error from trailer parsing/verification
}

// NewTrailerChunkReader creates a ChunkReader that supports trailing checksums
func NewTrailerChunkReader(cfg ChunkReaderConfig) *TrailerChunkReader {
	return &TrailerChunkReader{
		VerifyingChunkReader: NewVerifyingChunkReader(cfg),
		trailers:             make(map[string]string),
	}
}

// Read implements io.Reader, returning verified chunk data and parsing trailers at EOF
func (t *TrailerChunkReader) Read(p []byte) (n int, err error) {
	if t.trailerErr != nil {
		return 0, t.trailerErr
	}
	if t.err != nil {
		return 0, t.err
	}
	if t.eof {
		return 0, io.EOF
	}

	// If we have buffered verified data, serve from buffer first
	if t.chunkBuffer.Len() > 0 {
		return t.chunkBuffer.Read(p)
	}

	// Buffer is empty, read and verify next chunk
	if err := t.readAndBufferChunkWithTrailers(); err != nil {
		if err == io.EOF {
			return 0, io.EOF
		}
		t.err = err
		return 0, err
	}
	if t.eof {
		return 0, io.EOF
	}

	return t.chunkBuffer.Read(p)
}

// readAndBufferChunkWithTrailers reads a chunk and handles trailers at the end
func (t *TrailerChunkReader) readAndBufferChunkWithTrailers() error {
	// Read chunk header
	line, err := t.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading chunk header: %w", err)
	}

	line = strings.TrimSuffix(line, "\r\n")
	line = strings.TrimSuffix(line, "\n")

	parts := strings.SplitN(line, ";", 2)
	if len(parts) != 2 {
		return fmt.Errorf("%w: missing chunk-signature", ErrInvalidChunkFormat)
	}

	chunkSize, err := strconv.ParseInt(parts[0], 16, 64)
	if err != nil {
		return fmt.Errorf("%w: invalid chunk size", ErrInvalidChunkFormat)
	}

	if chunkSize > MaxChunkSize {
		return ErrChunkTooLarge
	}

	sigPart := strings.TrimSpace(parts[1])
	if !strings.HasPrefix(sigPart, chunkSignaturePrefix) {
		return fmt.Errorf("%w: invalid signature prefix", ErrInvalidChunkFormat)
	}
	t.chunkSig = strings.TrimPrefix(sigPart, chunkSignaturePrefix)

	// Handle final chunk
	if chunkSize == 0 {
		expectedSig := t.calculateChunkSignature([]byte{})
		if !constantTimeCompare(t.chunkSig, expectedSig) {
			return ErrChunkSignatureMismatch
		}

		// Update previous signature for trailer verification
		t.prevSig = t.chunkSig

		// Read and verify trailers
		if err := t.readAndVerifyTrailers(); err != nil {
			t.trailerErr = err
			return err
		}

		t.eof = true
		return nil
	}

	// Read chunk data into buffer
	t.chunkBuffer.Reset()
	t.chunkBuffer.Grow(int(chunkSize))

	// Use pooled hash writer to compute hash while reading
	hasher := utils.Sha256PoolGetHasher()
	defer utils.Sha256PoolPutHasher(hasher)
	writer := io.MultiWriter(&t.chunkBuffer, hasher)

	n, err := io.CopyN(writer, t.reader, chunkSize)
	if err != nil {
		return fmt.Errorf("reading chunk data: %w", err)
	}
	if n != chunkSize {
		return fmt.Errorf("%w: incomplete chunk", ErrInvalidChunkFormat)
	}

	// Read trailing CRLF
	crlf := make([]byte, 2)
	if _, err := io.ReadFull(t.reader, crlf); err != nil {
		return fmt.Errorf("reading chunk trailer: %w", err)
	}

	// Verify chunk signature
	chunkHash := hex.EncodeToString(hasher.Sum(nil))
	stringToSign := strings.Join([]string{
		chunkSigningAlgorithm,
		t.timestamp,
		t.credScope,
		t.prevSig,
		HashedEmptyPayload,
		chunkHash,
	}, "\n")

	expectedSig := hex.EncodeToString(hmacSHA256(t.signingKey, []byte(stringToSign)))
	if !constantTimeCompare(t.chunkSig, expectedSig) {
		return ErrChunkSignatureMismatch
	}

	// Update previous signature for next chunk
	t.prevSig = t.chunkSig

	return nil
}

// readAndVerifyTrailers reads trailing headers and verifies their signature
func (t *TrailerChunkReader) readAndVerifyTrailers() error {
	// Collect all trailer headers for signature verification
	// Format: header-name:header-value\r\n
	var trailerHeaders []string
	var trailerSignature string

	for {
		line, err := t.reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("reading trailer: %w", err)
		}

		line = strings.TrimSuffix(line, "\r\n")
		line = strings.TrimSuffix(line, "\n")

		// Empty line marks end of trailers
		if line == "" {
			break
		}

		// Parse header: name:value
		colonIdx := strings.Index(line, ":")
		if colonIdx == -1 {
			return fmt.Errorf("%w: malformed trailer header", ErrInvalidTrailerFormat)
		}

		name := strings.ToLower(strings.TrimSpace(line[:colonIdx]))
		value := strings.TrimSpace(line[colonIdx+1:])

		if name == trailerSignatureHeader {
			trailerSignature = value
		} else {
			// Store trailer for later use (e.g., checksum verification by handler)
			t.trailers[name] = value
			// Keep original format for signature calculation (lowercase name)
			trailerHeaders = append(trailerHeaders, name+":"+value)
		}

		if err == io.EOF {
			break
		}
	}

	t.trailersRead = true

	// If no trailer signature present, assume no trailers were expected
	if trailerSignature == "" {
		if len(trailerHeaders) > 0 {
			return fmt.Errorf("%w: trailers present but no signature", ErrInvalidTrailerFormat)
		}
		return nil
	}

	// Verify trailer signature
	return t.verifyTrailerSignature(trailerHeaders, trailerSignature)
}

// verifyTrailerSignature verifies the signature over trailing headers
func (t *TrailerChunkReader) verifyTrailerSignature(trailerHeaders []string, providedSig string) error {
	if len(trailerHeaders) == 0 {
		return fmt.Errorf("%w: signature present but no trailers", ErrInvalidTrailerFormat)
	}

	// Sort trailer headers alphabetically for canonical ordering
	sortedHeaders := make([]string, len(trailerHeaders))
	copy(sortedHeaders, trailerHeaders)
	// Note: AWS requires headers in sorted order
	for i := 0; i < len(sortedHeaders)-1; i++ {
		for j := i + 1; j < len(sortedHeaders); j++ {
			if sortedHeaders[i] > sortedHeaders[j] {
				sortedHeaders[i], sortedHeaders[j] = sortedHeaders[j], sortedHeaders[i]
			}
		}
	}

	// Build the trailing header string with newlines
	// Format: "header1:value1\nheader2:value2\n"
	var trailerStr strings.Builder
	for _, h := range sortedHeaders {
		trailerStr.WriteString(h)
		trailerStr.WriteString("\n")
	}

	// Hash the trailing headers
	hasher := utils.Sha256PoolGetHasher()
	hasher.Write([]byte(trailerStr.String()))
	trailerHash := hex.EncodeToString(hasher.Sum(nil))
	utils.Sha256PoolPutHasher(hasher)

	// Build string to sign for trailer
	// AWS4-HMAC-SHA256-TRAILER
	// timestamp
	// credential-scope
	// previous-signature (last chunk signature)
	// hash(trailing-headers)
	stringToSign := strings.Join([]string{
		trailerSigningAlgorithm,
		t.timestamp,
		t.credScope,
		t.prevSig,
		trailerHash,
	}, "\n")

	// Calculate expected signature
	expectedSig := hex.EncodeToString(hmacSHA256(t.signingKey, []byte(stringToSign)))

	if !constantTimeCompare(providedSig, expectedSig) {
		return ErrTrailerSignatureMismatch
	}

	return nil
}

// Trailers returns the parsed trailing headers (e.g., x-amz-checksum-crc32c).
// This should be called after reading all data (after io.EOF).
// Returns nil if no trailers were present or trailers haven't been read yet.
func (t *TrailerChunkReader) Trailers() map[string]string {
	if !t.trailersRead {
		return nil
	}
	return t.trailers
}

// GetTrailer returns a specific trailer value, or empty string if not present.
func (t *TrailerChunkReader) GetTrailer(name string) string {
	return t.trailers[strings.ToLower(name)]
}
