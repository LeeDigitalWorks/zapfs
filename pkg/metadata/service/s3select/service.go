// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"compress/bzip2"
	"compress/gzip"
	"context"
	"fmt"
	"io"
)

// ============================================================================
// Service Implementation
// ============================================================================

// Config holds configuration for the S3 Select service.
type Config struct {
	// MaxRecordSize is the maximum size of a single record in bytes.
	// Default: 1 MB
	MaxRecordSize int

	// MaxRecordsBuffer is the maximum number of records to buffer.
	// Default: 1000
	MaxRecordsBuffer int

	// ChunkSize is the size of output chunks in the event stream.
	// Default: 256 KB
	ChunkSize int
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		MaxRecordSize:    1 * 1024 * 1024, // 1 MB
		MaxRecordsBuffer: 1000,            // 1000 records
		ChunkSize:        256 * 1024,      // 256 KB
	}
}

// serviceImpl implements the Service interface.
type serviceImpl struct {
	config    Config
	parser    Parser
	evaluator Evaluator
}

// NewService creates a new S3 Select service.
func NewService(cfg Config) Service {
	if cfg.MaxRecordSize == 0 {
		cfg.MaxRecordSize = DefaultConfig().MaxRecordSize
	}
	if cfg.MaxRecordsBuffer == 0 {
		cfg.MaxRecordsBuffer = DefaultConfig().MaxRecordsBuffer
	}
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = DefaultConfig().ChunkSize
	}

	return &serviceImpl{
		config:    cfg,
		parser:    &sqlParser{},
		evaluator: &exprEvaluator{},
	}
}

// Select executes an S3 Select query and returns a streaming result.
func (s *serviceImpl) Select(ctx context.Context, req *SelectRequest, objectReader io.ReadCloser) (*SelectResult, error) {
	// 1. Parse the SQL expression
	query, err := s.parser.Parse(req.Expression)
	if err != nil {
		return nil, &SelectError{
			Code:    "InvalidQuery",
			Message: fmt.Sprintf("failed to parse SQL expression: %v", err),
		}
	}

	// 2. Create decompression reader if needed
	reader, err := s.createDecompressor(objectReader, req.Input.CompressionType)
	if err != nil {
		objectReader.Close()
		return nil, err
	}

	// 3. Create the appropriate record reader based on input format
	recordReader, err := s.createRecordReader(reader, req.Input)
	if err != nil {
		reader.Close()
		return nil, err
	}

	// 4. Create the record writer based on output format
	outputPipe := newOutputPipe(s.config.ChunkSize)
	recordWriter, err := s.createRecordWriter(outputPipe, req.Output, query)
	if err != nil {
		recordReader.Close()
		return nil, err
	}

	// 5. Create the event stream
	eventStream := newEventStreamImpl(ctx, query, recordReader, recordWriter, s.evaluator, outputPipe, req.RequestProgress)

	// 6. Start processing in background
	go eventStream.run()

	return &SelectResult{
		EventStream: eventStream,
	}, nil
}

// createDecompressor wraps the reader with appropriate decompression.
func (s *serviceImpl) createDecompressor(r io.ReadCloser, compressionType string) (io.ReadCloser, error) {
	switch compressionType {
	case "", "NONE":
		return r, nil
	case "GZIP":
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, &SelectError{
				Code:    "InvalidCompressionFormat",
				Message: fmt.Sprintf("failed to create gzip reader: %v", err),
			}
		}
		return &compressedReader{Reader: gr, closer: r}, nil
	case "BZIP2":
		br := bzip2.NewReader(r)
		return &compressedReader{Reader: br, closer: r}, nil
	default:
		return nil, &SelectError{
			Code:    "InvalidCompressionFormat",
			Message: fmt.Sprintf("unsupported compression type: %s", compressionType),
		}
	}
}

// createRecordReader creates the appropriate record reader for the input format.
func (s *serviceImpl) createRecordReader(r io.Reader, input InputSerialization) (RecordReader, error) {
	switch {
	case input.CSV != nil:
		return newCSVReader(r, input.CSV, s.config.MaxRecordSize)
	case input.JSON != nil:
		return newJSONReader(r, input.JSON, s.config.MaxRecordSize)
	case input.Parquet != nil:
		// Parquet requires io.ReaderAt for random access (metadata at end of file).
		// Use NewParquetReader directly in the handler with buffered data.
		return nil, &SelectError{
			Code:    "UnsupportedFormat",
			Message: "Parquet format requires random access; use NewParquetReader with io.ReaderAt",
		}
	default:
		return nil, &SelectError{
			Code:    "InvalidInputFormat",
			Message: "no input serialization format specified",
		}
	}
}

// createRecordWriter creates the appropriate record writer for the output format.
func (s *serviceImpl) createRecordWriter(w io.Writer, output OutputSerialization, query *Query) (RecordWriter, error) {
	switch {
	case output.CSV != nil:
		return newCSVWriter(w, output.CSV, query)
	case output.JSON != nil:
		return newJSONWriter(w, output.JSON)
	default:
		return nil, &SelectError{
			Code:    "InvalidOutputFormat",
			Message: "no output serialization format specified",
		}
	}
}

// compressedReader wraps a decompressed reader with the original closer.
type compressedReader struct {
	io.Reader
	closer io.Closer
}

func (c *compressedReader) Close() error {
	if closer, ok := c.Reader.(io.Closer); ok {
		closer.Close()
	}
	return c.closer.Close()
}

// ============================================================================
// Error Types
// ============================================================================

// SelectError represents an S3 Select error.
type SelectError struct {
	Code    string
	Message string
}

func (e *SelectError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Common error codes:
// - InvalidQuery: SQL syntax error
// - InvalidInputFormat: Unsupported or invalid input format
// - InvalidOutputFormat: Unsupported or invalid output format
// - InvalidCompressionFormat: Unsupported compression type
// - ExpressionTooLong: SQL expression exceeds maximum length
// - RecordTooLarge: Input record exceeds maximum size
// - UnsupportedFunction: SQL function not supported
// - CastFailed: Type cast operation failed
// - OverMaxRecordSize: Output record exceeds maximum size
