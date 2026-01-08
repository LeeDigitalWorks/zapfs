// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"bytes"
	"context"
	"io"
	"sync"
)

// ============================================================================
// Event Stream Implementation
// ============================================================================

// eventStreamImpl implements the EventStream interface.
type eventStreamImpl struct {
	ctx             context.Context
	query           *Query
	reader          RecordReader
	writer          RecordWriter
	evaluator       Evaluator
	output          *outputPipe
	requestProgress bool

	// Statistics
	bytesScanned   int64
	bytesProcessed int64
	bytesReturned  int64

	// Event channel
	events chan Event
	err    error
	done   bool
	mu     sync.Mutex
}

// newEventStreamImpl creates a new event stream.
func newEventStreamImpl(
	ctx context.Context,
	query *Query,
	reader RecordReader,
	writer RecordWriter,
	evaluator Evaluator,
	output *outputPipe,
	requestProgress bool,
) *eventStreamImpl {
	return &eventStreamImpl{
		ctx:             ctx,
		query:           query,
		reader:          reader,
		writer:          writer,
		evaluator:       evaluator,
		output:          output,
		requestProgress: requestProgress,
		events:          make(chan Event, 10),
	}
}

// run processes records and generates events.
func (s *eventStreamImpl) run() {
	defer close(s.events)
	defer s.reader.Close()
	defer s.writer.Close()

	recordCount := int64(0)
	limit := s.query.Limit

	for {
		select {
		case <-s.ctx.Done():
			s.setError(s.ctx.Err())
			return
		default:
		}

		// Read next record
		record, err := s.reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			s.setError(err)
			return
		}

		// Apply WHERE filter
		if s.query.Where != nil {
			match, err := s.evaluator.EvaluateBool(s.query.Where, record)
			if err != nil {
				s.setError(err)
				return
			}
			if !match {
				continue
			}
		}

		// Project columns
		projected, err := s.projectRecord(record)
		if err != nil {
			s.setError(err)
			return
		}

		// Write to output
		if err := s.writer.WriteRecord(projected); err != nil {
			s.setError(err)
			return
		}

		recordCount++

		// Check LIMIT
		if limit > 0 && recordCount >= limit {
			break
		}

		// Flush output periodically and emit Records event
		if s.output.Len() >= s.output.chunkSize {
			s.flushRecords()
		}

		// Emit progress event if requested
		if s.requestProgress && recordCount%1000 == 0 {
			s.events <- &ProgressEvent{
				BytesScanned:   s.bytesScanned,
				BytesProcessed: s.bytesProcessed,
				BytesReturned:  s.bytesReturned,
			}
		}
	}

	// Final flush
	s.writer.Flush()
	s.flushRecords()

	// Emit stats
	s.events <- &StatsEvent{
		BytesScanned:   s.bytesScanned,
		BytesProcessed: s.bytesProcessed,
		BytesReturned:  s.bytesReturned,
	}

	// Emit end
	s.events <- &EndEvent{}
}

// projectRecord applies SELECT projections to a record.
func (s *eventStreamImpl) projectRecord(record Record) (Record, error) {
	if len(s.query.Projections) == 0 {
		return record, nil
	}

	// Check for SELECT *
	for _, p := range s.query.Projections {
		if _, ok := p.Expr.(*StarExpr); ok {
			return record, nil
		}
	}

	// Project specific columns
	names := make([]string, 0, len(s.query.Projections))
	values := make([]any, 0, len(s.query.Projections))

	for _, p := range s.query.Projections {
		val, err := s.evaluator.Evaluate(p.Expr, record)
		if err != nil {
			return nil, err
		}

		name := p.Alias
		if name == "" {
			if col, ok := p.Expr.(*ColumnRef); ok {
				name = col.Name
			}
		}

		names = append(names, name)
		values = append(values, val)
	}

	return &projectedRecord{names: names, vals: values}, nil
}

// flushRecords flushes buffered output as a Records event.
func (s *eventStreamImpl) flushRecords() {
	data := s.output.Bytes()
	if len(data) > 0 {
		s.bytesReturned += int64(len(data))
		s.events <- &RecordsEvent{Payload: data}
		s.output.Reset()
	}
}

// setError sets the error and marks stream as done.
func (s *eventStreamImpl) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// Next returns the next event.
func (s *eventStreamImpl) Next() (Event, error) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return nil, io.EOF
	}
	err := s.err
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	event, ok := <-s.events
	if !ok {
		s.mu.Lock()
		s.done = true
		err := s.err
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	return event, nil
}

// Close closes the event stream.
func (s *eventStreamImpl) Close() error {
	s.mu.Lock()
	s.done = true
	s.mu.Unlock()

	s.reader.Close()
	return nil
}

// ============================================================================
// Output Pipe
// ============================================================================

// outputPipe buffers output data and provides chunk-based access.
type outputPipe struct {
	buf       bytes.Buffer
	chunkSize int
}

// newOutputPipe creates a new output pipe.
func newOutputPipe(chunkSize int) *outputPipe {
	return &outputPipe{
		chunkSize: chunkSize,
	}
}

// Write writes data to the buffer.
func (p *outputPipe) Write(data []byte) (int, error) {
	return p.buf.Write(data)
}

// Bytes returns the buffered data.
func (p *outputPipe) Bytes() []byte {
	return p.buf.Bytes()
}

// Len returns the length of buffered data.
func (p *outputPipe) Len() int {
	return p.buf.Len()
}

// Reset resets the buffer.
func (p *outputPipe) Reset() {
	p.buf.Reset()
}

// ============================================================================
// Projected Record
// ============================================================================

// projectedRecord represents a record with projected columns.
type projectedRecord struct {
	names []string
	vals  []any
}

func (r *projectedRecord) Get(name string) any {
	for i, n := range r.names {
		if n == name && i < len(r.vals) {
			return r.vals[i]
		}
	}
	return nil
}

func (r *projectedRecord) GetByIndex(index int) any {
	if index >= 0 && index < len(r.vals) {
		return r.vals[index]
	}
	return nil
}

func (r *projectedRecord) ColumnNames() []string {
	return r.names
}

func (r *projectedRecord) Values() []any {
	return r.vals
}

// ============================================================================
// AWS Event Stream Protocol (Stub)
// ============================================================================

// S3 Select uses the AWS Event Stream format for responses.
// The format is a binary protocol with headers and payload.
//
// Event Stream Message Format:
// +------------------+------------------+------------------+
// | Prelude (8 bytes)| Headers (variable)| Payload (variable)|
// +------------------+------------------+------------------+
//
// Prelude:
// - Total byte length (4 bytes, big-endian)
// - Headers byte length (4 bytes, big-endian)
// - Prelude CRC (4 bytes)
//
// Headers:
// - Header name length (1 byte)
// - Header name (variable)
// - Header value type (1 byte)
// - Header value (variable based on type)
//
// Message CRC: 4 bytes at end
//
// For S3 Select, the event types are:
// - Records: Contains query result data
// - Stats: Contains query statistics
// - Progress: Contains progress information
// - Cont: Keep-alive continuation
// - End: End of results
//
// Implementation Note:
// AWS SDK handles event stream encoding/decoding automatically.
// The HTTP response should use:
// - Content-Type: application/octet-stream
// - Transfer-Encoding: chunked
// - x-amz-request-id: <request-id>
//
// For HTTP handler implementation, write events as:
// 1. Encode each event using AWS event stream format
// 2. Write to response as chunked transfer encoding
// 3. Flush after each event for real-time streaming
