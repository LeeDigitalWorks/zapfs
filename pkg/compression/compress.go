// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Encoder/decoder pools for reduced allocations
var (
	zstdEncoderPool sync.Pool
	zstdDecoderPool sync.Pool
	lz4WriterPool   sync.Pool
	lz4ReaderPool   sync.Pool
	s2WriterPool    sync.Pool
	s2ReaderPool    sync.Pool
)

func init() {
	zstdEncoderPool = sync.Pool{
		New: func() any {
			enc, _ := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.SpeedDefault),
				zstd.WithEncoderConcurrency(1),
			)
			return enc
		},
	}
	zstdDecoderPool = sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil,
				zstd.WithDecoderConcurrency(1),
			)
			return dec
		},
	}
	lz4WriterPool = sync.Pool{
		New: func() any {
			return lz4.NewWriter(nil)
		},
	}
	lz4ReaderPool = sync.Pool{
		New: func() any {
			return lz4.NewReader(nil)
		},
	}
	s2WriterPool = sync.Pool{
		New: func() any {
			return s2.NewWriter(nil, s2.WriterConcurrency(1))
		},
	}
	s2ReaderPool = sync.Pool{
		New: func() any {
			return s2.NewReader(nil)
		},
	}
}

// ============================================================================
// Byte-based API (for smaller data or when full data is available)
// ============================================================================

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

// ============================================================================
// Streaming API (for large data with lower memory footprint)
// ============================================================================

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

// ============================================================================
// LZ4 implementation
// ============================================================================

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

func decompressLZ4(data []byte) ([]byte, error) {
	r := lz4ReaderPool.Get().(*lz4.Reader)
	r.Reset(bytes.NewReader(data))
	defer func() {
		r.Reset(nil)
		lz4ReaderPool.Put(r)
	}()

	decompressed, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("lz4 decompress: %w", err)
	}
	return decompressed, nil
}

type lz4CompressReader struct {
	pr     *io.PipeReader
	pw     *io.PipeWriter
	writer *lz4.Writer
	done   chan struct{}
}

func newLZ4CompressReader(r io.Reader) *lz4CompressReader {
	pr, pw := io.Pipe()
	w := lz4WriterPool.Get().(*lz4.Writer)
	w.Reset(pw)

	cr := &lz4CompressReader{
		pr:     pr,
		pw:     pw,
		writer: w,
		done:   make(chan struct{}),
	}

	go func() {
		defer close(cr.done)
		_, err := io.Copy(w, r)
		w.Close()
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	return cr
}

func (r *lz4CompressReader) Read(p []byte) (int, error) {
	return r.pr.Read(p)
}

func (r *lz4CompressReader) Close() error {
	r.pr.Close()
	<-r.done
	r.writer.Reset(nil)
	lz4WriterPool.Put(r.writer)
	return nil
}

func newLZ4DecompressReader(r io.Reader) io.ReadCloser {
	lr := lz4ReaderPool.Get().(*lz4.Reader)
	lr.Reset(r)
	return &pooledLZ4Reader{Reader: lr}
}

type pooledLZ4Reader struct {
	*lz4.Reader
}

func (r *pooledLZ4Reader) Close() error {
	r.Reset(nil)
	lz4ReaderPool.Put(r.Reader)
	return nil
}

func newLZ4CompressWriter(w io.Writer) io.WriteCloser {
	lw := lz4WriterPool.Get().(*lz4.Writer)
	lw.Reset(w)
	return &pooledLZ4Writer{Writer: lw}
}

type pooledLZ4Writer struct {
	*lz4.Writer
}

func (w *pooledLZ4Writer) Close() error {
	err := w.Writer.Close()
	w.Reset(nil)
	lz4WriterPool.Put(w.Writer)
	return err
}

// ============================================================================
// ZSTD implementation
// ============================================================================

func compressZSTD(data []byte) ([]byte, error) {
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	defer zstdEncoderPool.Put(enc)

	return enc.EncodeAll(data, nil), nil
}

func decompressZSTD(data []byte) ([]byte, error) {
	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(dec)

	decompressed, err := dec.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress: %w", err)
	}
	return decompressed, nil
}

type zstdCompressReader struct {
	pr   *io.PipeReader
	pw   *io.PipeWriter
	enc  *zstd.Encoder
	done chan struct{}
}

func newZSTDCompressReader(r io.Reader) *zstdCompressReader {
	pr, pw := io.Pipe()
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	enc.Reset(pw)

	cr := &zstdCompressReader{
		pr:   pr,
		pw:   pw,
		enc:  enc,
		done: make(chan struct{}),
	}

	go func() {
		defer close(cr.done)
		_, err := io.Copy(enc, r)
		enc.Close()
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	return cr
}

func (r *zstdCompressReader) Read(p []byte) (int, error) {
	return r.pr.Read(p)
}

func (r *zstdCompressReader) Close() error {
	r.pr.Close()
	<-r.done
	zstdEncoderPool.Put(r.enc)
	return nil
}

func newZSTDDecompressReader(r io.Reader) (io.ReadCloser, error) {
	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	if err := dec.Reset(r); err != nil {
		zstdDecoderPool.Put(dec)
		return nil, fmt.Errorf("zstd reset: %w", err)
	}
	return &pooledZSTDReader{Decoder: dec}, nil
}

type pooledZSTDReader struct {
	*zstd.Decoder
}

func (r *pooledZSTDReader) Read(p []byte) (int, error) {
	return r.Decoder.Read(p)
}

func (r *pooledZSTDReader) Close() error {
	zstdDecoderPool.Put(r.Decoder)
	return nil
}

func newZSTDCompressWriter(w io.Writer) io.WriteCloser {
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	enc.Reset(w)
	return &pooledZSTDWriter{Encoder: enc}
}

type pooledZSTDWriter struct {
	*zstd.Encoder
}

func (w *pooledZSTDWriter) Close() error {
	err := w.Encoder.Close()
	zstdEncoderPool.Put(w.Encoder)
	return err
}

// ============================================================================
// S2 implementation (replaces Snappy with better performance)
// ============================================================================

func compressS2(data []byte) ([]byte, error) {
	return s2.Encode(nil, data), nil
}

func decompressS2(data []byte) ([]byte, error) {
	decompressed, err := s2.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("s2 decompress: %w", err)
	}
	return decompressed, nil
}

type s2CompressReader struct {
	pr     *io.PipeReader
	pw     *io.PipeWriter
	writer *s2.Writer
	done   chan struct{}
}

func newS2CompressReader(r io.Reader) *s2CompressReader {
	pr, pw := io.Pipe()
	w := s2WriterPool.Get().(*s2.Writer)
	w.Reset(pw)

	cr := &s2CompressReader{
		pr:     pr,
		pw:     pw,
		writer: w,
		done:   make(chan struct{}),
	}

	go func() {
		defer close(cr.done)
		_, err := io.Copy(w, r)
		w.Close()
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()

	return cr
}

func (r *s2CompressReader) Read(p []byte) (int, error) {
	return r.pr.Read(p)
}

func (r *s2CompressReader) Close() error {
	r.pr.Close()
	<-r.done
	r.writer.Reset(nil)
	s2WriterPool.Put(r.writer)
	return nil
}

func newS2DecompressReader(r io.Reader) io.ReadCloser {
	sr := s2ReaderPool.Get().(*s2.Reader)
	sr.Reset(r)
	return &pooledS2Reader{Reader: sr}
}

type pooledS2Reader struct {
	*s2.Reader
}

func (r *pooledS2Reader) Close() error {
	r.Reset(nil)
	s2ReaderPool.Put(r.Reader)
	return nil
}

func newS2CompressWriter(w io.Writer) io.WriteCloser {
	sw := s2WriterPool.Get().(*s2.Writer)
	sw.Reset(w)
	return &pooledS2Writer{Writer: sw}
}

type pooledS2Writer struct {
	*s2.Writer
}

func (w *pooledS2Writer) Close() error {
	err := w.Writer.Close()
	w.Reset(nil)
	s2WriterPool.Put(w.Writer)
	return err
}

// ============================================================================
// Helper functions
// ============================================================================

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
