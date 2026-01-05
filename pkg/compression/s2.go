// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
)

var (
	s2WriterPool sync.Pool
	s2ReaderPool sync.Pool
)

func init() {
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
