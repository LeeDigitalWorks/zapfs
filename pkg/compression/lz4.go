// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/pierrec/lz4/v4"
)

var (
	lz4WriterPool sync.Pool
	lz4ReaderPool sync.Pool
)

func init() {
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
}

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
