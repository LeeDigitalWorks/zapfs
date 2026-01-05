// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	zstdEncoderPool sync.Pool
	zstdDecoderPool sync.Pool
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
}

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
