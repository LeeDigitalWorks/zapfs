// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"bytes"
	"crypto/md5"
	"hash"
	"hash/crc32"
	"hash/fnv"
	"sync"

	"github.com/minio/crc64nvme"
	"github.com/minio/sha256-simd"
)

var (
	syncPool = sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}
	fnvPool = sync.Pool{
		New: func() any {
			return fnv.New64a()
		},
	}
	crc32Pool = sync.Pool{
		New: func() any {
			return crc32.NewIEEE()
		},
	}
	sha256Pool = sync.Pool{
		New: func() any {
			return sha256.New()
		},
	}
	crc64nvmePool = sync.Pool{
		New: func() any {
			return crc64nvme.New()
		},
	}
	md5Pool = sync.Pool{
		New: func() any {
			return md5.New()
		},
	}
)

func SyncPoolGetBuffer() *bytes.Buffer {
	return syncPool.Get().(*bytes.Buffer)
}

func SyncPoolPutBuffer(buffer *bytes.Buffer) {
	buffer.Reset()
	syncPool.Put(buffer)
}

func FnvPoolGetHasher() hash.Hash64 {
	return fnvPool.Get().(hash.Hash64)
}

func FnvPoolPutHasher(h hash.Hash64) {
	h.Reset()
	fnvPool.Put(h)
}

func Crc32PoolGetHasher() hash.Hash32 {
	return crc32Pool.Get().(hash.Hash32)
}

func Crc32PoolPutHasher(h hash.Hash32) {
	h.Reset()
	crc32Pool.Put(h)
}

func Sha256PoolGetHasher() hash.Hash {
	return sha256Pool.Get().(hash.Hash)
}

func Sha256PoolPutHasher(h hash.Hash) {
	h.Reset()
	sha256Pool.Put(h)
}

func Crc64nvmePoolGetHasher() hash.Hash64 {
	return crc64nvmePool.Get().(hash.Hash64)
}

func Crc64nvmePoolPutHasher(h hash.Hash64) {
	h.Reset()
	crc64nvmePool.Put(h)
}

func Md5PoolGetHasher() hash.Hash {
	return md5Pool.Get().(hash.Hash)
}

func Md5PoolPutHasher(h hash.Hash) {
	h.Reset()
	md5Pool.Put(h)
}
