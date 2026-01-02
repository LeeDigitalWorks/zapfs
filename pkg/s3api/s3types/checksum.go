// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import "github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"

type ChecksumAlgorithm uint8

const (
	ChecksumAlgorithmNone ChecksumAlgorithm = iota
	ChecksumAlgorithmCRC32
	ChecksumAlgorithmCRC32C
	ChecksumAlgorithmCRC64NVMe
	ChecksumAlgorithmSHA1
	ChecksumAlgorithmSHA256
)

var (
	checksumAlgorithmTypes = map[ChecksumAlgorithm]string{
		ChecksumAlgorithmNone:      "NONE",
		ChecksumAlgorithmCRC32:     "CRC32",
		ChecksumAlgorithmCRC32C:    "CRC32C",
		ChecksumAlgorithmCRC64NVMe: "CRC64-NVMe",
		ChecksumAlgorithmSHA1:      "SHA1",
		ChecksumAlgorithmSHA256:    "SHA256",
	}
	checksumAlgorithmNames = map[string]ChecksumAlgorithm{
		"NONE":       ChecksumAlgorithmNone,
		"CRC32":      ChecksumAlgorithmCRC32,
		"CRC32C":     ChecksumAlgorithmCRC32C,
		"CRC64-NVMe": ChecksumAlgorithmCRC64NVMe,
		"SHA1":       ChecksumAlgorithmSHA1,
		"SHA256":     ChecksumAlgorithmSHA256,
	}
)

func (c ChecksumAlgorithm) String() string {
	if name, ok := checksumAlgorithmTypes[c]; ok {
		return name
	}
	return "NONE"
}

func (c ChecksumAlgorithm) IsValid() bool {
	return c != ChecksumAlgorithmNone
}

func ParseChecksumAlgorithm(s string) (ChecksumAlgorithm, error) {
	if alg, ok := checksumAlgorithmNames[s]; ok {
		return alg, nil
	}
	return ChecksumAlgorithmNone, s3err.ErrInvalidDigest
}
