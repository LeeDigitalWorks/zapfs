// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package ec

import (
	"github.com/klauspost/reedsolomon"
)

// ReedSolomonCoder implements ErasureCoder using Reed-Solomon
type ReedSolomonCoder struct {
	enc          reedsolomon.Encoder
	dataShards   int
	parityShards int
}

// NewReedSolomonCoder creates a Reed-Solomon encoder
func NewReedSolomonCoder(dataShards, parityShards int) (*ReedSolomonCoder, error) {
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	return &ReedSolomonCoder{
		enc:          enc,
		dataShards:   dataShards,
		parityShards: parityShards,
	}, nil
}

func (r *ReedSolomonCoder) DataShards() int {
	return r.dataShards
}

func (r *ReedSolomonCoder) ParityShards() int {
	return r.parityShards
}

// EncodeData splits data into data+parity shards
func (r *ReedSolomonCoder) EncodeData(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, r.dataShards+r.parityShards), nil
	}

	shards, err := r.enc.Split(data)
	if err != nil {
		return nil, err
	}
	if err := r.enc.Encode(shards); err != nil {
		return nil, err
	}
	return shards, nil
}

// DecodeData reconstructs missing shards in place
// shards with nil values will be reconstructed
func (r *ReedSolomonCoder) DecodeData(shards [][]byte) error {
	// Check if reconstruction needed
	needsReconstruct := false
	for _, s := range shards {
		if s == nil {
			needsReconstruct = true
			break
		}
	}
	if !needsReconstruct {
		return nil
	}
	return r.enc.Reconstruct(shards)
}
