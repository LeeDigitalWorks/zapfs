//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package kms provides external Key Management Service integration.
// This package is only available in the enterprise edition of ZapFS.
//
// Supported KMS providers:
// - AWS KMS
// - HashiCorp Vault
// - Google Cloud KMS
// - Azure Key Vault
//
// Features:
// - External master key management
// - Key rotation
// - Envelope encryption
// - HSM-backed keys
package kms

// TODO: Implement KMS integration
// - KMSProvider interface
// - AWS KMS implementation
// - Vault implementation
// - Key rotation scheduler
