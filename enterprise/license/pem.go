//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package license

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
)

// encodePrivateKeyToPEM converts an RSA private key to PEM format.
func encodePrivateKeyToPEM(privateKey *rsa.PrivateKey) []byte {
	privASN1 := x509.MarshalPKCS1PrivateKey(privateKey)
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privASN1,
	})
}

// encodePublicKeyToPEM converts an RSA public key to PEM format.
func encodePublicKeyToPEM(publicKey *rsa.PublicKey) []byte {
	pubASN1, _ := x509.MarshalPKIXPublicKey(publicKey)
	return pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubASN1,
	})
}
