//go:build enterprise

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
