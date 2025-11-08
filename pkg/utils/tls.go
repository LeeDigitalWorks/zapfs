package utils

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// LoadServerTLSConfig loads TLS configuration for gRPC server
// Returns nil if certFile and keyFile are empty (insecure mode)
func LoadServerTLSConfig(certFile, keyFile string) (credentials.TransportCredentials, error) {
	if certFile == "" || keyFile == "" {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load key pair: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert, // Can be changed to tls.RequireAndVerifyClientCert for mTLS
	}

	return credentials.NewTLS(tlsConfig), nil
}

// LoadClientTLSConfig loads TLS configuration for gRPC client
// certFile: client certificate (optional, for mTLS)
// keyFile: client key (optional, for mTLS)
// caFile: CA certificate to verify server (optional, uses system CA if empty)
// Returns insecure credentials if all files are empty
func LoadClientTLSConfig(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	// If no TLS files provided, use insecure
	if certFile == "" && keyFile == "" && caFile == "" {
		return insecure.NewCredentials(), nil
	}

	tlsConfig := &tls.Config{}

	// Load client certificate if provided (for mTLS)
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate if provided
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
		tlsConfig.RootCAs = certPool
	}

	return credentials.NewTLS(tlsConfig), nil
}

// GetServerDialOption returns the appropriate grpc.DialOption for client connections
// Use the same cert/key files if doing mTLS, otherwise pass empty strings for insecure
func GetServerDialOption(certFile, keyFile, caFile string) (grpc.DialOption, error) {
	creds, err := LoadClientTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	return grpc.WithTransportCredentials(creds), nil
}

// GetServerOption returns the appropriate grpc.ServerOption for server setup
// Returns nil if certFile and keyFile are empty (insecure mode)
func GetServerOption(certFile, keyFile string) (grpc.ServerOption, error) {
	creds, err := LoadServerTLSConfig(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	if creds == nil {
		return nil, nil // No TLS
	}
	return grpc.Creds(creds), nil
}
