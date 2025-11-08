package encryption

// Params contains encryption parameters for an operation
type Params struct {
	// SSE-C parameters (mutually exclusive with SSEKMS)
	SSEC *SSECParams

	// SSE-KMS parameters (mutually exclusive with SSEC)
	SSEKMS *SSEKMSParams
}

// SSECParams contains SSE-C (Server-Side Encryption with Customer-Provided Keys) parameters
type SSECParams struct {
	Algorithm string // "AES256"
	Key       []byte // 32 bytes for AES-256
	KeyMD5    string // Base64-encoded MD5 of key
}

// SSEKMSParams contains SSE-KMS (Server-Side Encryption with KMS) parameters
type SSEKMSParams struct {
	KeyID   string // KMS key ID
	Context string // JSON encryption context (optional)
}

// Metadata contains encryption information stored with an object
type Metadata struct {
	Algorithm     string // "AES256" for SSE-C, "aws:kms" for SSE-KMS
	CustomerKeyMD5 string // For SSE-C: MD5 of customer key
	KMSKeyID      string // For SSE-KMS: KMS key ID
	KMSContext    string // For SSE-KMS: encryption context
	DEKCiphertext string // For SSE-KMS: encrypted data encryption key (base64)
}

// Result contains the result of an encryption operation
type Result struct {
	Ciphertext []byte
	Metadata   *Metadata
}

// IsSSEC returns true if these are SSE-C parameters
func (p *Params) IsSSEC() bool {
	return p != nil && p.SSEC != nil
}

// IsSSEKMS returns true if these are SSE-KMS parameters
func (p *Params) IsSSEKMS() bool {
	return p != nil && p.SSEKMS != nil
}

// IsEncrypted returns true if any encryption is configured
func (p *Params) IsEncrypted() bool {
	return p != nil && (p.SSEC != nil || p.SSEKMS != nil)
}

// FromMetadata creates Params from stored Metadata for decryption
func FromMetadata(m *Metadata) *Params {
	if m == nil {
		return nil
	}

	switch m.Algorithm {
	case "AES256":
		// SSE-C - caller must provide the key
		return &Params{
			SSEC: &SSECParams{
				Algorithm: m.Algorithm,
				KeyMD5:    m.CustomerKeyMD5,
			},
		}
	case "aws:kms":
		return &Params{
			SSEKMS: &SSEKMSParams{
				KeyID:   m.KMSKeyID,
				Context: m.KMSContext,
			},
		}
	default:
		return nil
	}
}
