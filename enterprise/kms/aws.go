//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package kms

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// AWSProvider implements the Provider interface for AWS KMS
type AWSProvider struct {
	client *kms.Client
	config AWSConfig
}

// NewAWSProvider creates a new AWS KMS provider
func NewAWSProvider(ctx context.Context, cfg AWSConfig) (*AWSProvider, error) {
	// Build AWS config options
	var opts []func(*config.LoadOptions) error

	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}

	// Use explicit credentials if provided
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				"",
			),
		))
	}

	// Load AWS config
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create KMS client options
	var kmsOpts []func(*kms.Options)

	// Custom endpoint (for LocalStack/testing)
	if cfg.Endpoint != "" {
		kmsOpts = append(kmsOpts, func(o *kms.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	// Assume role if specified
	if cfg.RoleARN != "" {
		stsClient := sts.NewFromConfig(awsCfg)
		creds := stscreds.NewAssumeRoleProvider(stsClient, cfg.RoleARN)
		awsCfg.Credentials = aws.NewCredentialsCache(creds)
	}

	client := kms.NewFromConfig(awsCfg, kmsOpts...)

	return &AWSProvider{
		client: client,
		config: cfg,
	}, nil
}

// Name returns the provider name
func (p *AWSProvider) Name() string {
	return "aws"
}

// Encrypt encrypts plaintext using AWS KMS
func (p *AWSProvider) Encrypt(ctx context.Context, keyID string, plaintext []byte) ([]byte, error) {
	output, err := p.client.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: plaintext,
	})
	if err != nil {
		return nil, fmt.Errorf("AWS KMS encrypt failed: %w", err)
	}
	return output.CiphertextBlob, nil
}

// Decrypt decrypts ciphertext using AWS KMS
func (p *AWSProvider) Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error) {
	output, err := p.client.Decrypt(ctx, &kms.DecryptInput{
		KeyId:          aws.String(keyID),
		CiphertextBlob: ciphertext,
	})
	if err != nil {
		return nil, fmt.Errorf("AWS KMS decrypt failed: %w", err)
	}
	return output.Plaintext, nil
}

// GenerateDataKey generates a data encryption key
func (p *AWSProvider) GenerateDataKey(ctx context.Context, keyID string, keySpec string) ([]byte, []byte, error) {
	spec := types.DataKeySpecAes256
	switch keySpec {
	case "AES_128":
		spec = types.DataKeySpecAes128
	case "AES_256", "":
		spec = types.DataKeySpecAes256
	}

	output, err := p.client.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{
		KeyId:   aws.String(keyID),
		KeySpec: spec,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("AWS KMS generate data key failed: %w", err)
	}
	return output.Plaintext, output.CiphertextBlob, nil
}

// DescribeKey returns metadata about a key
func (p *AWSProvider) DescribeKey(ctx context.Context, keyID string) (*KeyMetadata, error) {
	output, err := p.client.DescribeKey(ctx, &kms.DescribeKeyInput{
		KeyId: aws.String(keyID),
	})
	if err != nil {
		return nil, fmt.Errorf("AWS KMS describe key failed: %w", err)
	}

	meta := output.KeyMetadata
	km := &KeyMetadata{
		KeyID:        aws.ToString(meta.KeyId),
		ARN:          aws.ToString(meta.Arn),
		CreationDate: aws.ToTime(meta.CreationDate),
		Description:  aws.ToString(meta.Description),
		KeyUsage:     string(meta.KeyUsage),
		Origin:       string(meta.Origin),
		Provider:     "aws",
	}

	// Map AWS key state to our KeyState
	switch meta.KeyState {
	case types.KeyStateEnabled:
		km.KeyState = KeyStateEnabled
	case types.KeyStateDisabled:
		km.KeyState = KeyStateDisabled
	case types.KeyStatePendingDeletion:
		km.KeyState = KeyStatePendingDeletion
	default:
		km.KeyState = KeyStateDisabled
	}

	return km, nil
}

// ListKeys returns all available key IDs
func (p *AWSProvider) ListKeys(ctx context.Context) ([]string, error) {
	var keyIDs []string
	paginator := kms.NewListKeysPaginator(p.client, &kms.ListKeysInput{})

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("AWS KMS list keys failed: %w", err)
		}
		for _, key := range output.Keys {
			keyIDs = append(keyIDs, aws.ToString(key.KeyId))
		}
	}

	return keyIDs, nil
}

// Close releases resources (no-op for AWS)
func (p *AWSProvider) Close() error {
	return nil
}

// CreateKey creates a new KMS key (implements KeyCreator interface)
func (p *AWSProvider) CreateKey(ctx context.Context, input CreateKeyInput) (*KeyMetadata, error) {
	kmsInput := &kms.CreateKeyInput{
		Description: aws.String(input.Description),
	}

	if input.KeyUsage != "" {
		kmsInput.KeyUsage = types.KeyUsageType(input.KeyUsage)
	}

	if len(input.Tags) > 0 {
		for k, v := range input.Tags {
			kmsInput.Tags = append(kmsInput.Tags, types.Tag{
				TagKey:   aws.String(k),
				TagValue: aws.String(v),
			})
		}
	}

	output, err := p.client.CreateKey(ctx, kmsInput)
	if err != nil {
		return nil, fmt.Errorf("AWS KMS create key failed: %w", err)
	}

	// Create alias if specified
	if input.Alias != "" {
		_, err = p.client.CreateAlias(ctx, &kms.CreateAliasInput{
			AliasName:   aws.String("alias/" + input.Alias),
			TargetKeyId: output.KeyMetadata.KeyId,
		})
		if err != nil {
			// Key was created but alias failed - log warning but don't fail
			// The key is still usable by its ID
		}
	}

	return p.DescribeKey(ctx, aws.ToString(output.KeyMetadata.KeyId))
}

// EnableKey enables a disabled key (implements KeyManager interface)
func (p *AWSProvider) EnableKey(ctx context.Context, keyID string) error {
	_, err := p.client.EnableKey(ctx, &kms.EnableKeyInput{
		KeyId: aws.String(keyID),
	})
	if err != nil {
		return fmt.Errorf("AWS KMS enable key failed: %w", err)
	}
	return nil
}

// DisableKey disables a key (implements KeyManager interface)
func (p *AWSProvider) DisableKey(ctx context.Context, keyID string) error {
	_, err := p.client.DisableKey(ctx, &kms.DisableKeyInput{
		KeyId: aws.String(keyID),
	})
	if err != nil {
		return fmt.Errorf("AWS KMS disable key failed: %w", err)
	}
	return nil
}

// ScheduleKeyDeletion schedules a key for deletion (implements KeyManager interface)
func (p *AWSProvider) ScheduleKeyDeletion(ctx context.Context, keyID string, pendingDays int) error {
	if pendingDays < 7 {
		pendingDays = 7 // AWS minimum is 7 days
	}
	if pendingDays > 30 {
		pendingDays = 30 // AWS maximum is 30 days
	}

	_, err := p.client.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{
		KeyId:               aws.String(keyID),
		PendingWindowInDays: aws.Int32(int32(pendingDays)),
	})
	if err != nil {
		return fmt.Errorf("AWS KMS schedule key deletion failed: %w", err)
	}
	return nil
}

// Ensure AWSProvider implements all interfaces
var (
	_ Provider   = (*AWSProvider)(nil)
	_ KeyCreator = (*AWSProvider)(nil)
	_ KeyManager = (*AWSProvider)(nil)
)
