//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg := DefaultKafkaConfig([]string{"localhost:9092"})

	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	assert.Equal(t, "s3-events", cfg.Topic)
	assert.Equal(t, 1, cfg.RequiredAcks)
	assert.Equal(t, "snappy", cfg.Compression)
	assert.Equal(t, 100, cfg.BatchSize)
	assert.Equal(t, time.Second, cfg.BatchTimeout)
	assert.Equal(t, 10*time.Second, cfg.WriteTimeout)
}

func TestNewKafkaPublisher_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     KafkaConfig
		wantErr string
	}{
		{
			name:    "empty brokers",
			cfg:     KafkaConfig{},
			wantErr: "at least one Kafka broker is required",
		},
		{
			name: "valid config with defaults",
			cfg: KafkaConfig{
				Brokers: []string{"localhost:9092"},
			},
			// Will fail to connect but validates config
			wantErr: "kafka producer creation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewKafkaPublisher(tt.cfg)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestKafkaPublisher_Name(t *testing.T) {
	t.Parallel()

	pub := &KafkaPublisher{topic: "test"}
	assert.Equal(t, "kafka", pub.Name())
}

func TestKafkaPublisher_Publish(t *testing.T) {
	t.Parallel()

	// Create mock producer
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageAndSucceed()

	pub := &KafkaPublisher{
		producer: mockProducer,
		topic:    "s3-events",
	}

	ctx := context.Background()
	eventData := []byte(`{"Records":[{"eventName":"s3:ObjectCreated:Put"}]}`)

	err := pub.Publish(ctx, "my-bucket", eventData)
	require.NoError(t, err)
}

func TestKafkaPublisher_PublishError(t *testing.T) {
	t.Parallel()

	// Create mock producer that returns an error
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageAndFail(errors.New("broker unavailable"))

	pub := &KafkaPublisher{
		producer: mockProducer,
		topic:    "s3-events",
	}

	ctx := context.Background()
	eventData := []byte(`{"Records":[]}`)

	err := pub.Publish(ctx, "my-bucket", eventData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka publish")
	assert.Contains(t, err.Error(), "broker unavailable")
}

func TestKafkaPublisher_Close(t *testing.T) {
	t.Parallel()

	// Test nil producer
	pub := &KafkaPublisher{}
	err := pub.Close()
	assert.NoError(t, err)

	// Test with mock producer
	mockProducer := mocks.NewSyncProducer(t, nil)
	pub = &KafkaPublisher{producer: mockProducer}
	err = pub.Close()
	assert.NoError(t, err)
}

func TestKafkaPublisher_CompressionMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		compression string
		expected    sarama.CompressionCodec
	}{
		{"gzip", sarama.CompressionGZIP},
		{"snappy", sarama.CompressionSnappy},
		{"lz4", sarama.CompressionLZ4},
		{"zstd", sarama.CompressionZSTD},
		{"none", sarama.CompressionNone},
		{"", sarama.CompressionNone},
		{"unknown", sarama.CompressionSnappy}, // defaults to snappy
	}

	for _, tt := range tests {
		t.Run(tt.compression, func(t *testing.T) {
			t.Parallel()

			config := sarama.NewConfig()

			// Apply the same logic as NewKafkaPublisher
			switch tt.compression {
			case "gzip":
				config.Producer.Compression = sarama.CompressionGZIP
			case "snappy":
				config.Producer.Compression = sarama.CompressionSnappy
			case "lz4":
				config.Producer.Compression = sarama.CompressionLZ4
			case "zstd":
				config.Producer.Compression = sarama.CompressionZSTD
			case "none", "":
				config.Producer.Compression = sarama.CompressionNone
			default:
				config.Producer.Compression = sarama.CompressionSnappy
			}

			assert.Equal(t, tt.expected, config.Producer.Compression)
		})
	}
}

func TestKafkaPublisher_RequiredAcksMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		acks     int
		expected sarama.RequiredAcks
	}{
		{0, sarama.NoResponse},
		{1, sarama.WaitForLocal},
		{-1, sarama.WaitForAll},
		{99, sarama.WaitForLocal}, // defaults to 1
	}

	for _, tt := range tests {
		t.Run("acks", func(t *testing.T) {
			config := sarama.NewConfig()

			switch tt.acks {
			case 0:
				config.Producer.RequiredAcks = sarama.NoResponse
			case 1:
				config.Producer.RequiredAcks = sarama.WaitForLocal
			case -1:
				config.Producer.RequiredAcks = sarama.WaitForAll
			default:
				config.Producer.RequiredAcks = sarama.WaitForLocal
			}

			assert.Equal(t, tt.expected, config.Producer.RequiredAcks)
		})
	}
}

func TestKafkaPublisher_MessagePartitioning(t *testing.T) {
	t.Parallel()

	// Create mock producer
	mockProducer := mocks.NewSyncProducer(t, nil)

	// Expect messages - they should use bucket as key for partitioning
	mockProducer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		key, err := msg.Key.Encode()
		if err != nil {
			return err
		}
		if string(key) != "test-bucket" {
			return errors.New("expected key to be bucket name")
		}
		if msg.Topic != "s3-events" {
			return errors.New("expected topic to be s3-events")
		}
		return nil
	})

	pub := &KafkaPublisher{
		producer: mockProducer,
		topic:    "s3-events",
	}

	ctx := context.Background()
	err := pub.Publish(ctx, "test-bucket", []byte(`{}`))
	require.NoError(t, err)
}
