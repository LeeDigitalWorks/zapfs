//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package events

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/events"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// KafkaPublisher publishes events to Kafka using sarama.
type KafkaPublisher struct {
	producer sarama.SyncProducer
	topic    string
}

// KafkaConfig configures the Kafka publisher.
type KafkaConfig struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string

	// Topic is the Kafka topic for events (default: "s3-events").
	Topic string

	// RequiredAcks: 0=none, 1=leader, -1=all (default: 1).
	RequiredAcks int

	// Compression: "none", "gzip", "snappy", "lz4", "zstd" (default: "snappy").
	Compression string

	// BatchSize is the maximum messages per batch (default: 100).
	BatchSize int

	// BatchTimeout is the maximum time to wait for a batch (default: 1s).
	BatchTimeout time.Duration

	// WriteTimeout is the timeout for write operations (default: 10s).
	WriteTimeout time.Duration

	// TLS enables TLS for broker connections.
	TLS bool

	// TLSSkipVerify skips TLS certificate verification (for testing).
	TLSSkipVerify bool

	// SASLEnabled enables SASL authentication.
	SASLEnabled bool

	// SASLMechanism is the SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
	SASLMechanism string

	// SASLUsername is the SASL username.
	SASLUsername string

	// SASLPassword is the SASL password.
	SASLPassword string
}

// DefaultKafkaConfig returns a KafkaConfig with sensible defaults.
func DefaultKafkaConfig(brokers []string) KafkaConfig {
	return KafkaConfig{
		Brokers:      brokers,
		Topic:        "s3-events",
		RequiredAcks: 1,
		Compression:  "snappy",
		BatchSize:    100,
		BatchTimeout: time.Second,
		WriteTimeout: 10 * time.Second,
	}
}

// NewKafkaPublisher creates a new Kafka publisher using sarama.
func NewKafkaPublisher(cfg KafkaConfig) (*KafkaPublisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("at least one Kafka broker is required")
	}

	if cfg.Topic == "" {
		cfg.Topic = "s3-events"
	}

	// Create sarama config
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Set RequiredAcks
	switch cfg.RequiredAcks {
	case 0:
		config.Producer.RequiredAcks = sarama.NoResponse
	case 1:
		config.Producer.RequiredAcks = sarama.WaitForLocal
	case -1:
		config.Producer.RequiredAcks = sarama.WaitForAll
	default:
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}

	// Set compression
	switch cfg.Compression {
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

	// Set batching
	if cfg.BatchSize > 0 {
		config.Producer.Flush.MaxMessages = cfg.BatchSize
	}
	if cfg.BatchTimeout > 0 {
		config.Producer.Flush.Frequency = cfg.BatchTimeout
	}

	// Set timeout
	if cfg.WriteTimeout > 0 {
		config.Producer.Timeout = cfg.WriteTimeout
		config.Net.WriteTimeout = cfg.WriteTimeout
		config.Net.ReadTimeout = cfg.WriteTimeout
	}

	// Configure TLS
	if cfg.TLS {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: cfg.TLSSkipVerify,
		}
	}

	// Configure SASL
	if cfg.SASLEnabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = cfg.SASLUsername
		config.Net.SASL.Password = cfg.SASLPassword

		switch cfg.SASLMechanism {
		case "SCRAM-SHA-256":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &scramClient{mechanism: scram.SHA256}
			}
		case "SCRAM-SHA-512":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &scramClient{mechanism: scram.SHA512}
			}
		default:
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}

	// Use hash partitioner for consistent bucket ordering
	config.Producer.Partitioner = sarama.NewHashPartitioner

	// Create producer
	producer, err := sarama.NewSyncProducer(cfg.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("kafka producer creation failed: %w", err)
	}

	logger.Info().
		Strs("brokers", cfg.Brokers).
		Str("topic", cfg.Topic).
		Str("compression", cfg.Compression).
		Int("required_acks", cfg.RequiredAcks).
		Msg("kafka event publisher connected")

	return &KafkaPublisher{
		producer: producer,
		topic:    cfg.Topic,
	}, nil
}

// Name returns the publisher identifier.
func (p *KafkaPublisher) Name() string {
	return "kafka"
}

// Publish sends an event to Kafka.
// The bucket name is used as the message key for consistent partitioning.
func (p *KafkaPublisher) Publish(ctx context.Context, bucket string, eventData []byte) error {
	start := time.Now()

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(bucket), // Partition by bucket for ordering
		Value: sarama.ByteEncoder(eventData),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("kafka publish: %w", err)
	}

	events.EventsDeliveryDuration.WithLabelValues("kafka").Observe(time.Since(start).Seconds())

	logger.Debug().
		Str("topic", p.topic).
		Str("bucket", bucket).
		Int32("partition", partition).
		Int64("offset", offset).
		Int("size", len(eventData)).
		Msg("published event to kafka")

	return nil
}

// Close closes the Kafka producer.
func (p *KafkaPublisher) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}

// scramClient implements the sarama.SCRAMClient interface for SCRAM authentication.
type scramClient struct {
	mechanism     scram.HashGeneratorFcn
	conversation  *scram.ClientConversation
}

func (c *scramClient) Begin(userName, password, authzID string) error {
	client, err := c.mechanism.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.conversation = client.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) {
	return c.conversation.Step(challenge)
}

func (c *scramClient) Done() bool {
	return c.conversation.Done()
}
