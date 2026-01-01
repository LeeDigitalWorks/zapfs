package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()

	assert.False(t, cfg.Enabled)

	// Redis defaults
	assert.False(t, cfg.Redis.Enabled)
	assert.Equal(t, "localhost:6379", cfg.Redis.Addr)
	assert.Equal(t, 0, cfg.Redis.DB)
	assert.Equal(t, "s3:events", cfg.Redis.Channel)
	assert.Equal(t, 10, cfg.Redis.PoolSize)

	// Kafka defaults
	assert.False(t, cfg.Kafka.Enabled)
	assert.Equal(t, "s3-events", cfg.Kafka.Topic)
	assert.Equal(t, 1, cfg.Kafka.RequiredAcks)
	assert.Equal(t, "snappy", cfg.Kafka.Compression)
	assert.Equal(t, 100, cfg.Kafka.BatchSize)
	assert.Equal(t, time.Second, cfg.Kafka.BatchTimeout)

	// Webhook defaults
	assert.False(t, cfg.Webhook.Enabled)
	assert.Equal(t, 30*time.Second, cfg.Webhook.Timeout)
	assert.Equal(t, 3, cfg.Webhook.MaxRetries)
	assert.Equal(t, time.Second, cfg.Webhook.RetryDelay)
	assert.Equal(t, "ZapFS/1.0", cfg.Webhook.UserAgent)
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	t.Run("applies defaults for empty values", func(t *testing.T) {
		t.Parallel()

		cfg := Config{}
		cfg.Validate()

		// Redis defaults
		assert.Equal(t, "localhost:6379", cfg.Redis.Addr)
		assert.Equal(t, "s3:events", cfg.Redis.Channel)
		assert.Equal(t, 10, cfg.Redis.PoolSize)

		// Kafka defaults
		assert.Equal(t, "s3-events", cfg.Kafka.Topic)
		// RequiredAcks=0 is valid (no acks required), so it's not overwritten
		assert.Equal(t, 0, cfg.Kafka.RequiredAcks)
		assert.Equal(t, "snappy", cfg.Kafka.Compression)
		assert.Equal(t, 100, cfg.Kafka.BatchSize)
		assert.Equal(t, time.Second, cfg.Kafka.BatchTimeout)

		// Webhook defaults
		assert.Equal(t, 30*time.Second, cfg.Webhook.Timeout)
		// MaxRetries=0 is valid (no retries), so it's not overwritten
		assert.Equal(t, 0, cfg.Webhook.MaxRetries)
		assert.Equal(t, time.Second, cfg.Webhook.RetryDelay)
		assert.Equal(t, "ZapFS/1.0", cfg.Webhook.UserAgent)
	})

	t.Run("preserves valid custom values", func(t *testing.T) {
		t.Parallel()

		cfg := Config{
			Enabled: true,
			Redis: RedisConfig{
				Enabled:  true,
				Addr:     "redis.example.com:6379",
				Channel:  "custom:events",
				PoolSize: 20,
			},
			Kafka: KafkaConfig{
				Enabled:      true,
				Topic:        "custom-topic",
				RequiredAcks: -1,
				Compression:  "gzip",
				BatchSize:    200,
				BatchTimeout: 2 * time.Second,
			},
			Webhook: WebhookConfig{
				Enabled:    true,
				Timeout:    60 * time.Second,
				MaxRetries: 5,
				RetryDelay: 2 * time.Second,
				UserAgent:  "CustomAgent/1.0",
			},
		}
		cfg.Validate()

		// Custom values preserved
		assert.Equal(t, "redis.example.com:6379", cfg.Redis.Addr)
		assert.Equal(t, "custom:events", cfg.Redis.Channel)
		assert.Equal(t, 20, cfg.Redis.PoolSize)

		assert.Equal(t, "custom-topic", cfg.Kafka.Topic)
		assert.Equal(t, -1, cfg.Kafka.RequiredAcks)
		assert.Equal(t, "gzip", cfg.Kafka.Compression)
		assert.Equal(t, 200, cfg.Kafka.BatchSize)
		assert.Equal(t, 2*time.Second, cfg.Kafka.BatchTimeout)

		assert.Equal(t, 60*time.Second, cfg.Webhook.Timeout)
		assert.Equal(t, 5, cfg.Webhook.MaxRetries)
		assert.Equal(t, 2*time.Second, cfg.Webhook.RetryDelay)
		assert.Equal(t, "CustomAgent/1.0", cfg.Webhook.UserAgent)
	})

	t.Run("fixes invalid RequiredAcks", func(t *testing.T) {
		t.Parallel()

		cfg := Config{
			Kafka: KafkaConfig{
				RequiredAcks: 5, // Invalid
			},
		}
		cfg.Validate()
		assert.Equal(t, 1, cfg.Kafka.RequiredAcks)
	})

	t.Run("fixes negative values", func(t *testing.T) {
		t.Parallel()

		cfg := Config{
			Redis: RedisConfig{
				PoolSize: -1,
			},
			Kafka: KafkaConfig{
				BatchSize:    -1,
				BatchTimeout: -1,
			},
			Webhook: WebhookConfig{
				Timeout:    -1,
				MaxRetries: -1,
				RetryDelay: -1,
			},
		}
		cfg.Validate()

		assert.Equal(t, 10, cfg.Redis.PoolSize)
		assert.Equal(t, 100, cfg.Kafka.BatchSize)
		assert.Equal(t, time.Second, cfg.Kafka.BatchTimeout)
		assert.Equal(t, 30*time.Second, cfg.Webhook.Timeout)
		assert.Equal(t, 3, cfg.Webhook.MaxRetries)
		assert.Equal(t, time.Second, cfg.Webhook.RetryDelay)
	})
}

func TestConfigHasPublishers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      Config
		expected bool
	}{
		{
			name:     "no publishers",
			cfg:      Config{},
			expected: false,
		},
		{
			name: "redis enabled",
			cfg: Config{
				Redis: RedisConfig{Enabled: true},
			},
			expected: true,
		},
		{
			name: "kafka enabled",
			cfg: Config{
				Kafka: KafkaConfig{Enabled: true},
			},
			expected: true,
		},
		{
			name: "webhook enabled",
			cfg: Config{
				Webhook: WebhookConfig{Enabled: true},
			},
			expected: true,
		},
		{
			name: "all enabled",
			cfg: Config{
				Redis:   RedisConfig{Enabled: true},
				Kafka:   KafkaConfig{Enabled: true},
				Webhook: WebhookConfig{Enabled: true},
			},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, tc.cfg.HasPublishers())
		})
	}
}
