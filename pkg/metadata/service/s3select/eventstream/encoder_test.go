// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package eventstream

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_WriteRecords(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	err := enc.WriteRecords([]byte("Alice,30\n"))
	require.NoError(t, err)

	// Decode and verify
	dec := eventstream.NewDecoder()
	msg, err := dec.Decode(&buf, nil)
	require.NoError(t, err)

	var eventType string
	for _, h := range msg.Headers {
		if h.Name == ":event-type" {
			eventType = h.Value.String()
		}
	}
	assert.Equal(t, "Records", eventType)
	assert.Equal(t, []byte("Alice,30\n"), msg.Payload)
}

func TestEncoder_WriteStats(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	err := enc.WriteStats(1000, 800, 200)
	require.NoError(t, err)

	dec := eventstream.NewDecoder()
	msg, err := dec.Decode(&buf, nil)
	require.NoError(t, err)

	var eventType string
	for _, h := range msg.Headers {
		if h.Name == ":event-type" {
			eventType = h.Value.String()
		}
	}
	assert.Equal(t, "Stats", eventType)
	assert.Contains(t, string(msg.Payload), "BytesScanned")
	assert.Contains(t, string(msg.Payload), "1000")
}

func TestEncoder_WriteEnd(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	err := enc.WriteEnd()
	require.NoError(t, err)

	dec := eventstream.NewDecoder()
	msg, err := dec.Decode(&buf, nil)
	require.NoError(t, err)

	var eventType string
	for _, h := range msg.Headers {
		if h.Name == ":event-type" {
			eventType = h.Value.String()
		}
	}
	assert.Equal(t, "End", eventType)
}

func TestEncoder_WriteError(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	err := enc.WriteError("InvalidQuery", "bad SQL")
	require.NoError(t, err)

	dec := eventstream.NewDecoder()
	msg, err := dec.Decode(&buf, nil)
	require.NoError(t, err)

	var msgType, errCode string
	for _, h := range msg.Headers {
		if h.Name == ":message-type" {
			msgType = h.Value.String()
		}
		if h.Name == ":error-code" {
			errCode = h.Value.String()
		}
	}
	assert.Equal(t, "error", msgType)
	assert.Equal(t, "InvalidQuery", errCode)
}
