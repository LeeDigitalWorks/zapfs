// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package eventstream

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
)

// Encoder encodes S3 Select events.
type Encoder struct {
	writer  io.Writer
	encoder *eventstream.Encoder
}

// NewEncoder creates a new event stream encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		writer:  w,
		encoder: eventstream.NewEncoder(),
	}
}

// WriteRecords writes a Records event.
func (e *Encoder) WriteRecords(payload []byte) error {
	msg := eventstream.Message{
		Headers: eventstream.Headers{
			{Name: ":event-type", Value: eventstream.StringValue("Records")},
			{Name: ":content-type", Value: eventstream.StringValue("application/octet-stream")},
			{Name: ":message-type", Value: eventstream.StringValue("event")},
		},
		Payload: payload,
	}
	return e.encoder.Encode(e.writer, msg)
}

// WriteStats writes a Stats event.
func (e *Encoder) WriteStats(bytesScanned, bytesProcessed, bytesReturned int64) error {
	payload := fmt.Sprintf(
		`<Stats><BytesScanned>%d</BytesScanned><BytesProcessed>%d</BytesProcessed><BytesReturned>%d</BytesReturned></Stats>`,
		bytesScanned, bytesProcessed, bytesReturned,
	)
	msg := eventstream.Message{
		Headers: eventstream.Headers{
			{Name: ":event-type", Value: eventstream.StringValue("Stats")},
			{Name: ":content-type", Value: eventstream.StringValue("text/xml")},
			{Name: ":message-type", Value: eventstream.StringValue("event")},
		},
		Payload: []byte(payload),
	}
	return e.encoder.Encode(e.writer, msg)
}

// WriteProgress writes a Progress event.
func (e *Encoder) WriteProgress(bytesScanned, bytesProcessed, bytesReturned int64) error {
	payload := fmt.Sprintf(
		`<Progress><BytesScanned>%d</BytesScanned><BytesProcessed>%d</BytesProcessed><BytesReturned>%d</BytesReturned></Progress>`,
		bytesScanned, bytesProcessed, bytesReturned,
	)
	msg := eventstream.Message{
		Headers: eventstream.Headers{
			{Name: ":event-type", Value: eventstream.StringValue("Progress")},
			{Name: ":content-type", Value: eventstream.StringValue("text/xml")},
			{Name: ":message-type", Value: eventstream.StringValue("event")},
		},
		Payload: []byte(payload),
	}
	return e.encoder.Encode(e.writer, msg)
}

// WriteContinuation writes a Cont (keep-alive) event.
func (e *Encoder) WriteContinuation() error {
	msg := eventstream.Message{
		Headers: eventstream.Headers{
			{Name: ":event-type", Value: eventstream.StringValue("Cont")},
			{Name: ":message-type", Value: eventstream.StringValue("event")},
		},
	}
	return e.encoder.Encode(e.writer, msg)
}

// WriteEnd writes an End event.
func (e *Encoder) WriteEnd() error {
	msg := eventstream.Message{
		Headers: eventstream.Headers{
			{Name: ":event-type", Value: eventstream.StringValue("End")},
			{Name: ":message-type", Value: eventstream.StringValue("event")},
		},
	}
	return e.encoder.Encode(e.writer, msg)
}

// WriteError writes an error event.
func (e *Encoder) WriteError(code, message string) error {
	msg := eventstream.Message{
		Headers: eventstream.Headers{
			{Name: ":error-code", Value: eventstream.StringValue(code)},
			{Name: ":error-message", Value: eventstream.StringValue(message)},
			{Name: ":message-type", Value: eventstream.StringValue("error")},
		},
	}
	return e.encoder.Encode(e.writer, msg)
}
