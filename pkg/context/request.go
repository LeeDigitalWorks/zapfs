// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package context

import (
	"context"

	"github.com/google/uuid"
)

const (
	RequestKey = "zapfs-request-id"
)

type RequestID struct{}

func WithUUID(c context.Context) (context.Context, string) {
	if id := c.Value(RequestID{}); id != nil {
		return c, id.(string)
	}
	newID := uuid.New().String()
	c = context.WithValue(c, RequestID{}, newID)
	return c, newID
}

func FromUUID(c context.Context, reqID string) context.Context {
	return context.WithValue(c, RequestID{}, reqID)
}
