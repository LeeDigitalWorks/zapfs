// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package data

import (
	"context"
	"io"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3action"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

type Data struct {
	Ctx      context.Context
	Req      *http.Request
	S3Info   *S3Info
	Identity *iam.Identity // Authenticated user identity (nil for anonymous)

	// VerifiedBody is set for streaming signed requests (aws-chunked).
	// When set, handlers should read from this instead of Req.Body to get
	// chunk-signature-verified data. The original request body is wrapped
	// in a ChunkReader that verifies each chunk's signature.
	VerifiedBody io.Reader

	// IsWebsiteRequest indicates this request is for static website hosting.
	// When true, errors should be returned as HTML instead of XML.
	IsWebsiteRequest bool

	// ResponseWriter allows filters to write HTTP responses directly.
	// Set by the server before invoking the filter chain.
	ResponseWriter http.ResponseWriter

	// Federation fields - set by FederationFilter when bucket is federated.
	// FederationConfig contains the external S3 connection details.
	FederationConfig *s3types.FederationConfig

	// FederationExtConfig is the client pool config derived from FederationConfig.
	// Used by handlers for lazy migration fetches.
	FederationExtConfig interface{}
}

func NewData(ctx context.Context, req *http.Request) *Data {
	return &Data{
		Ctx:    ctx,
		Req:    req,
		S3Info: &S3Info{},
	}
}

type S3Info struct {
	Bucket    string
	Key       string
	Action    s3action.Action
	OwnerID   string
	AccessKey string
	SecretKey string
}
