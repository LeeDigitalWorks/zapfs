// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

type ParserFilter struct {
	router *Router
}

func NewParserFilter(hosts ...string) *ParserFilter {
	return &ParserFilter{
		router: NewRouter(hosts...),
	}
}

func (f *ParserFilter) Type() string {
	return "parser"
}

func (f *ParserFilter) Run(d *data.Data) (Response, error) {
	if d.Ctx.Err() != nil {
		return nil, d.Ctx.Err()
	}

	match, ok := f.router.MatchRequest(d.Req)
	if !ok {
		return End{}, s3err.ErrInvalidRequest
	}
	d.S3Info.Action = match.Action
	d.S3Info.Bucket = match.Bucket
	d.S3Info.Key = match.Key

	return Next{}, nil
}
