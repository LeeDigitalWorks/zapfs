// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package parser

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/s3select"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_SelectStar(t *testing.T) {
	p := New()
	query, err := p.Parse("SELECT * FROM s3object")
	require.NoError(t, err)
	require.NotNil(t, query)
	assert.Len(t, query.Projections, 1)
	_, isStar := query.Projections[0].Expr.(*s3select.StarExpr)
	assert.True(t, isStar)
	assert.Equal(t, "s3object", query.FromAlias)
}

func TestParser_SelectColumns(t *testing.T) {
	p := New()
	query, err := p.Parse("SELECT name, age FROM s3object")
	require.NoError(t, err)
	assert.Len(t, query.Projections, 2)
}

func TestParser_SelectWithAlias(t *testing.T) {
	p := New()
	query, err := p.Parse("SELECT * FROM s3object s")
	require.NoError(t, err)
	assert.Equal(t, "s", query.FromAlias)
}

func TestParser_RejectsNonSelect(t *testing.T) {
	p := New()
	_, err := p.Parse("INSERT INTO s3object VALUES (1)")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only SELECT")
}

func TestParser_RejectsWrongTable(t *testing.T) {
	p := New()
	_, err := p.Parse("SELECT * FROM othertable")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "s3object")
}
