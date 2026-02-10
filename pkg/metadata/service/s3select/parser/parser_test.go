// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package parser

import (
	"strings"
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

func TestParser_WhereClause(t *testing.T) {
	p := New()
	query, err := p.Parse("SELECT * FROM s3object WHERE age > 25")
	require.NoError(t, err)
	require.NotNil(t, query.Where)

	binOp, ok := query.Where.(*s3select.BinaryOp)
	require.True(t, ok)
	assert.Equal(t, ">", binOp.Op)
}

func TestParser_WhereWithAnd(t *testing.T) {
	p := New()
	query, err := p.Parse("SELECT * FROM s3object WHERE age > 25 AND name = 'Alice'")
	require.NoError(t, err)
	require.NotNil(t, query.Where)

	binOp, ok := query.Where.(*s3select.BinaryOp)
	require.True(t, ok)
	assert.Equal(t, "and", strings.ToLower(binOp.Op))
}

func TestParser_WhereWithOr(t *testing.T) {
	p := New()
	query, err := p.Parse("SELECT * FROM s3object WHERE age < 20 OR age > 60")
	require.NoError(t, err)
	require.NotNil(t, query.Where)
}

func TestParser_StringLiteral(t *testing.T) {
	p := New()
	query, err := p.Parse("SELECT * FROM s3object WHERE name = 'Alice'")
	require.NoError(t, err)

	binOp := query.Where.(*s3select.BinaryOp)
	lit := binOp.Right.(*s3select.Literal)
	assert.Equal(t, "Alice", lit.Value)
}

func TestParser_Limit(t *testing.T) {
	p := New()
	query, err := p.Parse("SELECT * FROM s3object LIMIT 100")
	require.NoError(t, err)
	assert.Equal(t, int64(100), query.Limit)
}

func TestParser_RejectsGroupBy(t *testing.T) {
	p := New()
	_, err := p.Parse("SELECT name, COUNT(*) FROM s3object GROUP BY name")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GROUP BY")
}

func TestParser_RejectsOrderBy(t *testing.T) {
	p := New()
	_, err := p.Parse("SELECT * FROM s3object ORDER BY name")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ORDER BY")
}

func TestParser_RejectsHaving(t *testing.T) {
	p := New()
	_, err := p.Parse("SELECT * FROM s3object HAVING count > 1")
	assert.Error(t, err)
}

func TestParseFunctionCall(t *testing.T) {
	p := New()

	tests := []struct {
		sql  string
		desc string
	}{
		{"SELECT LOWER(name) FROM s3object", "LOWER function"},
		{"SELECT UPPER(name) FROM s3object", "UPPER function"},
		{"SELECT CAST(age AS DECIMAL) FROM s3object", "CAST function"},
		{"SELECT COUNT(*) FROM s3object", "COUNT aggregate"},
		{"SELECT TRIM(name) FROM s3object", "TRIM function"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			query, err := p.Parse(tt.sql)
			assert.NoError(t, err, "should parse: %s", tt.sql)
			assert.NotNil(t, query)
			assert.Len(t, query.Projections, 1)
			_, ok := query.Projections[0].Expr.(*s3select.FunctionCall)
			assert.True(t, ok, "projection should be FunctionCall for: %s", tt.sql)
		})
	}
}

func TestParseFunctionCall_Args(t *testing.T) {
	p := New()

	t.Run("LOWER has one arg", func(t *testing.T) {
		query, err := p.Parse("SELECT LOWER(name) FROM s3object")
		require.NoError(t, err)
		fc := query.Projections[0].Expr.(*s3select.FunctionCall)
		assert.Equal(t, "LOWER", fc.Name)
		assert.Len(t, fc.Args, 1)
		_, isCol := fc.Args[0].(*s3select.ColumnRef)
		assert.True(t, isCol)
	})

	t.Run("COUNT(*) has star arg", func(t *testing.T) {
		query, err := p.Parse("SELECT COUNT(*) FROM s3object")
		require.NoError(t, err)
		fc := query.Projections[0].Expr.(*s3select.FunctionCall)
		assert.Equal(t, "COUNT", fc.Name)
		assert.Len(t, fc.Args, 1)
		_, isStar := fc.Args[0].(*s3select.StarExpr)
		assert.True(t, isStar)
	})

	t.Run("CAST has two args", func(t *testing.T) {
		query, err := p.Parse("SELECT CAST(age AS DECIMAL) FROM s3object")
		require.NoError(t, err)
		fc := query.Projections[0].Expr.(*s3select.FunctionCall)
		assert.Equal(t, "CAST", fc.Name)
		assert.Len(t, fc.Args, 2)
		// First arg is the expression, second is the type name
		_, isCol := fc.Args[0].(*s3select.ColumnRef)
		assert.True(t, isCol)
		typeLit, isLit := fc.Args[1].(*s3select.Literal)
		assert.True(t, isLit)
		assert.Equal(t, "decimal", typeLit.Value)
	})
}

func TestParseUnaryExpr(t *testing.T) {
	p := New()

	query, err := p.Parse("SELECT * FROM s3object WHERE NOT age > 25")
	require.NoError(t, err)
	require.NotNil(t, query.Where)

	unary, ok := query.Where.(*s3select.UnaryOp)
	require.True(t, ok)
	assert.Equal(t, "NOT", strings.ToUpper(unary.Op))
}

func TestParseNullLiteral(t *testing.T) {
	p := New()

	query, err := p.Parse("SELECT * FROM s3object WHERE name = null")
	require.NoError(t, err)
	require.NotNil(t, query.Where)

	binOp, ok := query.Where.(*s3select.BinaryOp)
	require.True(t, ok)
	lit, ok := binOp.Right.(*s3select.Literal)
	require.True(t, ok)
	assert.Nil(t, lit.Value)
}
