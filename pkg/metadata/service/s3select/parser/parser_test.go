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

func TestParseCAST_S3Types(t *testing.T) {
	p := New()

	tests := []struct {
		sql          string
		desc         string
		expectedType string
	}{
		{"SELECT CAST(age AS INT) FROM s3object", "CAST AS INT", "int"},
		{"SELECT CAST(age AS INTEGER) FROM s3object", "CAST AS INTEGER", "int"},
		{"SELECT CAST(val AS FLOAT) FROM s3object", "CAST AS FLOAT", "decimal"},
		{"SELECT CAST(val AS DOUBLE) FROM s3object", "CAST AS DOUBLE", "decimal"},
		{"SELECT CAST(val AS BOOL) FROM s3object", "CAST AS BOOL", "int"},
		{"SELECT CAST(val AS BOOLEAN) FROM s3object", "CAST AS BOOLEAN", "int"},
		{"SELECT CAST(val AS STRING) FROM s3object", "CAST AS STRING", "string"},
		{"SELECT CAST(val AS VARCHAR) FROM s3object", "CAST AS VARCHAR", "string"},
		{"SELECT CAST(val AS TIMESTAMP) FROM s3object", "CAST AS TIMESTAMP", "timestamp"},
		{"SELECT CAST(age AS DECIMAL) FROM s3object", "CAST AS DECIMAL (native)", "decimal"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			query, err := p.Parse(tt.sql)
			require.NoError(t, err, "should parse: %s", tt.sql)
			require.NotNil(t, query)
			require.Len(t, query.Projections, 1)
			fc, ok := query.Projections[0].Expr.(*s3select.FunctionCall)
			require.True(t, ok, "projection should be FunctionCall")
			assert.Equal(t, "CAST", fc.Name)
			require.Len(t, fc.Args, 2)
			typeLit, ok := fc.Args[1].(*s3select.Literal)
			require.True(t, ok)
			assert.Equal(t, tt.expectedType, typeLit.Value)
		})
	}
}

func TestParser_WhereWithCAST(t *testing.T) {
	p := New()

	// This is the exact query from the integration test
	query, err := p.Parse("SELECT name, age FROM s3object WHERE CAST(age AS INT) > 28")
	require.NoError(t, err)
	require.NotNil(t, query.Where)
	assert.Len(t, query.Projections, 2)

	// WHERE clause should be: CAST(age AS INT) > 28
	binOp, ok := query.Where.(*s3select.BinaryOp)
	require.True(t, ok)
	assert.Equal(t, ">", binOp.Op)

	// Left side should be CAST function call
	fc, ok := binOp.Left.(*s3select.FunctionCall)
	require.True(t, ok)
	assert.Equal(t, "CAST", fc.Name)
	require.Len(t, fc.Args, 2)

	// CAST arg 1: column ref "age"
	col, ok := fc.Args[0].(*s3select.ColumnRef)
	require.True(t, ok)
	assert.Equal(t, "age", col.Name)

	// CAST arg 2: type "int" (mapped from INT via SIGNED)
	typeLit, ok := fc.Args[1].(*s3select.Literal)
	require.True(t, ok)
	assert.Equal(t, "int", typeLit.Value)

	// Right side should be literal 28
	lit, ok := binOp.Right.(*s3select.Literal)
	require.True(t, ok)
	assert.Equal(t, int64(28), lit.Value)
}

func TestNormalizeCASTTypes(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			"SELECT CAST(age AS INT) FROM s3object",
			"SELECT CAST(age AS SIGNED) FROM s3object",
		},
		{
			"SELECT * FROM s3object WHERE CAST(age AS INTEGER) > 28",
			"SELECT * FROM s3object WHERE CAST(age AS SIGNED) > 28",
		},
		{
			"SELECT CAST(val AS FLOAT) FROM s3object",
			"SELECT CAST(val AS DECIMAL) FROM s3object",
		},
		{
			"SELECT CAST(val AS STRING) FROM s3object",
			"SELECT CAST(val AS CHAR) FROM s3object",
		},
		{
			"SELECT CAST(val AS TIMESTAMP) FROM s3object",
			"SELECT CAST(val AS DATETIME) FROM s3object",
		},
		{
			// DECIMAL is native MySQL, should not be changed
			"SELECT CAST(val AS DECIMAL) FROM s3object",
			"SELECT CAST(val AS DECIMAL) FROM s3object",
		},
		{
			// Column alias "AS integer" (no closing paren) should not be affected
			"SELECT name AS integer FROM s3object",
			"SELECT name AS integer FROM s3object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeCASTTypes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
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
