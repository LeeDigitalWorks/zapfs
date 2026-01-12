// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package parser

import (
	"fmt"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/s3select"
	"github.com/xwb1989/sqlparser"
)

// Parser parses S3 Select SQL expressions.
type Parser struct{}

// New creates a new S3 Select parser.
func New() *Parser {
	return &Parser{}
}

// Parse parses an S3 Select SQL expression.
func (p *Parser) Parse(sql string) (*s3select.Query, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, &s3select.SelectError{
			Code:    "InvalidQuery",
			Message: fmt.Sprintf("SQL parse error: %v", err),
		}
	}

	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, &s3select.SelectError{
			Code:    "InvalidQuery",
			Message: "only SELECT statements are supported",
		}
	}

	query := &s3select.Query{}

	// Parse FROM clause
	if len(sel.From) != 1 {
		return nil, &s3select.SelectError{
			Code:    "InvalidQuery",
			Message: "exactly one FROM table required",
		}
	}

	tableName, alias := p.parseTableExpr(sel.From[0])
	if !strings.EqualFold(tableName, "s3object") {
		return nil, &s3select.SelectError{
			Code:    "InvalidQuery",
			Message: "FROM clause must reference s3object",
		}
	}
	if alias != "" {
		query.FromAlias = alias
	} else {
		query.FromAlias = tableName
	}

	// Reject unsupported clauses
	if sel.GroupBy != nil {
		return nil, &s3select.SelectError{
			Code:    "UnsupportedSyntax",
			Message: "GROUP BY is not supported",
		}
	}
	if sel.OrderBy != nil {
		return nil, &s3select.SelectError{
			Code:    "UnsupportedSyntax",
			Message: "ORDER BY is not supported",
		}
	}
	if sel.Having != nil {
		return nil, &s3select.SelectError{
			Code:    "UnsupportedSyntax",
			Message: "HAVING is not supported",
		}
	}

	// Parse SELECT projections
	for _, expr := range sel.SelectExprs {
		proj, err := p.parseSelectExpr(expr)
		if err != nil {
			return nil, err
		}
		query.Projections = append(query.Projections, proj)
	}

	// Parse WHERE clause
	if sel.Where != nil {
		where, err := p.parseExpr(sel.Where.Expr)
		if err != nil {
			return nil, err
		}
		query.Where = where
	}

	// Parse LIMIT clause
	if sel.Limit != nil && sel.Limit.Rowcount != nil {
		if val, ok := sel.Limit.Rowcount.(*sqlparser.SQLVal); ok {
			fmt.Sscanf(string(val.Val), "%d", &query.Limit)
		}
	}

	return query, nil
}

func (p *Parser) parseTableExpr(expr sqlparser.TableExpr) (name, alias string) {
	switch t := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tbl, ok := t.Expr.(sqlparser.TableName); ok {
			name = tbl.Name.String()
		}
		alias = t.As.String()
	}
	return
}

func (p *Parser) parseSelectExpr(expr sqlparser.SelectExpr) (s3select.Projection, error) {
	switch e := expr.(type) {
	case *sqlparser.StarExpr:
		return s3select.Projection{Expr: &s3select.StarExpr{}}, nil
	case *sqlparser.AliasedExpr:
		parsed, err := p.parseExpr(e.Expr)
		if err != nil {
			return s3select.Projection{}, err
		}
		return s3select.Projection{
			Expr:  parsed,
			Alias: e.As.String(),
		}, nil
	default:
		return s3select.Projection{}, &s3select.SelectError{
			Code:    "InvalidQuery",
			Message: fmt.Sprintf("unsupported select expression: %T", expr),
		}
	}
}

func (p *Parser) parseExpr(expr sqlparser.Expr) (s3select.Expression, error) {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		return &s3select.ColumnRef{Name: e.Name.String()}, nil

	case *sqlparser.SQLVal:
		return p.parseLiteral(e)

	case *sqlparser.ComparisonExpr:
		left, err := p.parseExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.parseExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return &s3select.BinaryOp{
			Left:  left,
			Op:    e.Operator,
			Right: right,
		}, nil

	case *sqlparser.AndExpr:
		left, err := p.parseExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.parseExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return &s3select.BinaryOp{Left: left, Op: "AND", Right: right}, nil

	case *sqlparser.OrExpr:
		left, err := p.parseExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.parseExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return &s3select.BinaryOp{Left: left, Op: "OR", Right: right}, nil

	case *sqlparser.ParenExpr:
		return p.parseExpr(e.Expr)

	default:
		return nil, &s3select.SelectError{
			Code:    "InvalidQuery",
			Message: fmt.Sprintf("unsupported expression: %T", expr),
		}
	}
}

func (p *Parser) parseLiteral(val *sqlparser.SQLVal) (s3select.Expression, error) {
	switch val.Type {
	case sqlparser.StrVal:
		return &s3select.Literal{Value: string(val.Val)}, nil
	case sqlparser.IntVal:
		var i int64
		fmt.Sscanf(string(val.Val), "%d", &i)
		return &s3select.Literal{Value: i}, nil
	case sqlparser.FloatVal:
		var f float64
		fmt.Sscanf(string(val.Val), "%f", &f)
		return &s3select.Literal{Value: f}, nil
	default:
		return &s3select.Literal{Value: string(val.Val)}, nil
	}
}
