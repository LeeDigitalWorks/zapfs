//go:build enterprise

package accesslog

import (
	_ "embed"
)

//go:embed schema.sql
var schemaSQL string

// SchemaSQL returns the ClickHouse schema SQL.
func SchemaSQL() string {
	return schemaSQL
}
