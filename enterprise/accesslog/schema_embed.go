//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

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
