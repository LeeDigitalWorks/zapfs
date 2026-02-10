// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildHTMLErrorPage_EscapesHTML(t *testing.T) {
	page := buildHTMLErrorPage("<script>alert(1)</script>", "test&\"msg<br>", 400)
	assert.NotContains(t, page, "<script>")
	assert.NotContains(t, page, "<br>")
	assert.Contains(t, page, "&lt;script&gt;")
	assert.Contains(t, page, "&lt;br&gt;")
}
