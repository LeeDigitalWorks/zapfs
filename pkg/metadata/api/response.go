// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"encoding/xml"
	"html"
	"io"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

const (
	maxXMLBodySize = 1 << 20 // 1 MB
)

type wrappedResponseRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
	wroteHeader  bool
}

func (w *wrappedResponseRecorder) WriteHeader(code int) {
	if !w.wroteHeader {
		w.statusCode = code
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *wrappedResponseRecorder) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytesWritten += int64(n)
	return n, err
}

func writeXMLErrorResponse(w http.ResponseWriter, d *data.Data, s3code s3err.ErrorCode) {
	w.Header().Set("Content-Type", "application/xml")
	var bytesBuffer bytes.Buffer
	e := xml.NewEncoder(&bytesBuffer)

	s3error := s3code.ToErrorResponse(d.S3Info.Bucket)
	if d.Req.Header.Get(s3consts.XAmzRequestID) != "" {
		s3error.RequestID = d.Req.Header.Get(s3consts.XAmzRequestID)
	} else {
		s3error.RequestID = "NotAvailable"
	}

	e.Encode(s3error)

	w.WriteHeader(s3error.HTTPCode)
	if len(bytesBuffer.Bytes()) > 0 {
		w.Write(bytesBuffer.Bytes())
	}
}

// writeHTMLErrorResponse writes an HTML error page for website hosting requests.
func writeHTMLErrorResponse(w http.ResponseWriter, d *data.Data, s3code s3err.ErrorCode) {
	s3error := s3code.ToErrorResponse(d.S3Info.Bucket)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(s3error.HTTPCode)

	html := buildHTMLErrorPage(s3error.Code, s3error.Message, s3error.HTTPCode)
	w.Write([]byte(html))
}

// buildHTMLErrorPage generates an HTML error page.
func buildHTMLErrorPage(code, message string, httpCode int) string {
	return `<!DOCTYPE html>
<html>
<head>
    <title>` + html.EscapeString(http.StatusText(httpCode)) + `</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 40px; }
        h1 { color: #c00; }
        .error-code { color: #666; font-size: 14px; }
    </style>
</head>
<body>
    <h1>` + html.EscapeString(http.StatusText(httpCode)) + `</h1>
    <p>` + html.EscapeString(message) + `</p>
    <p class="error-code">Error Code: ` + html.EscapeString(code) + `</p>
</body>
</html>`
}

// writeErrorResponse writes an error response, choosing HTML or XML based on request type.
func writeErrorResponse(w http.ResponseWriter, d *data.Data, s3code s3err.ErrorCode) {
	if d.IsWebsiteRequest {
		writeHTMLErrorResponse(w, d, s3code)
	} else {
		writeXMLErrorResponse(w, d, s3code)
	}
}

// ============================================================================
// Shared Handler Utilities
// ============================================================================

// S3ErrorConverter is an interface for domain errors that can be converted to S3 errors.
type S3ErrorConverter interface {
	error
	ToS3Error() s3err.ErrorCode
}

// handleServiceError handles domain errors by converting them to S3 error responses.
// Returns true if an error was handled (response written), false otherwise.
func handleServiceError(w http.ResponseWriter, d *data.Data, err error) bool {
	if err == nil {
		return false
	}

	// Try to convert to S3 error via interface
	if converter, ok := err.(S3ErrorConverter); ok {
		writeXMLErrorResponse(w, d, converter.ToS3Error())
		return true
	}

	// Default to internal error
	writeXMLErrorResponse(w, d, s3err.ErrInternalError)
	return true
}

// writeXMLResponse writes an XML response with standard S3 headers.
func writeXMLResponse(w http.ResponseWriter, d *data.Data, statusCode int, body any) {
	w.Header().Set("Content-Type", "application/xml")
	if reqID := d.Req.Header.Get(s3consts.XAmzRequestID); reqID != "" {
		w.Header().Set(s3consts.XAmzRequestID, reqID)
	}
	w.WriteHeader(statusCode)
	if body != nil {
		xml.NewEncoder(w).Encode(body)
	}
}

// writeNoContent writes a 204 No Content response with standard S3 headers.
func writeNoContent(w http.ResponseWriter, d *data.Data) {
	if reqID := d.Req.Header.Get(s3consts.XAmzRequestID); reqID != "" {
		w.Header().Set(s3consts.XAmzRequestID, reqID)
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseXMLBody reads and parses the request body as XML into the provided target.
// Returns the parsed error code if parsing fails, or ErrNone on success.
func parseXMLBody(d *data.Data, target any) s3err.ErrorCode {
	body, err := io.ReadAll(io.LimitReader(d.Req.Body, maxXMLBodySize+1))
	if err != nil {
		return s3err.ErrMalformedXML
	}
	if len(body) == 0 {
		return s3err.ErrMalformedXML
	}
	if int64(len(body)) > maxXMLBodySize {
		return s3err.ErrEntityTooLarge
	}

	if err := xml.Unmarshal(body, target); err != nil {
		return s3err.ErrMalformedXML
	}

	return s3err.ErrNone
}
