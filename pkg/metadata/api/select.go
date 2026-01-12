// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/xml"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/object"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/s3select"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/s3select/parser"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// ============================================================================
// S3 Select Handler
// ============================================================================

// SelectObjectContentHandler performs S3 Select queries on objects.
// POST /{bucket}/{key}?select&select-type=2
//
// S3 Select allows using SQL to filter and project data from CSV, JSON, and Parquet objects.
// This reduces data transfer by only returning the requested subset of data.
//
// Request Body (XML):
//
//	<SelectObjectContentRequest>
//	  <Expression>SELECT * FROM s3object WHERE age > 25</Expression>
//	  <ExpressionType>SQL</ExpressionType>
//	  <InputSerialization>
//	    <CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV>
//	  </InputSerialization>
//	  <OutputSerialization>
//	    <CSV/>
//	  </OutputSerialization>
//	</SelectObjectContentRequest>
//
// Response: Event stream with Records, Stats, Progress, and End events.
func (s *MetadataServer) SelectObjectContentHandler(d *data.Data, w http.ResponseWriter) {
	bucket := d.S3Info.Bucket
	key := d.S3Info.Key

	// Parse request body
	var req SelectObjectContentRequest
	if err := xml.NewDecoder(d.Req.Body).Decode(&req); err != nil {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Validate required fields
	if req.Expression == "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidRequest)
		return
	}

	if req.ExpressionType != "SQL" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidRequest)
		return
	}

	// Validate input serialization
	if req.InputSerialization.CSV == nil && req.InputSerialization.JSON == nil && req.InputSerialization.Parquet == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidRequest)
		return
	}

	// Validate output serialization
	if req.OutputSerialization.CSV == nil && req.OutputSerialization.JSON == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidRequest)
		return
	}

	// Phase 1: Only support CSV input/output
	// JSON and Parquet support will be added in future phases
	if req.InputSerialization.JSON != nil {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}
	if req.InputSerialization.Parquet != nil {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}
	if req.OutputSerialization.JSON != nil {
		writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
		return
	}

	// Parse the SQL expression
	p := parser.New()
	query, err := p.Parse(req.Expression)
	if err != nil {
		if selectErr, ok := err.(*s3select.SelectError); ok {
			writeSelectError(w, d, selectErr)
			return
		}
		writeXMLErrorResponse(w, d, s3err.ErrInvalidRequest)
		return
	}

	// Get the object from storage
	result, err := s.svc.Objects().GetObject(d.Ctx, &object.GetObjectRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		s.handleObjectError(w, d, err)
		return
	}
	defer result.Body.Close()

	// Convert API types to s3select types
	csvInput := convertCSVInput(req.InputSerialization.CSV)
	csvOutput := convertCSVOutput(req.OutputSerialization.CSV)

	// Create CSV reader
	reader := s3select.NewCSVReader(result.Body, csvInput)
	defer reader.Close()

	// Create executor
	exec := s3select.NewExecutor(query, reader, csvOutput)

	// Set response headers for event stream
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	// Execute the query and stream results
	if err := exec.Execute(d.Ctx, w); err != nil {
		// Error occurred during execution - at this point headers are already sent
		// The executor should have already written an error event to the stream
		// We just log and return
		logger.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("s3 select execution error")
	}
}

// writeSelectError converts a SelectError to an S3 error response.
func writeSelectError(w http.ResponseWriter, d *data.Data, err *s3select.SelectError) {
	// Map SelectError codes to S3 error codes
	var s3Code s3err.ErrorCode
	switch err.Code {
	case "InvalidQuery", "InvalidExpression":
		s3Code = s3err.ErrInvalidRequest
	case "UnsupportedSyntax":
		s3Code = s3err.ErrNotImplemented
	case "CSVParsingError":
		s3Code = s3err.ErrInvalidRequest
	case "UnsupportedFormat":
		s3Code = s3err.ErrNotImplemented
	default:
		s3Code = s3err.ErrInternalError
	}
	writeXMLErrorResponse(w, d, s3Code)
}

// convertCSVInput converts API SelectCSVInput to s3select.CSVInput.
func convertCSVInput(in *SelectCSVInput) *s3select.CSVInput {
	if in == nil {
		return &s3select.CSVInput{}
	}
	return &s3select.CSVInput{
		FileHeaderInfo:             in.FileHeaderInfo,
		Comments:                   in.Comments,
		QuoteEscapeCharacter:       in.QuoteEscapeCharacter,
		RecordDelimiter:            in.RecordDelimiter,
		FieldDelimiter:             in.FieldDelimiter,
		QuoteCharacter:             in.QuoteCharacter,
		AllowQuotedRecordDelimiter: in.AllowQuotedRecordDelimiter,
	}
}

// convertCSVOutput converts API SelectCSVOutput to s3select.CSVOutput.
func convertCSVOutput(out *SelectCSVOutput) *s3select.CSVOutput {
	if out == nil {
		return &s3select.CSVOutput{}
	}
	return &s3select.CSVOutput{
		QuoteFields:          out.QuoteFields,
		QuoteEscapeCharacter: out.QuoteEscapeCharacter,
		RecordDelimiter:      out.RecordDelimiter,
		FieldDelimiter:       out.FieldDelimiter,
		QuoteCharacter:       out.QuoteCharacter,
	}
}

// ============================================================================
// Request/Response Types
// ============================================================================

// SelectObjectContentRequest is the XML request body for SelectObjectContent.
type SelectObjectContentRequest struct {
	XMLName             xml.Name                  `xml:"SelectObjectContentRequest"`
	Expression          string                    `xml:"Expression"`
	ExpressionType      string                    `xml:"ExpressionType"`
	InputSerialization  SelectInputSerialization  `xml:"InputSerialization"`
	OutputSerialization SelectOutputSerialization `xml:"OutputSerialization"`
	RequestProgress     *SelectRequestProgress    `xml:"RequestProgress,omitempty"`
	ScanRange           *SelectScanRange          `xml:"ScanRange,omitempty"`
}

// SelectInputSerialization defines input data format.
type SelectInputSerialization struct {
	CompressionType string              `xml:"CompressionType,omitempty"` // NONE, GZIP, BZIP2
	CSV             *SelectCSVInput     `xml:"CSV,omitempty"`
	JSON            *SelectJSONInput    `xml:"JSON,omitempty"`
	Parquet         *SelectParquetInput `xml:"Parquet,omitempty"`
}

// SelectCSVInput defines CSV input options.
type SelectCSVInput struct {
	FileHeaderInfo             string `xml:"FileHeaderInfo,omitempty"`             // USE, IGNORE, NONE
	Comments                   string `xml:"Comments,omitempty"`                   // Comment character
	QuoteEscapeCharacter       string `xml:"QuoteEscapeCharacter,omitempty"`       // Quote escape char
	RecordDelimiter            string `xml:"RecordDelimiter,omitempty"`            // Record delimiter
	FieldDelimiter             string `xml:"FieldDelimiter,omitempty"`             // Field delimiter
	QuoteCharacter             string `xml:"QuoteCharacter,omitempty"`             // Quote char
	AllowQuotedRecordDelimiter bool   `xml:"AllowQuotedRecordDelimiter,omitempty"` // Allow delim in quotes
}

// SelectJSONInput defines JSON input options.
type SelectJSONInput struct {
	Type string `xml:"Type,omitempty"` // DOCUMENT or LINES
}

// SelectParquetInput defines Parquet input options.
type SelectParquetInput struct {
	// Parquet is self-describing, no options needed
}

// SelectOutputSerialization defines output data format.
type SelectOutputSerialization struct {
	CSV  *SelectCSVOutput  `xml:"CSV,omitempty"`
	JSON *SelectJSONOutput `xml:"JSON,omitempty"`
}

// SelectCSVOutput defines CSV output options.
type SelectCSVOutput struct {
	QuoteFields          string `xml:"QuoteFields,omitempty"`          // ALWAYS, ASNEEDED
	QuoteEscapeCharacter string `xml:"QuoteEscapeCharacter,omitempty"` // Quote escape char
	RecordDelimiter      string `xml:"RecordDelimiter,omitempty"`      // Record delimiter
	FieldDelimiter       string `xml:"FieldDelimiter,omitempty"`       // Field delimiter
	QuoteCharacter       string `xml:"QuoteCharacter,omitempty"`       // Quote char
}

// SelectJSONOutput defines JSON output options.
type SelectJSONOutput struct {
	RecordDelimiter string `xml:"RecordDelimiter,omitempty"` // Record delimiter
}

// SelectRequestProgress enables progress reporting.
type SelectRequestProgress struct {
	Enabled bool `xml:"Enabled"`
}

// SelectScanRange specifies byte range to scan.
type SelectScanRange struct {
	Start int64 `xml:"Start"`
	End   int64 `xml:"End"`
}
