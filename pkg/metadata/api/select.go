// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"time"

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
	if err := xml.NewDecoder(io.LimitReader(d.Req.Body, maxXMLBodySize+1)).Decode(&req); err != nil {
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

	// Parquet does not support compression (it has internal compression)
	if req.InputSerialization.Parquet != nil && req.InputSerialization.CompressionType != "" {
		writeXMLErrorResponse(w, d, s3err.ErrInvalidRequest)
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

	// Wrap with decompression reader if compression type is specified
	var inputReader io.Reader = result.Body
	if req.InputSerialization.CompressionType != "" {
		decompressor, err := s3select.WrapReader(result.Body, req.InputSerialization.CompressionType)
		if err != nil {
			if selectErr, ok := err.(*s3select.SelectError); ok {
				writeSelectError(w, d, selectErr)
				return
			}
			writeXMLErrorResponse(w, d, s3err.ErrInvalidRequest)
			return
		}
		defer decompressor.Close()
		inputReader = decompressor
	}

	// Convert API types to s3select types
	var csvOutput *s3select.CSVOutput
	var jsonOutputOpt s3select.ExecutorOption
	if req.OutputSerialization.JSON != nil {
		jsonOutputOpt = s3select.WithJSONOutput(convertJSONOutput(req.OutputSerialization.JSON))
	} else {
		csvOutput = convertCSVOutput(req.OutputSerialization.CSV)
	}

	// Create reader based on input format
	var reader s3select.RecordReader
	if req.InputSerialization.CSV != nil {
		csvInput := convertCSVInput(req.InputSerialization.CSV)
		reader = s3select.NewCSVReader(inputReader, csvInput)
	} else if req.InputSerialization.JSON != nil {
		jsonInput := convertJSONInput(req.InputSerialization.JSON)
		reader = s3select.NewJSONReader(inputReader, jsonInput)
	} else if req.InputSerialization.Parquet != nil {
		// Parquet requires io.ReaderAt for random access (metadata at end of file)
		// Read entire object into memory
		data, err := io.ReadAll(inputReader)
		if err != nil {
			logger.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("failed to read parquet object")
			writeXMLErrorResponse(w, d, s3err.ErrInternalError)
			return
		}
		parquetReader, err := s3select.NewParquetReader(
			bytes.NewReader(data),
			int64(len(data)),
			nil, // Parquet is self-describing, no options needed
		)
		if err != nil {
			if selectErr, ok := err.(*s3select.SelectError); ok {
				writeSelectError(w, d, selectErr)
				return
			}
			writeXMLErrorResponse(w, d, s3err.ErrInvalidRequest)
			return
		}
		reader = parquetReader
	}
	defer reader.Close()

	// Build executor options
	var opts []s3select.ExecutorOption
	if req.RequestProgress != nil && req.RequestProgress.Enabled {
		opts = append(opts, s3select.WithProgress(true, 0))
	}
	if jsonOutputOpt != nil {
		opts = append(opts, jsonOutputOpt)
	}

	// Create executor
	exec := s3select.NewExecutor(query, reader, csvOutput, opts...)

	// Determine input format for metrics
	inputFormat := s3select.GetInputFormat(
		req.InputSerialization.CSV != nil,
		req.InputSerialization.JSON != nil,
		req.InputSerialization.Parquet != nil,
	)

	// Set response headers for event stream
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	// Execute the query and stream results
	start := time.Now()
	err = exec.Execute(d.Ctx, w)
	if err != nil {
		// Error occurred during execution - at this point headers are already sent
		// The executor should have already written an error event to the stream
		// We just log and return
		logger.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("s3 select execution error")
		s3select.RecordError(inputFormat, "ExecutionError")
	}

	// Record metrics
	bytesScanned, _, bytesReturned, recordsProcessed := exec.Stats()
	s3select.RecordRequest(inputFormat, time.Since(start), bytesScanned, bytesReturned, recordsProcessed, err)
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
	case "CSVParsingError", "JSONParsingError", "ParquetParsingError":
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

// convertJSONOutput converts API SelectJSONOutput to s3select.JSONOutput.
func convertJSONOutput(out *SelectJSONOutput) *s3select.JSONOutput {
	if out == nil {
		return &s3select.JSONOutput{}
	}
	return &s3select.JSONOutput{
		RecordDelimiter: out.RecordDelimiter,
	}
}

// convertJSONInput converts API SelectJSONInput to s3select.JSONInput.
func convertJSONInput(in *SelectJSONInput) *s3select.JSONInput {
	if in == nil {
		return &s3select.JSONInput{}
	}
	return &s3select.JSONInput{
		Type: in.Type,
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
