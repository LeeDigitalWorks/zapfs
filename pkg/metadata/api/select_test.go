// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/s3select"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectObjectContentRequest_Unmarshal(t *testing.T) {
	t.Parallel()

	reqBody := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object WHERE age > 25</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
            <FieldDelimiter>,</FieldDelimiter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
            <FieldDelimiter>,</FieldDelimiter>
        </CSV>
    </OutputSerialization>
</SelectObjectContentRequest>`

	var req SelectObjectContentRequest
	err := xml.NewDecoder(strings.NewReader(reqBody)).Decode(&req)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM s3object WHERE age > 25", req.Expression)
	assert.Equal(t, "SQL", req.ExpressionType)
	require.NotNil(t, req.InputSerialization.CSV)
	assert.Equal(t, "USE", req.InputSerialization.CSV.FileHeaderInfo)
	assert.Equal(t, ",", req.InputSerialization.CSV.FieldDelimiter)
	require.NotNil(t, req.OutputSerialization.CSV)
	assert.Equal(t, ",", req.OutputSerialization.CSV.FieldDelimiter)
	assert.Nil(t, req.RequestProgress)
	assert.Nil(t, req.ScanRange)
}

func TestSelectObjectContentRequest_UnmarshalWithScanRange(t *testing.T) {
	t.Parallel()

	reqBody := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CSV>
            <FileHeaderInfo>NONE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV/>
    </OutputSerialization>
    <ScanRange>
        <Start>1000</Start>
        <End>5000</End>
    </ScanRange>
</SelectObjectContentRequest>`

	var req SelectObjectContentRequest
	err := xml.NewDecoder(strings.NewReader(reqBody)).Decode(&req)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM s3object", req.Expression)
	assert.Equal(t, "SQL", req.ExpressionType)
	require.NotNil(t, req.InputSerialization.CSV)
	assert.Equal(t, "NONE", req.InputSerialization.CSV.FileHeaderInfo)
	require.NotNil(t, req.OutputSerialization.CSV)
	require.NotNil(t, req.ScanRange)
	assert.Equal(t, int64(1000), req.ScanRange.Start)
	assert.Equal(t, int64(5000), req.ScanRange.End)
}

func TestSelectObjectContentRequest_UnmarshalWithRequestProgress(t *testing.T) {
	t.Parallel()

	reqBody := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT name, age FROM s3object WHERE status = 'active'</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
            <RecordDelimiter>\n</RecordDelimiter>
            <FieldDelimiter>,</FieldDelimiter>
            <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
            <RecordDelimiter>\n</RecordDelimiter>
            <FieldDelimiter>,</FieldDelimiter>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>true</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>`

	var req SelectObjectContentRequest
	err := xml.NewDecoder(strings.NewReader(reqBody)).Decode(&req)
	require.NoError(t, err)

	assert.Equal(t, "SELECT name, age FROM s3object WHERE status = 'active'", req.Expression)
	assert.Equal(t, "SQL", req.ExpressionType)
	require.NotNil(t, req.InputSerialization.CSV)
	assert.Equal(t, "USE", req.InputSerialization.CSV.FileHeaderInfo)
	assert.Equal(t, "\\n", req.InputSerialization.CSV.RecordDelimiter)
	assert.Equal(t, ",", req.InputSerialization.CSV.FieldDelimiter)
	assert.Equal(t, "\"", req.InputSerialization.CSV.QuoteCharacter)
	require.NotNil(t, req.OutputSerialization.CSV)
	assert.Equal(t, "\\n", req.OutputSerialization.CSV.RecordDelimiter)
	assert.Equal(t, ",", req.OutputSerialization.CSV.FieldDelimiter)
	require.NotNil(t, req.RequestProgress)
	assert.True(t, req.RequestProgress.Enabled)
}

func TestSelectObjectContentRequest_UnmarshalWithAllCSVOptions(t *testing.T) {
	t.Parallel()

	reqBody := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>GZIP</CompressionType>
        <CSV>
            <FileHeaderInfo>IGNORE</FileHeaderInfo>
            <Comments>#</Comments>
            <QuoteEscapeCharacter>\</QuoteEscapeCharacter>
            <RecordDelimiter>\n</RecordDelimiter>
            <FieldDelimiter>;</FieldDelimiter>
            <QuoteCharacter>'</QuoteCharacter>
            <AllowQuotedRecordDelimiter>true</AllowQuotedRecordDelimiter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
            <QuoteFields>ALWAYS</QuoteFields>
            <QuoteEscapeCharacter>\</QuoteEscapeCharacter>
            <RecordDelimiter>\n</RecordDelimiter>
            <FieldDelimiter>;</FieldDelimiter>
            <QuoteCharacter>'</QuoteCharacter>
        </CSV>
    </OutputSerialization>
</SelectObjectContentRequest>`

	var req SelectObjectContentRequest
	err := xml.NewDecoder(strings.NewReader(reqBody)).Decode(&req)
	require.NoError(t, err)

	// Check input serialization
	assert.Equal(t, "GZIP", req.InputSerialization.CompressionType)
	require.NotNil(t, req.InputSerialization.CSV)
	assert.Equal(t, "IGNORE", req.InputSerialization.CSV.FileHeaderInfo)
	assert.Equal(t, "#", req.InputSerialization.CSV.Comments)
	assert.Equal(t, "\\", req.InputSerialization.CSV.QuoteEscapeCharacter)
	assert.Equal(t, "\\n", req.InputSerialization.CSV.RecordDelimiter)
	assert.Equal(t, ";", req.InputSerialization.CSV.FieldDelimiter)
	assert.Equal(t, "'", req.InputSerialization.CSV.QuoteCharacter)
	assert.True(t, req.InputSerialization.CSV.AllowQuotedRecordDelimiter)

	// Check output serialization
	require.NotNil(t, req.OutputSerialization.CSV)
	assert.Equal(t, "ALWAYS", req.OutputSerialization.CSV.QuoteFields)
	assert.Equal(t, "\\", req.OutputSerialization.CSV.QuoteEscapeCharacter)
	assert.Equal(t, "\\n", req.OutputSerialization.CSV.RecordDelimiter)
	assert.Equal(t, ";", req.OutputSerialization.CSV.FieldDelimiter)
	assert.Equal(t, "'", req.OutputSerialization.CSV.QuoteCharacter)
}

func TestSelectObjectContentRequest_UnmarshalWithJSON(t *testing.T) {
	t.Parallel()

	reqBody := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <JSON>
            <Type>LINES</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
            <RecordDelimiter>\n</RecordDelimiter>
        </JSON>
    </OutputSerialization>
</SelectObjectContentRequest>`

	var req SelectObjectContentRequest
	err := xml.NewDecoder(strings.NewReader(reqBody)).Decode(&req)
	require.NoError(t, err)

	assert.Nil(t, req.InputSerialization.CSV)
	require.NotNil(t, req.InputSerialization.JSON)
	assert.Equal(t, "LINES", req.InputSerialization.JSON.Type)
	assert.Nil(t, req.OutputSerialization.CSV)
	require.NotNil(t, req.OutputSerialization.JSON)
	assert.Equal(t, "\\n", req.OutputSerialization.JSON.RecordDelimiter)
}

func TestSelectObjectContentRequest_UnmarshalWithParquet(t *testing.T) {
	t.Parallel()

	reqBody := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <Parquet/>
    </InputSerialization>
    <OutputSerialization>
        <CSV/>
    </OutputSerialization>
</SelectObjectContentRequest>`

	var req SelectObjectContentRequest
	err := xml.NewDecoder(strings.NewReader(reqBody)).Decode(&req)
	require.NoError(t, err)

	assert.Nil(t, req.InputSerialization.CSV)
	assert.Nil(t, req.InputSerialization.JSON)
	require.NotNil(t, req.InputSerialization.Parquet)
	require.NotNil(t, req.OutputSerialization.CSV)
}

func TestConvertCSVInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *SelectCSVInput
		expected *s3select.CSVInput
	}{
		{
			name: "populated fields",
			input: &SelectCSVInput{
				FileHeaderInfo:             "USE",
				Comments:                   "#",
				QuoteEscapeCharacter:       "\\",
				RecordDelimiter:            "\n",
				FieldDelimiter:             ",",
				QuoteCharacter:             "\"",
				AllowQuotedRecordDelimiter: true,
			},
			expected: &s3select.CSVInput{
				FileHeaderInfo:             "USE",
				Comments:                   "#",
				QuoteEscapeCharacter:       "\\",
				RecordDelimiter:            "\n",
				FieldDelimiter:             ",",
				QuoteCharacter:             "\"",
				AllowQuotedRecordDelimiter: true,
			},
		},
		{
			name: "partial fields",
			input: &SelectCSVInput{
				FileHeaderInfo: "IGNORE",
				FieldDelimiter: ";",
			},
			expected: &s3select.CSVInput{
				FileHeaderInfo: "IGNORE",
				FieldDelimiter: ";",
			},
		},
		{
			name:     "empty struct",
			input:    &SelectCSVInput{},
			expected: &s3select.CSVInput{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := convertCSVInput(tc.input)

			require.NotNil(t, result)
			assert.Equal(t, tc.expected.FileHeaderInfo, result.FileHeaderInfo)
			assert.Equal(t, tc.expected.Comments, result.Comments)
			assert.Equal(t, tc.expected.QuoteEscapeCharacter, result.QuoteEscapeCharacter)
			assert.Equal(t, tc.expected.RecordDelimiter, result.RecordDelimiter)
			assert.Equal(t, tc.expected.FieldDelimiter, result.FieldDelimiter)
			assert.Equal(t, tc.expected.QuoteCharacter, result.QuoteCharacter)
			assert.Equal(t, tc.expected.AllowQuotedRecordDelimiter, result.AllowQuotedRecordDelimiter)
		})
	}
}

func TestConvertCSVInput_Nil(t *testing.T) {
	t.Parallel()

	result := convertCSVInput(nil)

	require.NotNil(t, result, "convertCSVInput should return non-nil for nil input")
	assert.Equal(t, "", result.FileHeaderInfo)
	assert.Equal(t, "", result.Comments)
	assert.Equal(t, "", result.QuoteEscapeCharacter)
	assert.Equal(t, "", result.RecordDelimiter)
	assert.Equal(t, "", result.FieldDelimiter)
	assert.Equal(t, "", result.QuoteCharacter)
	assert.False(t, result.AllowQuotedRecordDelimiter)
}

func TestConvertCSVOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		output   *SelectCSVOutput
		expected *s3select.CSVOutput
	}{
		{
			name: "populated fields",
			output: &SelectCSVOutput{
				QuoteFields:          "ALWAYS",
				QuoteEscapeCharacter: "\\",
				RecordDelimiter:      "\n",
				FieldDelimiter:       ",",
				QuoteCharacter:       "\"",
			},
			expected: &s3select.CSVOutput{
				QuoteFields:          "ALWAYS",
				QuoteEscapeCharacter: "\\",
				RecordDelimiter:      "\n",
				FieldDelimiter:       ",",
				QuoteCharacter:       "\"",
			},
		},
		{
			name: "partial fields",
			output: &SelectCSVOutput{
				QuoteFields:    "ASNEEDED",
				FieldDelimiter: "\t",
			},
			expected: &s3select.CSVOutput{
				QuoteFields:    "ASNEEDED",
				FieldDelimiter: "\t",
			},
		},
		{
			name:     "empty struct",
			output:   &SelectCSVOutput{},
			expected: &s3select.CSVOutput{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := convertCSVOutput(tc.output)

			require.NotNil(t, result)
			assert.Equal(t, tc.expected.QuoteFields, result.QuoteFields)
			assert.Equal(t, tc.expected.QuoteEscapeCharacter, result.QuoteEscapeCharacter)
			assert.Equal(t, tc.expected.RecordDelimiter, result.RecordDelimiter)
			assert.Equal(t, tc.expected.FieldDelimiter, result.FieldDelimiter)
			assert.Equal(t, tc.expected.QuoteCharacter, result.QuoteCharacter)
		})
	}
}

func TestConvertCSVOutput_Nil(t *testing.T) {
	t.Parallel()

	result := convertCSVOutput(nil)

	require.NotNil(t, result, "convertCSVOutput should return non-nil for nil input")
	assert.Equal(t, "", result.QuoteFields)
	assert.Equal(t, "", result.QuoteEscapeCharacter)
	assert.Equal(t, "", result.RecordDelimiter)
	assert.Equal(t, "", result.FieldDelimiter)
	assert.Equal(t, "", result.QuoteCharacter)
}

func TestSelectInputSerialization_XMLParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		xmlInput      string
		expectCSV     bool
		expectJSON    bool
		expectParquet bool
	}{
		{
			name: "CSV only",
			xmlInput: `<InputSerialization>
				<CSV><FileHeaderInfo>USE</FileHeaderInfo></CSV>
			</InputSerialization>`,
			expectCSV:     true,
			expectJSON:    false,
			expectParquet: false,
		},
		{
			name: "JSON only",
			xmlInput: `<InputSerialization>
				<JSON><Type>DOCUMENT</Type></JSON>
			</InputSerialization>`,
			expectCSV:     false,
			expectJSON:    true,
			expectParquet: false,
		},
		{
			name: "Parquet only",
			xmlInput: `<InputSerialization>
				<Parquet/>
			</InputSerialization>`,
			expectCSV:     false,
			expectJSON:    false,
			expectParquet: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var input SelectInputSerialization
			err := xml.NewDecoder(strings.NewReader(tc.xmlInput)).Decode(&input)
			require.NoError(t, err)

			assert.Equal(t, tc.expectCSV, input.CSV != nil)
			assert.Equal(t, tc.expectJSON, input.JSON != nil)
			assert.Equal(t, tc.expectParquet, input.Parquet != nil)
		})
	}
}

func TestSelectOutputSerialization_XMLParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		xmlOutput  string
		expectCSV  bool
		expectJSON bool
	}{
		{
			name: "CSV only",
			xmlOutput: `<OutputSerialization>
				<CSV><FieldDelimiter>,</FieldDelimiter></CSV>
			</OutputSerialization>`,
			expectCSV:  true,
			expectJSON: false,
		},
		{
			name: "JSON only",
			xmlOutput: `<OutputSerialization>
				<JSON><RecordDelimiter>\n</RecordDelimiter></JSON>
			</OutputSerialization>`,
			expectCSV:  false,
			expectJSON: true,
		},
		{
			name: "Empty CSV",
			xmlOutput: `<OutputSerialization>
				<CSV/>
			</OutputSerialization>`,
			expectCSV:  true,
			expectJSON: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var output SelectOutputSerialization
			err := xml.NewDecoder(strings.NewReader(tc.xmlOutput)).Decode(&output)
			require.NoError(t, err)

			assert.Equal(t, tc.expectCSV, output.CSV != nil)
			assert.Equal(t, tc.expectJSON, output.JSON != nil)
		})
	}
}

func TestSelectRequestProgress_XMLParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		xmlInput string
		expected bool
	}{
		{
			name:     "enabled true",
			xmlInput: `<RequestProgress><Enabled>true</Enabled></RequestProgress>`,
			expected: true,
		},
		{
			name:     "enabled false",
			xmlInput: `<RequestProgress><Enabled>false</Enabled></RequestProgress>`,
			expected: false,
		},
		{
			name:     "empty element",
			xmlInput: `<RequestProgress></RequestProgress>`,
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var progress SelectRequestProgress
			err := xml.NewDecoder(strings.NewReader(tc.xmlInput)).Decode(&progress)
			require.NoError(t, err)

			assert.Equal(t, tc.expected, progress.Enabled)
		})
	}
}

func TestSelectScanRange_XMLParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		xmlInput      string
		expectedStart int64
		expectedEnd   int64
	}{
		{
			name:          "basic range",
			xmlInput:      `<ScanRange><Start>0</Start><End>1000</End></ScanRange>`,
			expectedStart: 0,
			expectedEnd:   1000,
		},
		{
			name:          "large range",
			xmlInput:      `<ScanRange><Start>1000000</Start><End>5000000</End></ScanRange>`,
			expectedStart: 1000000,
			expectedEnd:   5000000,
		},
		{
			name:          "start only",
			xmlInput:      `<ScanRange><Start>500</Start></ScanRange>`,
			expectedStart: 500,
			expectedEnd:   0,
		},
		{
			name:          "end only",
			xmlInput:      `<ScanRange><End>1000</End></ScanRange>`,
			expectedStart: 0,
			expectedEnd:   1000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var scanRange SelectScanRange
			err := xml.NewDecoder(strings.NewReader(tc.xmlInput)).Decode(&scanRange)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedStart, scanRange.Start)
			assert.Equal(t, tc.expectedEnd, scanRange.End)
		})
	}
}
