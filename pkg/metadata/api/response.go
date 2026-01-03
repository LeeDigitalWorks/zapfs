// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"encoding/xml"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

type wrappedResponseRecorder struct {
	http.ResponseWriter
	statusCode    int
	bytesWritten  int64
	wroteHeader   bool
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

// writeXMLResponse writes an XML response with proper headers.
// Used by GET handlers to return bucket/object configuration.
func writeXMLResponse(w http.ResponseWriter, d *data.Data, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(v)
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
