package api

import (
	"bytes"
	"encoding/xml"
	"net/http"

	"zapfs/pkg/metadata/data"
	"zapfs/pkg/s3api/s3consts"
	"zapfs/pkg/s3api/s3err"
)

type wrappedResponseRecorder struct {
	http.ResponseWriter
	statusCode int
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
