package filter_test

import (
	"fmt"
	"net/http"
	"testing"

	"zapfs/pkg/metadata/filter"
	"zapfs/pkg/s3api/s3action"

	"github.com/google/go-cmp/cmp"
)

func TestRouter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		method  string
		host    string
		path    string
		query   string
		headers http.Header

		expectedResult bool
		expectedMatch  filter.Match
	}{
		// Virtual-hosted-style tests
		{
			name:           "Virtual-hosted-style GET Object",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "/mykey",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "mykey",
				Action: s3action.GetObject,
			},
		},
		{
			name:           "Virtual-hosted-style PUT Object",
			method:         http.MethodPut,
			host:           "mybucket.s3.example.com",
			path:           "/path/to/object.txt",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "path/to/object.txt",
				Action: s3action.PutObject,
			},
		},
		{
			name:           "Virtual-hosted-style DELETE Object",
			method:         http.MethodDelete,
			host:           "mybucket.s3.example.com",
			path:           "/mykey",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "mykey",
				Action: s3action.DeleteObject,
			},
		},
		{
			name:           "Virtual-hosted-style HEAD Object",
			method:         http.MethodHead,
			host:           "mybucket.s3.example.com",
			path:           "/mykey",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "mykey",
				Action: s3action.HeadObject,
			},
		},
		{
			name:           "Virtual-hosted-style List Objects",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "/",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.ListObjects,
			},
		},
		{
			name:           "Virtual-hosted-style HEAD Bucket",
			method:         http.MethodHead,
			host:           "mybucket.s3.example.com",
			path:           "/",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.HeadBucket,
			},
		},
		{
			name:           "Virtual-hosted-style Create Bucket",
			method:         http.MethodPut,
			host:           "mybucket.s3.example.com",
			path:           "/",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.CreateBucket,
			},
		},
		{
			name:           "Virtual-hosted-style Delete Bucket",
			method:         http.MethodDelete,
			host:           "mybucket.s3.example.com",
			path:           "/",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.DeleteBucket,
			},
		},

		// Path-style tests
		{
			name:           "Path-style GET Object",
			method:         http.MethodGet,
			host:           "s3.example.com",
			path:           "/mybucket/mykey",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "mykey",
				Action: s3action.GetObject,
			},
		},
		{
			name:           "Path-style PUT Object",
			method:         http.MethodPut,
			host:           "s3.example.com",
			path:           "/mybucket/mykey",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "mykey",
				Action: s3action.PutObject,
			},
		},
		{
			name:           "Path-style DELETE Object",
			method:         http.MethodDelete,
			host:           "s3.example.com",
			path:           "/mybucket/path/to/object",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "path/to/object",
				Action: s3action.DeleteObject,
			},
		},
		{
			name:           "Path-style List Objects",
			method:         http.MethodGet,
			host:           "s3.example.com",
			path:           "/mybucket/",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.ListObjects,
			},
		},
		{
			name:           "Path-style List Buckets",
			method:         http.MethodGet,
			host:           "s3.example.com",
			path:           "/",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "",
				Key:    "",
				Action: s3action.ListBuckets,
			},
		},

		// Multipart upload tests
		{
			name:           "Create Multipart Upload",
			method:         http.MethodPost,
			host:           "mybucket.s3.example.com",
			path:           "/large-file.bin",
			query:          "uploads",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "large-file.bin",
				Action: s3action.CreateMultipartUpload,
			},
		},
		{
			name:           "Upload Part",
			method:         http.MethodPut,
			host:           "mybucket.s3.example.com",
			path:           "/large-file.bin",
			query:          "partNumber=1&uploadId=xyz123",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "large-file.bin",
				Action: s3action.UploadPart,
			},
		},
		{
			name:           "Complete Multipart Upload",
			method:         http.MethodPost,
			host:           "mybucket.s3.example.com",
			path:           "/large-file.bin",
			query:          "uploadId=xyz123",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "large-file.bin",
				Action: s3action.CompleteMultipartUpload,
			},
		},
		{
			name:           "Abort Multipart Upload",
			method:         http.MethodDelete,
			host:           "mybucket.s3.example.com",
			path:           "/large-file.bin",
			query:          "uploadId=xyz123",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "large-file.bin",
				Action: s3action.AbortMultipartUpload,
			},
		},
		{
			name:           "List Parts",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "/large-file.bin",
			query:          "uploadId=xyz123",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "large-file.bin",
				Action: s3action.ListParts,
			},
		},

		// Query parameter operations
		{
			name:           "Get Bucket ACL",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "/",
			query:          "acl",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.GetBucketAcl,
			},
		},
		{
			name:           "Put Bucket ACL",
			method:         http.MethodPut,
			host:           "mybucket.s3.example.com",
			path:           "/",
			query:          "acl",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.PutBucketAcl,
			},
		},
		{
			name:           "Get Bucket Versioning",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "/",
			query:          "versioning",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.GetBucketVersioning,
			},
		},
		{
			name:           "Put Bucket Versioning",
			method:         http.MethodPut,
			host:           "mybucket.s3.example.com",
			path:           "/",
			query:          "versioning",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.PutBucketVersioning,
			},
		},
		{
			name:           "Get Bucket Location",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "/",
			query:          "location",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.GetBucketLocation,
			},
		},

		// Unicode and special characters
		{
			name:           "Unicode key - Latin characters with accents",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "/café/résumé.pdf",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "café/résumé.pdf",
				Action: s3action.GetObject,
			},
		},
		{
			name:           "Unicode key - Spanish characters",
			method:         http.MethodPut,
			host:           "s3.example.com",
			path:           "/mybucket/año/niño.txt",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "año/niño.txt",
				Action: s3action.PutObject,
			},
		},
		{
			name:           "Unicode key - German umlauts",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "/Müller/Größe.doc",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "Müller/Größe.doc",
				Action: s3action.GetObject,
			},
		},
		{
			name:           "Unicode key - French characters",
			method:         http.MethodPut,
			host:           "mybucket.s3.example.com",
			path:           "/français/événement.txt",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "français/événement.txt",
				Action: s3action.PutObject,
			},
		},
		{
			name:           "Unicode key - Nordic characters",
			method:         http.MethodGet,
			host:           "s3.example.com",
			path:           "/mybucket/Åse/Øster/Æble.jpg",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "Åse/Øster/Æble.jpg",
				Action: s3action.GetObject,
			},
		},
		{
			name:           "Unicode key - Eastern European",
			method:         http.MethodPut,
			host:           "mybucket.s3.example.com",
			path:           "/Česká/Řeka.txt",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "Česká/Řeka.txt",
				Action: s3action.PutObject,
			},
		},
		{
			name:           "Special characters - spaces and symbols",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "/my folder/file (1).txt",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "my folder/file (1).txt",
				Action: s3action.GetObject,
			},
		},
		{
			name:           "Special characters - mixed Latin extended",
			method:         http.MethodDelete,
			host:           "s3.example.com",
			path:           "/mybucket/Łódź/Żółć.json",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "Łódź/Żółć.json",
				Action: s3action.DeleteObject,
			},
		},

		// Edge cases
		{
			name:           "Empty key (bucket root)",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.ListObjects,
			},
		},
		{
			name:           "Deep nested path",
			method:         http.MethodPut,
			host:           "mybucket.s3.example.com",
			path:           "/a/b/c/d/e/f/g/file.txt",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "a/b/c/d/e/f/g/file.txt",
				Action: s3action.PutObject,
			},
		},
		{
			name:           "Key with trailing slash",
			method:         http.MethodPut,
			host:           "s3.example.com",
			path:           "/mybucket/folder/",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "folder/",
				Action: s3action.PutObject,
			},
		},
		{
			name:           "Bucket name with dashes",
			method:         http.MethodGet,
			host:           "my-bucket-123.s3.example.com",
			path:           "/my-key",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "my-bucket-123",
				Key:    "my-key",
				Action: s3action.GetObject,
			},
		},
		{
			name:           "POST Object (form upload)",
			method:         http.MethodPost,
			host:           "mybucket.s3.example.com",
			path:           "/upload.txt",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "upload.txt",
				Action: s3action.PostObject,
			},
		},
		{
			name:           "Host with port number",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com:9000",
			path:           "/mykey",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "mykey",
				Action: s3action.GetObject,
			},
		},
		{
			name:           "Alternate base domain",
			method:         http.MethodGet,
			host:           "mybucket.static.example.com",
			path:           "/image.png",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "image.png",
				Action: s3action.GetObject,
			},
		},

		// Negative cases
		{
			name:           "Unknown host (treated as virtual-hosted bucket)",
			method:         http.MethodGet,
			host:           "unknown.example.com",
			path:           "/mykey",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "unknown.example.com",
				Key:    "mykey",
				Action: s3action.GetObject,
			},
		},
		{
			name:           "Unsupported HTTP method - PATCH",
			method:         "PATCH",
			host:           "mybucket.s3.example.com",
			path:           "/mykey",
			expectedResult: false,
		},
		{
			name:           "Unsupported HTTP method - CONNECT",
			method:         "CONNECT",
			host:           "s3.example.com",
			path:           "/bucket/key",
			expectedResult: false,
		},
		{
			name:           "Unsupported HTTP method - TRACE",
			method:         "TRACE",
			host:           "mybucket.s3.example.com",
			path:           "/",
			expectedResult: false,
		},
		{
			name:           "Invalid method name",
			method:         "INVALID",
			host:           "s3.example.com",
			path:           "/bucket",
			expectedResult: false,
		},
		{
			name:           "Empty method (falls back to GET behavior)",
			method:         "",
			host:           "mybucket.s3.example.com",
			path:           "/key",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "key",
				Action: s3action.GetObject,
			},
		},
		{
			name:           "Empty path on service host (treated as virtual-hosted)",
			method:         http.MethodGet,
			host:           "s3.example.com",
			path:           "",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "s3.example.com",
				Key:    "",
				Action: s3action.ListObjects,
			},
		},
		{
			name:           "Invalid query parameter combination - missing uploadId",
			method:         http.MethodPut,
			host:           "mybucket.s3.example.com",
			path:           "/key",
			query:          "partNumber=1",
			expectedResult: true, // Falls back to PutObject
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "key",
				Action: s3action.PutObject,
			},
		},
		{
			name:           "Invalid query parameter combination - missing partNumber",
			method:         http.MethodPut,
			host:           "mybucket.s3.example.com",
			path:           "/key",
			query:          "uploadId=xyz",
			expectedResult: true, // Falls back to PutObject
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "key",
				Action: s3action.PutObject,
			},
		},
		{
			name:           "Query with metrics but no id - lists all metrics configs",
			method:         http.MethodGet,
			host:           "mybucket.s3.example.com",
			path:           "/",
			query:          "metrics",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.ListBucketMetricsConfigurations,
			},
		},
		{
			name:           "Invalid CORS preflight - missing Origin",
			method:         "OPTIONS",
			host:           "mybucket.s3.example.com",
			path:           "/",
			headers:        http.Header{"Access-Control-Request-Method": []string{"GET"}},
			expectedResult: false, // Requires both headers
		},
		{
			name:           "Invalid CORS preflight - missing Access-Control-Request-Method",
			method:         "OPTIONS",
			host:           "mybucket.s3.example.com",
			path:           "/key",
			headers:        http.Header{"Origin": []string{"https://example.com"}},
			expectedResult: false, // Requires both headers
		},
		{
			name:           "POST without multipart header should still work",
			method:         http.MethodPost,
			host:           "mybucket.s3.example.com",
			path:           "/key",
			expectedResult: true,
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "key",
				Action: s3action.PostObject,
			},
		},
		{
			name:           "DELETE with query but wrong method",
			method:         http.MethodGet, // Should be DELETE
			host:           "mybucket.s3.example.com",
			path:           "/",
			query:          "cors",
			expectedResult: true, // Falls back to GetBucketCors
			expectedMatch: filter.Match{
				Bucket: "mybucket",
				Key:    "",
				Action: s3action.GetBucketCors,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := filter.NewRouter("s3.example.com", "static.example.com")
			urlStr := fmt.Sprintf("http://%s%s", tt.host, tt.path)
			if tt.query != "" {
				urlStr += "?" + tt.query
			}
			req, err := http.NewRequest(tt.method, urlStr, nil)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}
			req.Header = tt.headers

			match, ok := rt.MatchRequest(req)
			if ok != tt.expectedResult {
				t.Errorf("Expected result %v, got %v", tt.expectedResult, ok)
			}
			if ok {
				if !cmp.Equal(tt.expectedMatch, match) {
					t.Errorf("Expected match %+v, got %+v\nDiff: %s", tt.expectedMatch, match, cmp.Diff(tt.expectedMatch, match))
				}
			}
		})
	}
}
