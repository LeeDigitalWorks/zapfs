package filter

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	"zapfs/pkg/s3api/s3action"

	"golang.org/x/text/encoding/charmap"
)

// Match represents a successful S3 operation match.
type Match struct {
	Action s3action.Action
	Bucket string
	Key    string
}

// Router represents an S3 request parser that can parse out the matching
// operation, bucket, and key.
type Router struct {
	routers []router
}

// NewRouter returns a new router using the provided hosts.
func NewRouter(hosts ...string) *Router {
	pr := newPathRouter()
	sr := newServiceRouter()
	ar := newAnyHostRouter()

	// Router order matters - more specific routers should come first
	routers := []router{
		// Any host router - for operations that work on any host (like WriteGetObjectResponse)
		anyHost{r: ar},
	}

	// Service router - matches exact host at root path for ListBuckets
	// Bucket-specific routers (virtual-hosted and path-style)
	for _, host := range hosts {
		routers = append(routers,
			serviceRouter{host: host, r: sr},
			newBucketHost(host, pr),
			newLegacyHost(host, pr),
		)
	}

	// virtualHost is a fallback that matches any host - must be last
	routers = append(routers, virtualHost{pr: pr})

	return &Router{routers: routers}
}

// MatchRequest returns the Match information and true if a request can be successfully
// parsed. Otherwise, it returns an empty Match and false.
func (r *Router) MatchRequest(req *http.Request) (Match, bool) {
	host := getHost(req)
	v := req.URL.Query()

	for _, r := range r.routers {
		if match, ok := r.matchReq(host, req, v); ok {
			if err := normalizeUTF8(&match); err != nil {
				return match, false
			}
			return match, true
		}
	}
	return Match{}, false
}

func normalizeUTF8(m *Match) error {
	var err error
	if m.Bucket, err = getUTF8String(m.Bucket); err != nil {
		return err
	}
	m.Key, err = getUTF8String(m.Key)
	return err
}

type router interface {
	matchReq(host string, r *http.Request, v url.Values) (Match, bool)
}

// legacyHost represents a request with the bucket in the path (path-style).
type legacyHost struct {
	host string
	pr   pathRouter
}

func newLegacyHost(host string, pr pathRouter) legacyHost {
	return legacyHost{
		host: host,
		pr:   pr,
	}
}

func (h legacyHost) matchReq(host string, req *http.Request, v url.Values) (Match, bool) {
	var match Match

	if h.host != host {
		return match, false
	}

	path := strings.TrimPrefix(req.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	if bucket == "" {
		return match, false
	}

	match.Bucket = bucket
	match.Key = key

	var ok bool
	match.Action, ok = h.pr.match(req, v, key)
	return match, ok
}

// bucketHost represents a request with the bucket in the host (virtual-hosted-style).
type bucketHost struct {
	suffix string
	pr     pathRouter
}

func newBucketHost(host string, pr pathRouter) bucketHost {
	return bucketHost{
		suffix: "." + host,
		pr:     pr,
	}
}

func (h bucketHost) matchReq(host string, req *http.Request, v url.Values) (Match, bool) {
	if len(h.suffix) >= len(host) {
		return Match{}, false
	}

	bucket, ok := strings.CutSuffix(host, h.suffix)
	if !ok {
		return Match{}, false
	}
	key := strings.TrimPrefix(req.URL.Path, "/")

	match := Match{Bucket: bucket, Key: key}
	match.Action, ok = h.pr.match(req, v, match.Key)
	return match, ok
}

// virtualHost represents a request with the bucket as the host.
type virtualHost struct {
	pr pathRouter
}

func (h virtualHost) matchReq(host string, req *http.Request, v url.Values) (Match, bool) {
	var match Match

	match.Bucket = host
	match.Key = strings.TrimPrefix(req.URL.Path, "/")

	var ok bool
	match.Action, ok = h.pr.match(req, v, match.Key)
	return match, ok
}

type serviceRouter struct {
	host string
	r    rootPathRouter
}

func (r serviceRouter) matchReq(host string, req *http.Request, v url.Values) (Match, bool) {
	if r.host != host {
		return Match{}, false
	}
	return r.r.match(req, v)
}

type anyHost struct {
	r rootPathRouter
}

func (r anyHost) matchReq(_host string, req *http.Request, v url.Values) (Match, bool) {
	return r.r.match(req, v)
}

func getHost(req *http.Request) string {
	host := req.Host
	if req.URL.IsAbs() {
		host = req.URL.Host
	}
	// Strip any port.
	host, _, _ = strings.Cut(host, ":")
	return host
}

type routes []route

type route struct {
	action s3action.Action
	conds  []condition
}

func (r route) matches(req *http.Request, v url.Values) bool {
	for _, cond := range r.conds {
		if !cond(req, v) {
			return false
		}
	}
	return true
}

type condition func(*http.Request, url.Values) bool

// Condition constructors
func queryExists(key string) condition {
	return func(r *http.Request, v url.Values) bool { return v.Has(key) }
}

func queryEquals(key, value string) condition {
	return func(r *http.Request, v url.Values) bool { return v.Get(key) == value }
}

func headerExists(key string) condition {
	key = http.CanonicalHeaderKey(key)
	return func(r *http.Request, v url.Values) bool {
		_, ok := r.Header[key]
		return ok
	}
}

func headerContains(key, val string) condition {
	key = http.CanonicalHeaderKey(key)
	return func(r *http.Request, _ url.Values) bool {
		return strings.Contains(r.Header.Get(key), val)
	}
}

type pathType uint8

const (
	bucketPath pathType = 0
	keyPath    pathType = 1
)

type pathRouter struct {
	bucketPath methodRouter
	keyPath    methodRouter
}

func (rc *pathRouter) add(method string, path pathType, action s3action.Action, conds ...condition) {
	if path == bucketPath {
		rc.bucketPath.add(method, action, conds...)
	} else {
		rc.keyPath.add(method, action, conds...)
	}
}

func (rc pathRouter) match(req *http.Request, v url.Values, key string) (s3action.Action, bool) {
	var methods methodRouter
	if key != "" {
		methods = rc.keyPath
	} else {
		methods = rc.bucketPath
	}

	return methods.match(req, v)
}

type rootPathRouter struct {
	mr methodRouter
}

func (r rootPathRouter) match(req *http.Request, v url.Values) (Match, bool) {
	if req.URL.Path != "/" {
		return Match{}, false
	}
	action, ok := r.mr.match(req, v)
	if !ok {
		return Match{}, false
	}
	return Match{Action: action}, true
}

type methodRouter struct {
	get     routes
	head    routes
	put     routes
	post    routes
	delete  routes
	options routes
}

func (r *methodRouter) add(method string, action s3action.Action, conds ...condition) {
	rte := route{action: action, conds: conds}
	switch method {
	case http.MethodGet:
		r.get = append(r.get, rte)
	case http.MethodHead:
		r.head = append(r.head, rte)
	case http.MethodPut:
		r.put = append(r.put, rte)
	case http.MethodPost:
		r.post = append(r.post, rte)
	case http.MethodDelete:
		r.delete = append(r.delete, rte)
	case http.MethodOptions:
		r.options = append(r.options, rte)
	default:
		panic(fmt.Sprintf("adding unexpected method: %s", method))
	}
}

func (r methodRouter) match(req *http.Request, v url.Values) (s3action.Action, bool) {
	var rts routes
	switch req.Method {
	case http.MethodGet:
		rts = r.get
	case http.MethodHead:
		rts = r.head
	case http.MethodPut:
		rts = r.put
	case http.MethodPost:
		rts = r.post
	case http.MethodDelete:
		rts = r.delete
	case http.MethodOptions:
		rts = r.options
	}

	for _, rt := range rts {
		if rt.matches(req, v) {
			return rt.action, true
		}
	}
	return s3action.Unknown, false
}

func getUTF8String(s string) (string, error) {
	if utf8.ValidString(s) {
		return s, nil
	}
	return charmap.ISO8859_1.NewDecoder().String(s)
}

// routeDef defines a single route with its conditions
type routeDef struct {
	method string
	path   pathType
	action s3action.Action
	conds  []condition
}

func newPathRouter() pathRouter {
	var pr pathRouter

	routes := []routeDef{
		// CORS preflight
		{http.MethodOptions, bucketPath, s3action.OptionsPreflight, []condition{headerExists("Access-Control-Request-Method"), headerExists("Origin")}},
		{http.MethodOptions, keyPath, s3action.OptionsPreflight, []condition{headerExists("Access-Control-Request-Method"), headerExists("Origin")}},

		// Browser uploads
		{http.MethodPost, bucketPath, s3action.PostObject, []condition{headerContains("Content-Type", "multipart/form-data")}},
		{http.MethodPost, keyPath, s3action.PostObject, []condition{headerContains("Content-Type", "multipart/form-data")}},

		// Bucket operations - multiple query params
		{http.MethodDelete, bucketPath, s3action.DeleteBucketAnalyticsConfiguration, []condition{queryExists("analytics"), queryExists("id")}},
		{http.MethodDelete, bucketPath, s3action.DeleteBucketIntelligentTieringConfiguration, []condition{queryExists("intelligent-tiering"), queryExists("id")}},
		{http.MethodDelete, bucketPath, s3action.DeleteBucketInventoryConfiguration, []condition{queryExists("inventory"), queryExists("id")}},
		{http.MethodDelete, bucketPath, s3action.DeleteBucketMetricsConfiguration, []condition{queryExists("metrics"), queryExists("id")}},
		{http.MethodGet, bucketPath, s3action.GetBucketAnalyticsConfiguration, []condition{queryExists("analytics"), queryExists("id")}},
		{http.MethodGet, bucketPath, s3action.GetBucketIntelligentTieringConfiguration, []condition{queryExists("intelligent-tiering"), queryExists("id")}},
		{http.MethodGet, bucketPath, s3action.GetBucketInventoryConfiguration, []condition{queryExists("inventory"), queryExists("id")}},
		{http.MethodGet, bucketPath, s3action.GetBucketMetricsConfiguration, []condition{queryExists("metrics"), queryExists("id")}},
		{http.MethodPut, bucketPath, s3action.PutBucketAnalyticsConfiguration, []condition{queryExists("analytics"), queryExists("id")}},
		{http.MethodPut, bucketPath, s3action.PutBucketIntelligentTieringConfiguration, []condition{queryExists("intelligent-tiering"), queryExists("id")}},
		{http.MethodPut, bucketPath, s3action.PutBucketInventoryConfiguration, []condition{queryExists("inventory"), queryExists("id")}},
		{http.MethodPut, bucketPath, s3action.PutBucketMetricsConfiguration, []condition{queryExists("metrics"), queryExists("id")}},
		// Bucket operations - single query param
		{http.MethodDelete, bucketPath, s3action.DeleteBucketCors, []condition{queryExists("cors")}},
		{http.MethodDelete, bucketPath, s3action.DeleteBucketEncryption, []condition{queryExists("encryption")}},
		{http.MethodDelete, bucketPath, s3action.DeleteBucketLifecycle, []condition{queryExists("lifecycle")}},
		{http.MethodDelete, bucketPath, s3action.DeleteBucketOwnershipControls, []condition{queryExists("ownershipControls")}},
		{http.MethodDelete, bucketPath, s3action.DeleteBucketPolicy, []condition{queryExists("policy")}},
		{http.MethodDelete, bucketPath, s3action.DeleteBucketReplication, []condition{queryExists("replication")}},
		{http.MethodDelete, bucketPath, s3action.DeleteBucketTagging, []condition{queryExists("tagging")}},
		{http.MethodDelete, bucketPath, s3action.DeleteBucketWebsite, []condition{queryExists("website")}},
		{http.MethodPost, bucketPath, s3action.DeleteObjects, []condition{queryExists("delete")}},
		{http.MethodDelete, bucketPath, s3action.DeletePublicAccessBlock, []condition{queryExists("publicAccessBlock")}},
		{http.MethodGet, bucketPath, s3action.GetBucketAccelerateConfiguration, []condition{queryExists("accelerate")}},
		{http.MethodGet, bucketPath, s3action.GetBucketAcl, []condition{queryExists("acl")}},
		{http.MethodGet, bucketPath, s3action.GetBucketCors, []condition{queryExists("cors")}},
		{http.MethodGet, bucketPath, s3action.GetBucketEncryption, []condition{queryExists("encryption")}},
		{http.MethodGet, bucketPath, s3action.GetBucketLifecycleConfiguration, []condition{queryExists("lifecycle")}},
		{http.MethodGet, bucketPath, s3action.GetBucketLocation, []condition{queryExists("location")}},
		{http.MethodGet, bucketPath, s3action.GetBucketLogging, []condition{queryExists("logging")}},
		{http.MethodGet, bucketPath, s3action.GetBucketNotificationConfiguration, []condition{queryExists("notification")}},
		{http.MethodGet, bucketPath, s3action.GetBucketOwnershipControls, []condition{queryExists("ownershipControls")}},
		{http.MethodGet, bucketPath, s3action.GetBucketPolicy, []condition{queryExists("policy")}},
		{http.MethodGet, bucketPath, s3action.GetBucketPolicyStatus, []condition{queryExists("policyStatus")}},
		{http.MethodGet, bucketPath, s3action.GetBucketReplication, []condition{queryExists("replication")}},
		{http.MethodGet, bucketPath, s3action.GetBucketRequestPayment, []condition{queryExists("requestPayment")}},
		{http.MethodGet, bucketPath, s3action.GetBucketTagging, []condition{queryExists("tagging")}},
		{http.MethodGet, bucketPath, s3action.GetBucketVersioning, []condition{queryExists("versioning")}},
		{http.MethodGet, bucketPath, s3action.GetBucketWebsite, []condition{queryExists("website")}},
		{http.MethodGet, bucketPath, s3action.GetObjectLockConfiguration, []condition{queryExists("object-lock")}},
		{http.MethodGet, bucketPath, s3action.GetPublicAccessBlock, []condition{queryExists("publicAccessBlock")}},
		{http.MethodGet, bucketPath, s3action.ListBucketAnalyticsConfigurations, []condition{queryExists("analytics")}},
		{http.MethodGet, bucketPath, s3action.ListBucketIntelligentTieringConfigurations, []condition{queryExists("intelligent-tiering")}},
		{http.MethodGet, bucketPath, s3action.ListBucketInventoryConfigurations, []condition{queryExists("inventory")}},
		{http.MethodGet, bucketPath, s3action.ListBucketMetricsConfigurations, []condition{queryExists("metrics")}},
		{http.MethodGet, bucketPath, s3action.ListMultipartUploads, []condition{queryExists("uploads")}},
		{http.MethodGet, bucketPath, s3action.ListObjectVersions, []condition{queryExists("versions")}},
		{http.MethodGet, bucketPath, s3action.ListObjectsV2, []condition{queryEquals("list-type", "2")}},
		{http.MethodPut, bucketPath, s3action.PutBucketAccelerateConfiguration, []condition{queryExists("accelerate")}},
		{http.MethodPut, bucketPath, s3action.PutBucketAcl, []condition{queryExists("acl")}},
		{http.MethodPut, bucketPath, s3action.PutBucketCors, []condition{queryExists("cors")}},
		{http.MethodPut, bucketPath, s3action.PutBucketEncryption, []condition{queryExists("encryption")}},
		{http.MethodPut, bucketPath, s3action.PutBucketLifecycleConfiguration, []condition{queryExists("lifecycle")}},
		{http.MethodPut, bucketPath, s3action.PutBucketLogging, []condition{queryExists("logging")}},
		{http.MethodPut, bucketPath, s3action.PutBucketNotificationConfiguration, []condition{queryExists("notification")}},
		{http.MethodPut, bucketPath, s3action.PutBucketOwnershipControls, []condition{queryExists("ownershipControls")}},
		{http.MethodPut, bucketPath, s3action.PutBucketPolicy, []condition{queryExists("policy")}},
		{http.MethodPut, bucketPath, s3action.PutBucketReplication, []condition{queryExists("replication")}},
		{http.MethodPut, bucketPath, s3action.PutBucketRequestPayment, []condition{queryExists("requestPayment")}},
		{http.MethodPut, bucketPath, s3action.PutBucketTagging, []condition{queryExists("tagging")}},
		{http.MethodPut, bucketPath, s3action.PutBucketVersioning, []condition{queryExists("versioning")}},
		{http.MethodPut, bucketPath, s3action.PutBucketWebsite, []condition{queryExists("website")}},
		{http.MethodPut, bucketPath, s3action.PutObjectLockConfiguration, []condition{queryExists("object-lock")}},
		{http.MethodPut, bucketPath, s3action.PutPublicAccessBlock, []condition{queryExists("publicAccessBlock")}},

		// Bucket operations - no params
		{http.MethodPut, bucketPath, s3action.CreateBucket, nil},
		{http.MethodDelete, bucketPath, s3action.DeleteBucket, nil},
		{http.MethodHead, bucketPath, s3action.HeadBucket, nil},
		{http.MethodGet, bucketPath, s3action.ListObjects, nil},

		// Object operations - query params
		{http.MethodDelete, keyPath, s3action.AbortMultipartUpload, []condition{queryExists("uploadId")}},
		{http.MethodDelete, keyPath, s3action.DeleteObjectTagging, []condition{queryExists("tagging")}},
		{http.MethodGet, keyPath, s3action.GetObjectAttributes, []condition{queryExists("attributes"), headerExists("x-amz-object-attributes")}},
		{http.MethodGet, keyPath, s3action.GetObjectAcl, []condition{queryExists("acl")}},
		{http.MethodGet, keyPath, s3action.GetObjectLegalHold, []condition{queryExists("legal-hold")}},
		{http.MethodGet, keyPath, s3action.GetObjectRetention, []condition{queryExists("retention")}},
		{http.MethodGet, keyPath, s3action.GetObjectTagging, []condition{queryExists("tagging")}},
		{http.MethodGet, keyPath, s3action.GetObjectTorrent, []condition{queryExists("torrent")}},
		{http.MethodGet, keyPath, s3action.ListParts, []condition{queryExists("uploadId")}},
		{http.MethodPost, keyPath, s3action.SelectObjectContent, []condition{queryExists("select"), queryEquals("select-type", "2")}},
		{http.MethodPost, keyPath, s3action.CompleteMultipartUpload, []condition{queryExists("uploadId")}},
		{http.MethodPost, keyPath, s3action.CreateMultipartUpload, []condition{queryExists("uploads")}},
		{http.MethodPost, keyPath, s3action.RestoreObject, []condition{queryExists("restore")}},
		{http.MethodPut, keyPath, s3action.UploadPartCopy, []condition{headerExists("x-amz-copy-source"), queryExists("partNumber"), queryExists("uploadId")}},
		{http.MethodPut, keyPath, s3action.UploadPart, []condition{queryExists("partNumber"), queryExists("uploadId")}},
		{http.MethodPut, keyPath, s3action.CopyObject, []condition{headerExists("x-amz-copy-source")}},
		{http.MethodPut, keyPath, s3action.PutObjectAcl, []condition{queryExists("acl")}},
		{http.MethodPut, keyPath, s3action.PutObjectLegalHold, []condition{queryExists("legal-hold")}},
		{http.MethodPut, keyPath, s3action.PutObjectRetention, []condition{queryExists("retention")}},
		{http.MethodPut, keyPath, s3action.PutObjectTagging, []condition{queryExists("tagging")}},

		// Object operations - no params
		{http.MethodDelete, keyPath, s3action.DeleteObject, nil},
		{http.MethodGet, keyPath, s3action.GetObject, nil},
		{http.MethodHead, keyPath, s3action.HeadObject, nil},
		{http.MethodPut, keyPath, s3action.PutObject, nil},
		{http.MethodPost, keyPath, s3action.PostObject, nil},
	}

	for _, r := range routes {
		pr.add(r.method, r.path, r.action, r.conds...)
	}

	return pr
}

func newServiceRouter() rootPathRouter {
	var mr methodRouter

	// ListBuckets operation (service-level)
	mr.add(http.MethodGet, s3action.ListBuckets)
	mr.add(http.MethodHead, s3action.ListBuckets)

	return rootPathRouter{mr: mr}
}

func newAnyHostRouter() rootPathRouter {
	var mr methodRouter

	// WriteGetObjectResponse - special operation that works on any host
	mr.add(http.MethodPost, s3action.WriteGetObjectResponse, headerExists("x-amz-request-route"), headerExists("x-amz-request-token"))

	return rootPathRouter{mr: mr}
}
