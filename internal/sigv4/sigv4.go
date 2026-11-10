// Package sigv4 verifies AWS Signature Version 4 on S3 requests: the
// Authorization header form, presigned URLs (query form), and the
// per-chunk signatures of signed streaming uploads
// (STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]).
//
// Credentials are static access-key/secret pairs. The region in a
// request's credential scope is accepted as given (this store has one
// region), but it is part of the signature, so it cannot be altered.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	algorithm   = "AWS4-HMAC-SHA256"
	amzDate     = "20060102T150405Z"
	scopeSuffix = "aws4_request"
	service     = "s3"

	// UnsignedPayload is the x-amz-content-sha256 value for unsigned bodies.
	UnsignedPayload = "UNSIGNED-PAYLOAD"
	// StreamingPayload and StreamingPayloadTrailer mark signed aws-chunked bodies.
	StreamingPayload        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	StreamingPayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

	// MaxSkew is how far a request's timestamp may be from the server clock.
	MaxSkew = 15 * time.Minute
	// MaxPresignExpiry is S3's limit for presigned URLs (7 days).
	MaxPresignExpiry = 7 * 24 * time.Hour
)

// emptySHA256 is hex(sha256("")).
const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// Error is an authentication failure with its S3 error code.
type Error struct {
	Code    string
	Message string
	Status  int
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func newErr(code string, status int, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Status: status}
}

func errMalformed(format string, args ...any) *Error {
	return newErr("AuthorizationHeaderMalformed", http.StatusBadRequest, format, args...)
}

// Verifier checks signatures against a fixed set of credentials.
type Verifier struct {
	// Credentials maps access key IDs to secret keys.
	Credentials map[string]string
	// Now is the server clock (nil: time.Now).
	Now func() time.Time
}

// Result describes a verified request.
type Result struct {
	AccessKey string
	// Stream is non-nil for signed streaming uploads; the body decoder must
	// check every chunk signature with it.
	Stream *Stream
}

// Signed reports whether r carries SigV4 authentication at all.
func Signed(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Authorization"), algorithm) || r.URL.Query().Has("X-Amz-Signature")
}

func (v *Verifier) now() time.Time {
	if v.Now != nil {
		return v.Now()
	}
	return time.Now()
}

// signingParams is what both auth forms resolve to.
type signingParams struct {
	accessKey     string
	date          time.Time
	scope         string // yyyymmdd/region/s3/aws4_request
	signedHeaders []string
	signature     string
	payloadHash   string
	presigned     bool
}

// Verify checks r. Anonymous requests fail with AccessDenied.
func (v *Verifier) Verify(r *http.Request) (*Result, error) {
	var p signingParams
	var err *Error
	switch {
	case strings.HasPrefix(r.Header.Get("Authorization"), algorithm):
		p, err = v.parseHeader(r)
	case r.URL.Query().Has("X-Amz-Signature"):
		p, err = v.parsePresigned(r)
	case r.Header.Get("Authorization") != "":
		return nil, newErr("AccessDenied", http.StatusForbidden, "only AWS Signature Version 4 is supported")
	default:
		return nil, newErr("AccessDenied", http.StatusForbidden, "Access Denied")
	}
	if err != nil {
		return nil, err
	}
	secret, ok := v.Credentials[p.accessKey]
	if !ok {
		return nil, newErr("InvalidAccessKeyId", http.StatusForbidden, "The AWS Access Key Id you provided does not exist in our records.")
	}

	canonical := canonicalRequest(r, p)
	key := SigningKey(secret, p.scope)
	want := hex.EncodeToString(hmacSHA256(key, stringToSign(p.date, p.scope, canonical)))
	if subtle.ConstantTimeCompare([]byte(want), []byte(strings.ToLower(p.signature))) != 1 {
		return nil, newErr("SignatureDoesNotMatch", http.StatusForbidden,
			"The request signature we calculated does not match the signature you provided. Check your key and signing method.")
	}

	res := &Result{AccessKey: p.accessKey}
	if p.payloadHash == StreamingPayload || p.payloadHash == StreamingPayloadTrailer {
		res.Stream = &Stream{key: key, scope: p.scope, date: p.date.UTC().Format(amzDate), prev: want}
	}
	return res, nil
}

func (v *Verifier) parseHeader(r *http.Request) (signingParams, *Error) {
	var p signingParams
	fields := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), algorithm))
	parts := map[string]string{}
	for _, f := range strings.Split(fields, ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(f), "=")
		if !ok {
			return p, errMalformed("the authorization header is malformed")
		}
		parts[k] = val
	}
	cred, signed, sig := parts["Credential"], parts["SignedHeaders"], parts["Signature"]
	if cred == "" || signed == "" || sig == "" {
		return p, errMalformed("the authorization header is missing Credential, SignedHeaders or Signature")
	}
	dateStr := r.Header.Get("X-Amz-Date")
	if dateStr == "" {
		return p, newErr("AccessDenied", http.StatusForbidden, "AWS authentication requires a valid Date or x-amz-date header")
	}
	date, err := time.Parse(amzDate, dateStr)
	if err != nil {
		return p, newErr("AccessDenied", http.StatusForbidden, "invalid x-amz-date %q", dateStr)
	}
	if skew := v.now().Sub(date); skew > MaxSkew || skew < -MaxSkew {
		return p, newErr("RequestTimeTooSkewed", http.StatusForbidden, "The difference between the request time and the current time is too large.")
	}
	p.date, p.signature = date, sig
	p.signedHeaders = strings.Split(signed, ";")
	if e := parseCredential(cred, date, &p); e != nil {
		return p, e
	}
	p.payloadHash = r.Header.Get("X-Amz-Content-Sha256")
	if p.payloadHash == "" {
		return p, newErr("InvalidRequest", http.StatusBadRequest, "Missing required header for this request: x-amz-content-sha256")
	}
	return p, nil
}

func (v *Verifier) parsePresigned(r *http.Request) (signingParams, *Error) {
	var p signingParams
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != algorithm {
		return p, newErr("AuthorizationQueryParametersError", http.StatusBadRequest, "X-Amz-Algorithm only supports %q", algorithm)
	}
	date, err := time.Parse(amzDate, q.Get("X-Amz-Date"))
	if err != nil {
		return p, newErr("AuthorizationQueryParametersError", http.StatusBadRequest, "invalid X-Amz-Date")
	}
	expires, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil || expires < 1 || time.Duration(expires)*time.Second > MaxPresignExpiry {
		return p, newErr("AuthorizationQueryParametersError", http.StatusBadRequest, "X-Amz-Expires must be between 1 and 604800 seconds")
	}
	now := v.now()
	if now.Before(date.Add(-MaxSkew)) {
		return p, newErr("AccessDenied", http.StatusForbidden, "Request is not valid yet")
	}
	if now.After(date.Add(time.Duration(expires) * time.Second)) {
		return p, newErr("AccessDenied", http.StatusForbidden, "Request has expired")
	}
	p.date, p.signature, p.presigned = date, q.Get("X-Amz-Signature"), true
	p.signedHeaders = strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
	if e := parseCredential(q.Get("X-Amz-Credential"), date, &p); e != nil {
		return p, e
	}
	p.payloadHash = UnsignedPayload
	if h := q.Get("X-Amz-Content-Sha256"); h != "" {
		p.payloadHash = h
	}
	return p, nil
}

// parseCredential splits "AKID/20130524/us-east-1/s3/aws4_request".
func parseCredential(cred string, date time.Time, p *signingParams) *Error {
	parts := strings.Split(cred, "/")
	if len(parts) != 5 || parts[0] == "" {
		return errMalformed("the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\"")
	}
	if parts[1] != date.UTC().Format("20060102") {
		return errMalformed("the credential date %q does not match the request date", parts[1])
	}
	if parts[3] != service || parts[4] != scopeSuffix || parts[2] == "" {
		return errMalformed("the credential scope must be <date>/<region>/s3/aws4_request")
	}
	p.accessKey = parts[0]
	p.scope = strings.Join(parts[1:], "/")
	if !slices.Contains(p.signedHeaders, "host") {
		return errMalformed("the host header must be signed")
	}
	return nil
}

// canonicalRequest builds the SigV4 canonical request for r.
func canonicalRequest(r *http.Request, p signingParams) string {
	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(uriEncode(r.URL.Path, false))
	b.WriteByte('\n')
	b.WriteString(canonicalQuery(r.URL.RawQuery, p.presigned))
	b.WriteByte('\n')
	for _, h := range p.signedHeaders {
		b.WriteString(h)
		b.WriteByte(':')
		b.WriteString(headerValue(r, h))
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString(strings.Join(p.signedHeaders, ";"))
	b.WriteByte('\n')
	b.WriteString(p.payloadHash)
	return b.String()
}

func headerValue(r *http.Request, name string) string {
	var vals []string
	switch name {
	case "host":
		vals = []string{r.Host}
	case "content-length":
		if v := r.Header.Get("Content-Length"); v != "" {
			vals = []string{v}
		} else if r.ContentLength >= 0 {
			vals = []string{strconv.FormatInt(r.ContentLength, 10)}
		}
	default:
		vals = r.Header.Values(name)
	}
	for i, v := range vals {
		vals[i] = strings.Join(strings.Fields(v), " ") // trim, collapse spaces
	}
	return strings.Join(vals, ",")
}

// canonicalQuery sorts and re-encodes the query string. For presigned
// requests the signature parameter itself is excluded.
func canonicalQuery(raw string, presigned bool) string {
	type kv struct{ k, v string }
	var pairs []kv
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		k, v, _ := strings.Cut(part, "=")
		dk, err1 := url.QueryUnescape(k)
		dv, err2 := url.QueryUnescape(v)
		if err1 != nil || err2 != nil {
			dk, dv = k, v
		}
		if presigned && dk == "X-Amz-Signature" {
			continue
		}
		pairs = append(pairs, kv{uriEncode(dk, true), uriEncode(dv, true)})
	}
	slices.SortFunc(pairs, func(a, b kv) int {
		if c := strings.Compare(a.k, b.k); c != 0 {
			return c
		}
		return strings.Compare(a.v, b.v)
	})
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.k + "=" + p.v
	}
	return strings.Join(out, "&")
}

// uriEncode applies SigV4 URI encoding: unreserved characters are kept,
// '/' is kept unless encodeSlash, everything else becomes %XX (upper case).
func uriEncode(s string, encodeSlash bool) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&15])
		}
	}
	return b.String()
}

func stringToSign(date time.Time, scope, canonical string) []byte {
	sum := sha256.Sum256([]byte(canonical))
	return []byte(algorithm + "\n" + date.UTC().Format(amzDate) + "\n" + scope + "\n" + hex.EncodeToString(sum[:]))
}

// SigningKey derives the SigV4 signing key for a scope
// (yyyymmdd/region/service/aws4_request).
func SigningKey(secret, scope string) []byte {
	parts := strings.Split(scope, "/")
	k := []byte("AWS4" + secret)
	for _, p := range parts {
		k = hmacSHA256(k, []byte(p))
	}
	return k
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// Stream verifies the chained chunk signatures of a signed streaming
// upload. Each chunk's signature covers the previous signature and the
// chunk's SHA-256, so chunks cannot be altered, reordered or dropped.
type Stream struct {
	key   []byte
	scope string
	date  string
	prev  string
}

// VerifyChunk checks the signature of the next chunk (the final, empty
// chunk included) given the SHA-256 of its data.
func (s *Stream) VerifyChunk(signature string, dataSHA256 []byte) error {
	sts := "AWS4-HMAC-SHA256-PAYLOAD\n" + s.date + "\n" + s.scope + "\n" + s.prev + "\n" +
		emptySHA256 + "\n" + hex.EncodeToString(dataSHA256)
	return s.check(signature, sts)
}

// VerifyTrailer checks x-amz-trailer-signature over the trailing headers,
// given as "name:value\n" lines in the order they were sent.
func (s *Stream) VerifyTrailer(signature string, trailers []byte) error {
	sum := sha256.Sum256(trailers)
	sts := "AWS4-HMAC-SHA256-TRAILER\n" + s.date + "\n" + s.scope + "\n" + s.prev + "\n" + hex.EncodeToString(sum[:])
	return s.check(signature, sts)
}

func (s *Stream) check(signature, sts string) error {
	want := hex.EncodeToString(hmacSHA256(s.key, []byte(sts)))
	if subtle.ConstantTimeCompare([]byte(want), []byte(strings.ToLower(signature))) != 1 {
		return newErr("SignatureDoesNotMatch", http.StatusForbidden, "chunk signature does not match")
	}
	s.prev = want
	return nil
}
