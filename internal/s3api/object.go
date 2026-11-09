package s3api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
)

// maxUserMetadata is S3's limit on the total size of x-amz-meta-* headers.
const maxUserMetadata = 2 << 10

// storedHeaders are request headers S3 persists and returns on GET/HEAD.
var storedHeaders = []string{"Cache-Control", "Content-Disposition", "Content-Encoding", "Content-Language", "Expires"}

// responseOverrides are GET query parameters that override response headers.
var responseOverrides = map[string]string{
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
	"response-content-encoding":    "Content-Encoding",
	"response-content-language":    "Content-Language",
	"response-content-type":        "Content-Type",
	"response-expires":             "Expires",
}

// body is a decoded request body plus how to read trailers after EOF.
type body struct {
	r        *errTrackingReader
	length   int64
	trailers func() map[string]string
}

// requestBody unwraps aws-chunked encoding and determines the payload length.
func requestBody(r *http.Request) (body, *APIError) {
	streaming := strings.HasPrefix(r.Header.Get("x-amz-content-sha256"), "STREAMING-") ||
		hasToken(r.Header.Values("Content-Encoding"), "aws-chunked")
	if streaming {
		n, err := strconv.ParseInt(r.Header.Get("x-amz-decoded-content-length"), 10, 64)
		if err != nil || n < 0 {
			return body{}, ErrMissingContentLength
		}
		cr := newChunkedReader(r.Body)
		return body{r: &errTrackingReader{r: cr}, length: n, trailers: cr.Trailers}, nil
	}
	if r.ContentLength < 0 {
		return body{}, ErrMissingContentLength
	}
	return body{r: &errTrackingReader{r: r.Body}, length: r.ContentLength, trailers: func() map[string]string { return nil }}, nil
}

func hasToken(values []string, token string) bool {
	for _, v := range values {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// errTrackingReader remembers the first non-EOF error from the client body,
// so a broken upload is reported as IncompleteBody rather than InternalError.
type errTrackingReader struct {
	r   io.Reader
	err error
}

func (e *errTrackingReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && e.err == nil {
		e.err = err
	}
	return n, err
}

// integrity collects every digest the client asked us to verify.
type integrity struct {
	md5       []byte           // from Content-MD5
	sha256Hex string           // from x-amz-content-sha256, when it is a real digest
	sha256    hash.Hash        // running hash for sha256Hex
	checksums []*checksumCheck // x-amz-checksum-* headers and trailers
}

func parseIntegrity(r *http.Request) (*integrity, *APIError) {
	in := &integrity{}
	if v := r.Header.Get("Content-MD5"); v != "" {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil || len(b) != 16 {
			return nil, ErrInvalidDigest
		}
		in.md5 = b
	}
	if v := r.Header.Get("x-amz-content-sha256"); len(v) == 64 {
		if _, err := hex.DecodeString(v); err == nil {
			in.sha256Hex = strings.ToLower(v)
			in.sha256 = sha256.New()
		}
	}
	for name, vals := range r.Header {
		if c := newChecksumCheck(name); c != nil && len(vals) > 0 {
			c.want = vals[0]
			in.checksums = append(in.checksums, c)
		}
	}
	for _, name := range strings.Split(r.Header.Get("x-amz-trailer"), ",") {
		if c := newChecksumCheck(name); c != nil {
			c.inTrailer = true
			in.checksums = append(in.checksums, c)
		}
	}
	return in, nil
}

func (in *integrity) writers() []io.Writer {
	var ws []io.Writer
	if in.sha256 != nil {
		ws = append(ws, in.sha256)
	}
	for _, c := range in.checksums {
		ws = append(ws, c.h)
	}
	return ws
}

// verify checks every digest after the body has been fully read.
func (in *integrity) verify(md5sum []byte, trailers map[string]string) *APIError {
	if in.md5 != nil && string(in.md5) != string(md5sum) {
		return ErrBadDigest
	}
	if in.sha256 != nil && hex.EncodeToString(in.sha256.Sum(nil)) != in.sha256Hex {
		return ErrSHA256Mismatch
	}
	for _, c := range in.checksums {
		want := c.want
		if c.inTrailer {
			want = trailers[c.header]
			if want == "" {
				return ErrInvalidRequest.withMessage("Missing trailing checksum " + c.header)
			}
		}
		if got := c.sum(); got != want {
			return ErrBadDigest.withMessage(fmt.Sprintf("Value for %s header is invalid.", c.header))
		}
	}
	return nil
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, req *request) {
	if r.Header.Get("x-amz-copy-source") != "" {
		s.writeError(w, r, req, ErrNotImplemented.withMessage("CopyObject is not implemented."))
		return
	}
	userMeta, apiErr := parseUserMeta(r.Header)
	if apiErr != nil {
		s.writeError(w, r, req, apiErr)
		return
	}
	// Fail fast on a missing bucket before accepting any data.
	if _, err := s.meta.HeadBucket(r.Context(), req.bucket); err != nil {
		s.fail(w, r, req, err)
		return
	}
	versionID := newVersionID()
	res, in, ok := s.ingest(w, r, req, dataplane.ObjectKey(req.bucket, req.key, versionID))
	if !ok {
		return
	}

	obj := metadata.ObjectMeta{
		Bucket:       req.bucket,
		Key:          req.key,
		VersionID:    versionID,
		Size:         res.Size,
		ETag:         `"` + hex.EncodeToString(res.MD5[:]) + `"`,
		ContentType:  r.Header.Get("Content-Type"),
		UserMeta:     userMeta,
		Headers:      parseStoredHeaders(r.Header),
		LastModified: s.now(),
		Chunks:       res.Chunks,
	}
	if obj.ContentType == "" {
		obj.ContentType = "binary/octet-stream"
	}
	if err := s.meta.PutObject(r.Context(), obj); err != nil {
		s.fail(w, r, req, err)
		return
	}
	w.Header().Set("ETag", obj.ETag)
	for _, c := range in.checksums {
		w.Header().Set(c.header, c.sum())
	}
	w.WriteHeader(http.StatusOK)
}

// ingest reads a request body (plain or aws-chunked), stores it through
// the data plane under placementKey and verifies every digest the client
// supplied. On failure it has already written the error response.
func (s *Server) ingest(w http.ResponseWriter, r *http.Request, req *request, placementKey string) (dataplane.WriteResult, *integrity, bool) {
	b, apiErr := requestBody(r)
	if apiErr != nil {
		s.writeError(w, r, req, apiErr)
		return dataplane.WriteResult{}, nil, false
	}
	if b.length > s.maxObjectSize {
		s.writeError(w, r, req, ErrEntityTooLarge)
		return dataplane.WriteResult{}, nil, false
	}
	in, apiErr := parseIntegrity(r)
	if apiErr != nil {
		s.writeError(w, r, req, apiErr)
		return dataplane.WriteResult{}, nil, false
	}
	var src io.Reader = b.r
	if ws := in.writers(); len(ws) > 0 {
		src = io.TeeReader(b.r, io.MultiWriter(ws...))
	}
	res, err := s.data.Write(r.Context(), src, b.length, placementKey)
	if err != nil {
		if b.r.err != nil {
			s.log.Info("upload body error", "request_id", req.id, "err", b.r.err)
			s.writeError(w, r, req, ErrIncompleteBody)
			return res, nil, false
		}
		s.fail(w, r, req, err)
		return res, nil, false
	}
	if res.Size != b.length {
		s.writeError(w, r, req, ErrIncompleteBody)
		return res, nil, false
	}
	if apiErr := in.verify(res.MD5[:], b.trailers()); apiErr != nil {
		s.writeError(w, r, req, apiErr)
		return res, nil, false
	}
	return res, in, true
}

func parseUserMeta(h http.Header) (map[string]string, *APIError) {
	var meta map[string]string
	total := 0
	for name, vals := range h {
		k, ok := strings.CutPrefix(strings.ToLower(name), "x-amz-meta-")
		if !ok {
			continue
		}
		v := strings.Join(vals, ",")
		total += len(k) + len(v)
		if meta == nil {
			meta = map[string]string{}
		}
		meta[k] = v
	}
	if total > maxUserMetadata {
		return nil, ErrMetadataTooLarge
	}
	return meta, nil
}

func parseStoredHeaders(h http.Header) map[string]string {
	var out map[string]string
	for _, name := range storedHeaders {
		v := h.Get(name)
		if name == "Content-Encoding" {
			v = stripToken(v, "aws-chunked")
		}
		if v == "" {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[name] = v
	}
	return out
}

func stripToken(v, token string) string {
	var keep []string
	for _, t := range strings.Split(v, ",") {
		if t = strings.TrimSpace(t); t != "" && !strings.EqualFold(t, token) {
			keep = append(keep, t)
		}
	}
	return strings.Join(keep, ",")
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, req *request) {
	obj, err := s.meta.GetObject(r.Context(), req.bucket, req.key)
	if err != nil {
		s.fail(w, r, req, err)
		return
	}

	h := w.Header()
	h.Set("ETag", obj.ETag)
	h.Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")

	switch evaluatePreconditions(r, obj) {
	case http.StatusPreconditionFailed:
		s.writeError(w, r, req, ErrPreconditionFailed)
		return
	case http.StatusNotModified:
		w.WriteHeader(http.StatusNotModified)
		return
	}

	rng, apiErr := parseRange(r.Header.Get("Range"), obj.Size)
	if apiErr != nil {
		s.writeError(w, r, req, apiErr)
		return
	}

	h.Set("Content-Type", obj.ContentType)
	for k, v := range obj.Headers {
		h.Set(k, v)
	}
	for k, v := range obj.UserMeta {
		h["X-Amz-Meta-"+k] = []string{v}
	}
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		for param, header := range responseOverrides {
			if q.Has(param) {
				h.Set(header, q.Get(param))
			}
		}
	}
	status := http.StatusOK
	if rng.partial {
		status = http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.start+rng.length-1, obj.Size))
	}
	h.Set("Content-Length", strconv.FormatInt(rng.length, 10))

	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	lw := &lazyWriter{w: w, status: status}
	if err := s.data.Read(r.Context(), lw, obj, rng.start, rng.length); err != nil {
		if !lw.started {
			s.fail(w, r, req, err)
			return
		}
		// Headers and part of the body are already sent; the only honest
		// signal left is to abort the connection so the client sees a
		// truncated response instead of silently wrong data.
		s.log.Error("read failed mid-response", "request_id", req.id, "err", err)
		panic(http.ErrAbortHandler)
	}
	if !lw.started {
		w.WriteHeader(status)
	}
}

// lazyWriter defers WriteHeader until the first body byte, so that an error
// before any data is produced can still be reported as a proper S3 error.
type lazyWriter struct {
	w       http.ResponseWriter
	status  int
	started bool
}

func (l *lazyWriter) Write(p []byte) (int, error) {
	if !l.started {
		l.started = true
		l.w.WriteHeader(l.status)
	}
	return l.w.Write(p)
}

// evaluatePreconditions applies RFC 9110 conditional request rules and
// returns 0 (proceed), 304 or 412.
func evaluatePreconditions(r *http.Request, obj metadata.ObjectMeta) int {
	lastMod := obj.LastModified.Truncate(time.Second)
	if v := r.Header.Get("If-Match"); v != "" {
		if !etagMatches(v, obj.ETag) {
			return http.StatusPreconditionFailed
		}
	} else if t, err := http.ParseTime(r.Header.Get("If-Unmodified-Since")); err == nil && lastMod.After(t) {
		return http.StatusPreconditionFailed
	}
	if v := r.Header.Get("If-None-Match"); v != "" {
		if etagMatches(v, obj.ETag) {
			return http.StatusNotModified
		}
	} else if t, err := http.ParseTime(r.Header.Get("If-Modified-Since")); err == nil && !lastMod.After(t) {
		return http.StatusNotModified
	}
	return 0
}

func etagMatches(header, etag string) bool {
	etag = strings.Trim(etag, `"`)
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" {
			return true
		}
		candidate = strings.Trim(strings.TrimPrefix(candidate, "W/"), `"`)
		if candidate == etag {
			return true
		}
	}
	return false
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request, req *request) {
	if err := s.meta.DeleteObject(r.Context(), req.bucket, req.key); err != nil {
		s.fail(w, r, req, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
