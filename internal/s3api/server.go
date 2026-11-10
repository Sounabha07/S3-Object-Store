// Package s3api implements the S3 REST API (path-style addressing) on top of
// the metadata service and the data plane.
package s3api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
)

// MaxObjectSize is the largest single PutObject S3 accepts (5 GiB).
const MaxObjectSize = 5 << 30

// Config configures a Server.
type Config struct {
	Meta          metadata.Metadata
	Data          *dataplane.Plane
	Log           *slog.Logger
	MaxObjectSize int64 // 0 means MaxObjectSize
}

// Server is an http.Handler serving the S3 API.
type Server struct {
	meta          metadata.Metadata
	data          *dataplane.Plane
	log           *slog.Logger
	maxObjectSize int64
	now           func() time.Time
}

// New returns a Server.
func New(cfg Config) *Server {
	s := &Server{
		meta:          cfg.Meta,
		data:          cfg.Data,
		log:           cfg.Log,
		maxObjectSize: cfg.MaxObjectSize,
		now:           func() time.Time { return time.Now().UTC() },
	}
	if s.maxObjectSize <= 0 {
		s.maxObjectSize = MaxObjectSize
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	return s
}

// unsupportedSubresources are query parameters that select S3 features this
// gateway does not implement. Requests carrying them get NotImplemented
// rather than being silently treated as plain object/bucket operations.
var unsupportedSubresources = []string{
	"accelerate", "acl", "analytics", "attributes", "cors", "delete", "encryption",
	"intelligent-tiering", "inventory", "legal-hold", "lifecycle", "logging", "metrics",
	"notification", "object-lock", "ownershipControls", "partNumber", "policy",
	"policyStatus", "publicAccessBlock", "replication", "requestPayment", "restore",
	"retention", "select", "tagging", "torrent", "uploadId", "uploads", "versioning",
	"versionId", "versions", "website",
}

// request carries per-request state through handlers.
type request struct {
	id     string
	bucket string
	key    string
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	req := &request{id: newRequestID()}
	rec.Header().Set("x-amz-request-id", req.id)
	rec.Header().Set("x-amz-id-2", req.id)
	rec.Header().Set("Server", "s3os")

	s.route(rec, r, req)

	s.log.LogAttrs(r.Context(), slog.LevelInfo, "request",
		slog.String("request_id", req.id),
		slog.String("method", r.Method),
		slog.String("bucket", req.bucket),
		slog.String("key", req.key),
		slog.Int("status", rec.status),
		slog.Int64("bytes_out", rec.bytes),
		slog.Duration("duration", time.Since(start)),
	)
}

func (s *Server) route(w http.ResponseWriter, r *http.Request, req *request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	req.bucket, req.key = bucket, key
	q := r.URL.Query()

	if bucket == "" {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			s.listBuckets(w, r, req)
			return
		}
		s.writeError(w, r, req, ErrMethodNotAllowed)
		return
	}
	for _, sub := range unsupportedSubresources {
		if q.Has(sub) {
			s.writeError(w, r, req, ErrNotImplemented.withMessage("The "+sub+" subresource is not implemented."))
			return
		}
	}

	if key == "" {
		switch r.Method {
		case http.MethodPut:
			s.createBucket(w, r, req)
		case http.MethodDelete:
			s.deleteBucket(w, r, req)
		case http.MethodHead:
			s.headBucket(w, r, req)
		case http.MethodGet:
			switch {
			case q.Has("location"):
				s.getBucketLocation(w, r, req)
			case q.Get("list-type") == "2":
				s.listObjectsV2(w, r, req)
			default:
				s.writeError(w, r, req, ErrNotImplemented.withMessage("ListObjects (v1) is not implemented; use ListObjectsV2."))
			}
		default:
			s.writeError(w, r, req, ErrMethodNotAllowed)
		}
		return
	}

	if apiErr := checkKey(key); apiErr != nil {
		s.writeError(w, r, req, apiErr)
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.putObject(w, r, req)
	case http.MethodGet, http.MethodHead:
		s.getObject(w, r, req)
	case http.MethodDelete:
		s.deleteObject(w, r, req)
	default:
		s.writeError(w, r, req, ErrMethodNotAllowed)
	}
}

// writeError sends an S3 XML error (no body for HEAD).
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, req *request, apiErr *APIError) {
	h := w.Header()
	for _, k := range []string{"Content-Length", "Content-Range", "ETag", "Last-Modified", "Accept-Ranges"} {
		h.Del(k)
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(apiErr.Status)
		return
	}
	h.Set("Content-Type", "application/xml")
	w.WriteHeader(apiErr.Status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(errorResponse{
		Code: apiErr.Code, Message: apiErr.Message, Resource: r.URL.Path, RequestID: req.id,
	})
}

// fail maps err to an S3 error, logging unexpected ones, and writes it.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, req *request, err error) {
	apiErr := toAPIError(err)
	if apiErr.Status >= 500 {
		s.log.Error("request failed", "request_id", req.id, "err", err)
	}
	s.writeError(w, r, req, apiErr)
}

func (s *Server) writeXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	if err := xml.NewEncoder(w).Encode(v); err != nil {
		s.log.Error("encode xml", "err", err)
	}
}

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

func newVersionID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// statusRecorder captures the status code and bytes written for logging.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wroteHeader = true
	n, err := s.ResponseWriter.Write(b)
	s.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
