package raftmeta

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
)

// maxCommandBytes bounds one proposed command. An ObjectMeta for a 5 GiB
// object has about 1,300 chunks, far below this.
const maxCommandBytes = 32 << 20

// Handler returns the node's HTTP API:
//
//	POST /v1/apply                      body: boltstore.Command
//	GET  /v1/buckets
//	GET  /v1/bucket?name=
//	GET  /v1/object?bucket=&key=
//	GET  /v1/objects?bucket=&prefix=&delimiter=&max-keys=&start-after=&token=
//	GET  /v1/chunks                     newline-delimited JSON stream
//	GET  /v1/status
//
// Only the leader serves /v1/apply and the reads; followers answer 421 with
// the leader's address so clients can redirect.
func Handler(n *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/apply", func(w http.ResponseWriter, r *http.Request) {
		var cmd boltstore.Command
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCommandBytes))
		if err := dec.Decode(&cmd); err != nil {
			writeError(w, errors.Join(metadata.ErrInvalidArgument, err))
			return
		}
		if err := n.Apply(r.Context(), cmd); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, struct{}{})
	})
	mux.HandleFunc("GET /v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		res, err := n.ListBuckets(r.Context())
		respond(w, res, err)
	})
	mux.HandleFunc("GET /v1/bucket", func(w http.ResponseWriter, r *http.Request) {
		res, err := n.HeadBucket(r.Context(), r.URL.Query().Get("name"))
		respond(w, res, err)
	})
	mux.HandleFunc("GET /v1/object", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		res, err := n.GetObject(r.Context(), q.Get("bucket"), q.Get("key"))
		respond(w, res, err)
	})
	mux.HandleFunc("GET /v1/objects", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		maxKeys, _ := strconv.Atoi(q.Get("max-keys"))
		res, err := n.ListObjects(r.Context(), q.Get("bucket"), metadata.ListOptions{
			Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"), MaxKeys: maxKeys,
			StartAfter: q.Get("start-after"), ContinuationToken: q.Get("token"),
		})
		respond(w, res, err)
	})
	mux.HandleFunc("GET /v1/chunks", func(w http.ResponseWriter, r *http.Request) { serveChunks(n, w, r, false) })
	mux.HandleFunc("GET /v1/partchunks", func(w http.ResponseWriter, r *http.Request) { serveChunks(n, w, r, true) })
	mux.HandleFunc("GET /v1/upload", func(w http.ResponseWriter, r *http.Request) {
		res, err := n.GetUpload(r.Context(), r.URL.Query().Get("id"))
		respond(w, res, err)
	})
	mux.HandleFunc("GET /v1/parts", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		maxParts, _ := strconv.Atoi(q.Get("max"))
		marker, _ := strconv.Atoi(q.Get("marker"))
		res, err := n.ListParts(r.Context(), q.Get("upload"), metadata.ListPartsOptions{MaxParts: maxParts, PartNumberMarker: marker})
		respond(w, res, err)
	})
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, n.Status()) })
	return mux
}

// chunkRecord is one line of the /v1/chunks stream. The stream ends with a
// record whose Done is true, or whose Error is set; a stream without either
// was truncated.
type chunkRecord struct {
	Bucket     string          `json:"bucket,omitempty"`
	Key        string          `json:"key,omitempty"`
	VersionID  string          `json:"version,omitempty"`
	ChunkIndex int             `json:"index,omitempty"`
	UploadID   string          `json:"upload,omitempty"`
	PartNumber int             `json:"part,omitempty"`
	Chunk      *metadata.Chunk `json:"chunk,omitempty"`
	Done       bool            `json:"done,omitempty"`
	Error      *wireError      `json:"error,omitempty"`
}

// serveChunks streams object chunks, or in-progress part chunks if parts.
func serveChunks(n *Node, w http.ResponseWriter, r *http.Request, parts bool) {
	if err := n.linearize(r.Context()); err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	var err error
	if parts {
		err = n.store.WalkPartChunks(r.Context(), func(uploadID string, part int, c metadata.Chunk) error {
			return enc.Encode(chunkRecord{UploadID: uploadID, PartNumber: part, Chunk: &c})
		})
	} else {
		err = n.store.WalkChunks(r.Context(), func(bucket, key, version string, idx int, c metadata.Chunk) error {
			return enc.Encode(chunkRecord{Bucket: bucket, Key: key, VersionID: version, ChunkIndex: idx, Chunk: &c})
		})
	}
	if err != nil {
		_ = enc.Encode(chunkRecord{Error: &wireError{Code: errorCode(err), Message: err.Error()}})
		return
	}
	_ = enc.Encode(chunkRecord{Done: true})
}

func respond(w http.ResponseWriter, v any, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, v)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	body := wireError{Code: errorCode(err), Message: err.Error()}
	status := http.StatusInternalServerError
	var nl *notLeaderError
	switch {
	case errors.As(err, &nl):
		body.Leader = nl.leader
		status = http.StatusMisdirectedRequest
	case errors.Is(err, errOutcomeUnknown):
		body.Code = codeOutcomeUnknown
		status = http.StatusServiceUnavailable
	case errors.Is(err, metadata.ErrUnavailable):
		status = http.StatusServiceUnavailable
	case errors.Is(err, metadata.ErrInvalidArgument), errors.Is(err, metadata.ErrInvalidPart),
		errors.Is(err, metadata.ErrInvalidPartOrder), errors.Is(err, metadata.ErrEntityTooSmall):
		status = http.StatusBadRequest
	case errors.Is(err, metadata.ErrNoSuchBucket), errors.Is(err, metadata.ErrNoSuchKey), errors.Is(err, metadata.ErrNoSuchUpload):
		status = http.StatusNotFound
	case errors.Is(err, metadata.ErrBucketAlreadyExists), errors.Is(err, metadata.ErrBucketNotEmpty):
		status = http.StatusConflict
	case errors.Is(err, metadata.ErrNotImplemented):
		status = http.StatusNotImplemented
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
