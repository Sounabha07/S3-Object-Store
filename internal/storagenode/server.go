// Package storagenode exposes a storage.Storage over HTTP, and provides a
// client that implements storage.Storage against such a node.
//
// API (all shard IDs are 64 lowercase hex characters):
//
//	POST   /v1/shards          body: shard bytes; optional X-Expected-Sha256
//	GET    /v1/shards/{id}     shard bytes
//	HEAD   /v1/shards/{id}     size in Content-Length, mod time in X-Mod-Time
//	DELETE /v1/shards/{id}
//	GET    /v1/shards          newline-delimited JSON of every shard, ending
//	                           with {"done":true}
package storagenode

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// MaxShardSize bounds one uploaded shard. Chunks are at most 4 MiB, so no
// legitimate shard is larger.
const MaxShardSize = 16 << 20

const (
	headerExpected = "X-Expected-Sha256"
	headerModTime  = "X-Mod-Time" // Unix nanoseconds
	headerCode     = "X-Error-Code"
)

// Error codes carried in X-Error-Code.
const (
	codeNotFound = "NotFound"
	codeCorrupt  = "Corrupt"
	codeInvalid  = "Invalid"
	codeInternal = "Internal"
)

// shardInfoJSON is the wire form of storage.ShardInfo.
type shardInfoJSON struct {
	ID      storage.ShardID `json:"id"`
	Size    int64           `json:"size"`
	ModTime time.Time       `json:"mod_time"`
}

type walkRecord struct {
	Shard *shardInfoJSON `json:"shard,omitempty"`
	Done  bool           `json:"done,omitempty"`
	Error string         `json:"error,omitempty"`
}

// Handler serves store over HTTP.
func Handler(store storage.Storage, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/shards", func(w http.ResponseWriter, r *http.Request) {
		var expected storage.ShardID
		if v := r.Header.Get(headerExpected); v != "" {
			id, err := storage.ParseShardID(v)
			if err != nil {
				writeErr(w, codeInvalid, http.StatusBadRequest, err)
				return
			}
			expected = id
		}
		info, err := store.Put(r.Context(), http.MaxBytesReader(w, r.Body, MaxShardSize), expected)
		if err != nil {
			var tooBig *http.MaxBytesError
			switch {
			case errors.Is(err, storage.ErrCorrupt):
				writeErr(w, codeCorrupt, http.StatusUnprocessableEntity, err)
			case errors.As(err, &tooBig):
				writeErr(w, codeInvalid, http.StatusRequestEntityTooLarge, err)
			default:
				log.Error("put shard", "err", err)
				writeErr(w, codeInternal, http.StatusInternalServerError, err)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(shardInfoJSON{ID: info.ID, Size: info.Size, ModTime: info.ModTime})
	})
	mux.HandleFunc("GET /v1/shards/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := shardID(w, r)
		if !ok {
			return
		}
		info, err := store.Stat(r.Context(), id)
		if err != nil {
			storeErr(w, err, log)
			return
		}
		rc, err := store.Get(r.Context(), id)
		if err != nil {
			storeErr(w, err, log)
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
		if _, err := io.Copy(w, rc); err != nil {
			// The status line is already sent. Abort so the client sees a
			// truncated body; it verifies the hash itself as well.
			log.Warn("get shard failed mid-stream", "id", id, "err", err)
			panic(http.ErrAbortHandler)
		}
	})
	mux.HandleFunc("HEAD /v1/shards/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := shardID(w, r)
		if !ok {
			return
		}
		info, err := store.Stat(r.Context(), id)
		if err != nil {
			storeErr(w, err, log)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
		w.Header().Set(headerModTime, strconv.FormatInt(info.ModTime.UnixNano(), 10))
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("DELETE /v1/shards/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := shardID(w, r)
		if !ok {
			return
		}
		if err := store.Delete(r.Context(), id); err != nil {
			storeErr(w, err, log)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/shards", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		enc := json.NewEncoder(w)
		err := store.Walk(r.Context(), func(info storage.ShardInfo) error {
			return enc.Encode(walkRecord{Shard: &shardInfoJSON{ID: info.ID, Size: info.Size, ModTime: info.ModTime}})
		})
		if err != nil {
			_ = enc.Encode(walkRecord{Error: err.Error()})
			return
		}
		_ = enc.Encode(walkRecord{Done: true})
	})
	return mux
}

func shardID(w http.ResponseWriter, r *http.Request) (storage.ShardID, bool) {
	id, err := storage.ParseShardID(r.PathValue("id"))
	if err != nil {
		writeErr(w, codeInvalid, http.StatusBadRequest, err)
		return id, false
	}
	return id, true
}

func storeErr(w http.ResponseWriter, err error, log *slog.Logger) {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		writeErr(w, codeNotFound, http.StatusNotFound, err)
	case errors.Is(err, storage.ErrCorrupt):
		writeErr(w, codeCorrupt, http.StatusUnprocessableEntity, err)
	default:
		log.Error("shard operation", "err", err)
		writeErr(w, codeInternal, http.StatusInternalServerError, err)
	}
}

func writeErr(w http.ResponseWriter, code string, status int, err error) {
	w.Header().Set(headerCode, code)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, err.Error()) // #nosec G705 -- text/plain with nosniff on an internal API; never rendered as HTML
}
