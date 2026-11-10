// Package gateway assembles the gateway process: metadata backend, data
// plane, S3 API and internal admin endpoints. The command in cmd/gateway
// and the integration tests both build the gateway through it.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/raftmeta"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/rebalance"
	"github.com/Sounabha07/S3-Object-Store/internal/s3api"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
	"github.com/Sounabha07/S3-Object-Store/internal/storagenode"
)

// Config selects the gateway's backends.
type Config struct {
	// DataDir holds local metadata and/or shards when no cluster is given.
	DataDir string
	// MetaEndpoints are Raft metadata node URLs; empty means a local
	// BoltDB file in DataDir.
	MetaEndpoints []string
	// StorageNodes maps node IDs to storage node URLs; empty means shards on
	// local disk in DataDir (single node, one copy).
	StorageNodes map[placement.NodeID]string
	ChunkSize    int64
	Log          *slog.Logger
	// HealthInterval is how often storage nodes are probed (0: 1s).
	HealthInterval time.Duration
}

// Gateway is an assembled gateway.
type Gateway struct {
	Meta    metadata.Metadata
	Plane   *dataplane.Plane
	Handler http.Handler // S3 API plus /_admin/
	Health  func(context.Context) error

	cancel  context.CancelFunc
	closers []func() error
}

// ParseNodes parses "id=url,id=url".
func ParseNodes(s string) (map[placement.NodeID]string, error) {
	out := map[placement.NodeID]string{}
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		id, url, ok := strings.Cut(item, "=")
		if !ok || id == "" || url == "" {
			return nil, fmt.Errorf("gateway: bad storage node %q, want id=url", item)
		}
		out[placement.NodeID(id)] = strings.TrimRight(url, "/")
	}
	return out, nil
}

// New builds a gateway. Call Close when done.
func New(cfg Config) (*Gateway, error) {
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	g := &Gateway{}
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	fail := func(err error) (*Gateway, error) {
		_ = g.Close()
		return nil, err
	}

	if len(cfg.MetaEndpoints) > 0 {
		g.Meta = raftmeta.NewClient(cfg.MetaEndpoints, nil)
	} else {
		if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
			return fail(err)
		}
		local, err := boltstore.Open(filepath.Join(cfg.DataDir, "meta.db"))
		if err != nil {
			return fail(err)
		}
		g.Meta = local
		g.closers = append(g.closers, local.Close)
	}

	if len(cfg.StorageNodes) > 0 {
		nodes := map[placement.NodeID]storage.Storage{}
		hc := storagenode.DefaultHTTPClient()
		for id, url := range cfg.StorageNodes {
			nodes[id] = storagenode.NewClient(url, hc)
		}
		p, err := dataplane.NewCluster(dataplane.Config{Nodes: nodes, ChunkSize: cfg.ChunkSize})
		if err != nil {
			return fail(err)
		}
		g.Plane = p
		interval := cfg.HealthInterval
		if interval <= 0 {
			interval = time.Second
		}
		go p.Health().Run(ctx, nodes, interval)
		g.Health = func(context.Context) error { return nil } // a gateway is healthy while it can serve; nodes are tracked separately
	} else {
		d, err := disk.Open(filepath.Join(cfg.DataDir, "shards"))
		if err != nil {
			return fail(err)
		}
		g.closers = append(g.closers, d.Close)
		g.Plane = dataplane.New(d, "local", cfg.ChunkSize)
		g.Health = d.Health
	}

	s3 := s3api.New(s3api.Config{Meta: g.Meta, Data: g.Plane, Log: log})
	admin := g.adminHandler(log)
	g.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// "_admin" can never be a valid bucket name, so this cannot shadow S3.
		if strings.HasPrefix(r.URL.Path, "/_admin/") {
			admin.ServeHTTP(w, r)
			return
		}
		s3.ServeHTTP(w, r)
	})
	return g, nil
}

// Close releases local resources and stops background work.
func (g *Gateway) Close() error {
	if g.cancel != nil {
		g.cancel()
	}
	var errs []error
	for _, c := range g.closers {
		errs = append(errs, c())
	}
	return errors.Join(errs...)
}

// adminHandler serves internal operations. It is meant for operators on the
// cluster network; M6 adds authentication.
func (g *Gateway) adminHandler(log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /_admin/rebalance", func(w http.ResponseWriter, r *http.Request) {
		st, err := (&rebalance.Rebalancer{Meta: g.Meta, Plane: g.Plane, Log: log}).Run(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	mux.HandleFunc("GET /_admin/nodes", func(w http.ResponseWriter, r *http.Request) {
		type nodeStatus struct {
			ID   placement.NodeID `json:"id"`
			Down bool             `json:"down"`
		}
		var out []nodeStatus
		for _, id := range g.Plane.Placement().Nodes() {
			out = append(out, nodeStatus{ID: id, Down: g.Plane.Health().IsDown(id)})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	return mux
}
