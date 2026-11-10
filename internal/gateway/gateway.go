// Package gateway assembles the gateway process: metadata backend, data
// plane, S3 API and internal admin endpoints. The command in cmd/gateway
// and the integration tests both build the gateway through it.
package gateway

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/gc"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/raftmeta"
	"github.com/Sounabha07/S3-Object-Store/internal/metrics"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/rebalance"
	"github.com/Sounabha07/S3-Object-Store/internal/repair"
	"github.com/Sounabha07/S3-Object-Store/internal/s3api"
	"github.com/Sounabha07/S3-Object-Store/internal/sigv4"
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
	MinPartSize  int64 // 0: S3's 5 MiB
	// DisableErasure replicates every object 3x (used to compare schemes).
	DisableErasure bool
	Log            *slog.Logger
	// HealthInterval is how often storage nodes are probed (0: 1s).
	HealthInterval time.Duration

	// RepairInterval and GCInterval run background passes; 0 disables
	// them (they can still be triggered through /_admin/).
	RepairInterval time.Duration
	DeepRepair     bool // verify shard contents, not just presence
	GCInterval     time.Duration
	GCGrace        time.Duration // 0: gc.DefaultGrace

	// Credentials maps S3 access key IDs to secrets. When non-empty every
	// S3 request must be SigV4-signed with one of them; when empty the
	// gateway accepts anonymous requests (development only).
	Credentials map[string]string
	// AdminToken authorises /_admin/ requests (Authorization: Bearer
	// <token>). With Credentials set and no token, /_admin/ is disabled.
	AdminToken string
}

// Gateway is an assembled gateway.
type Gateway struct {
	Metrics  *metrics.Gateway
	Meta     metadata.Metadata
	Plane    *dataplane.Plane
	Repairer *repair.Repairer
	GC       *gc.Collector
	Handler  http.Handler // S3 API plus /_admin/
	Health   func(context.Context) error

	cancel  context.CancelFunc
	closers []func() error

	repairMu, gcMu sync.Mutex
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
		p, err := dataplane.NewCluster(dataplane.Config{Nodes: nodes, ChunkSize: cfg.ChunkSize, DisableErasure: cfg.DisableErasure})
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

	g.Metrics = metrics.NewGateway()
	g.Plane.OnReadFailure(func(node placement.NodeID, err error) {
		g.Metrics.ShardReadFailures.WithLabelValues(string(node), metrics.ShardFailureReason(err)).Inc()
	})
	metrics.Gauge(g.Metrics.Registry, "storage_nodes_down", "Storage nodes currently considered down.",
		func() float64 { return float64(len(g.Plane.Health().Down())) })

	var auth *sigv4.Verifier
	if len(cfg.Credentials) > 0 {
		auth = &sigv4.Verifier{Credentials: cfg.Credentials}
	}
	s3 := s3api.New(s3api.Config{Meta: g.Meta, Data: g.Plane, Log: log, MinPartSize: cfg.MinPartSize, Auth: auth, Metrics: g.Metrics})
	admin := adminAuth(g.adminHandler(log), len(cfg.Credentials) > 0, cfg.AdminToken)
	g.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// "_admin" can never be a valid bucket name, so this cannot shadow S3.
		if strings.HasPrefix(r.URL.Path, "/_admin/") {
			admin.ServeHTTP(w, r)
			return
		}
		s3.ServeHTTP(w, r)
	})
	g.Repairer = &repair.Repairer{Meta: g.Meta, Plane: g.Plane, Log: log, Deep: cfg.DeepRepair}
	metrics.Gauge(g.Metrics.Registry, "repair_queue_depth", "Degraded chunks waiting in the current repair pass.",
		func() float64 { return float64(g.Repairer.QueueDepth()) })
	g.GC = &gc.Collector{Meta: g.Meta, Plane: g.Plane, Grace: cfg.GCGrace, Log: log}
	if cfg.RepairInterval > 0 {
		go every(ctx, cfg.RepairInterval, func() {
			if _, err := g.runRepair(ctx); err != nil && ctx.Err() == nil {
				log.Warn("background repair failed", "err", err)
			}
		})
	}
	if cfg.GCInterval > 0 {
		go every(ctx, cfg.GCInterval, func() {
			if _, err := g.runGC(ctx); err != nil && ctx.Err() == nil {
				log.Warn("background gc failed", "err", err)
			}
		})
	}
	return g, nil
}

// every calls fn every interval until ctx is done.
func every(ctx context.Context, interval time.Duration, fn func()) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}

// Passes of the same kind never overlap within one gateway.
func (g *Gateway) runRepair(ctx context.Context) (repair.Stats, error) {
	g.repairMu.Lock()
	defer g.repairMu.Unlock()
	st, err := g.Repairer.Run(ctx)
	g.Metrics.RepairShards.Add(float64(st.ShardsRebuilt))
	g.Metrics.RepairUnrecover.Add(float64(st.Unrecoverable))
	if err == nil {
		g.Metrics.RepairPasses.Inc()
	}
	return st, err
}

func (g *Gateway) runGC(ctx context.Context) (gc.Stats, error) {
	g.gcMu.Lock()
	defer g.gcMu.Unlock()
	st, err := g.GC.Run(ctx)
	g.Metrics.GCDeleted.Add(float64(st.Deleted))
	g.Metrics.GCBytesFreed.Add(float64(st.BytesFreed))
	return st, err
}

// MetricsHandler serves the gateway's Prometheus metrics.
func (g *Gateway) MetricsHandler() http.Handler { return metrics.Handler(g.Metrics.Registry) }

// adminAuth protects the admin API. Without S3 credentials (development
// mode) it is open, like the S3 API. With credentials it needs the admin
// bearer token, and is disabled if no token is configured.
func adminAuth(next http.Handler, authEnabled bool, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authEnabled {
			got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if token == "" || !ok || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
				http.Error(w, "admin API requires a valid bearer token", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ParseCredentials parses "accesskey:secret,accesskey:secret".
func ParseCredentials(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		k, v, ok := strings.Cut(item, ":")
		if !ok || k == "" || v == "" {
			return nil, fmt.Errorf("gateway: bad credential %q, want accesskey:secret", "<redacted>")
		}
		out[k] = v
	}
	return out, nil
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
	mux.HandleFunc("POST /_admin/repair", func(w http.ResponseWriter, r *http.Request) {
		st, err := g.runRepair(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	mux.HandleFunc("POST /_admin/gc", func(w http.ResponseWriter, r *http.Request) {
		st, err := g.runGC(r.Context())
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
