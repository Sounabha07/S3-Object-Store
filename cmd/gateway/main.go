// Command gateway is the S3-compatible HTTP front end.
//
// With -meta it is a stateless client of the Raft metadata cluster; with
// -storage it stores every chunk as 3 replicas across the storage nodes.
// Without them it runs single-node, keeping metadata and shards in -data.
package main

import (
	"context"
	"flag"
	"log/slog"
	"strings"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/gateway"
	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
)

func main() {
	dataDir := flag.String("data", "./data/gateway", "directory for local metadata/shards when no cluster is configured")
	chunkSize := flag.Int64("chunk-size", dataplane.DefaultChunkSize, "maximum chunk size in bytes")
	metaEndpoints := flag.String("meta", "", "comma-separated metadata node URLs (empty: local metadata)")
	storageNodes := flag.String("storage", "", "storage nodes as id=url,... (empty: local disk)")
	repairInterval := flag.Duration("repair-interval", 10*time.Minute, "background repair pass interval (0 disables)")
	deepRepair := flag.Bool("deep-repair", false, "repair verifies every shard's contents, not just presence")
	gcInterval := flag.Duration("gc-interval", time.Hour, "background garbage collection interval (0 disables)")
	gcGrace := flag.Duration("gc-grace", time.Hour, "minimum age of an unreferenced shard before GC deletes it")

	httpserv.Main(httpserv.Service{
		Name:        "gateway",
		DefaultAddr: ":9000",
		Setup: func(ctx context.Context, log *slog.Logger) (httpserv.Setup, error) {
			nodes, err := gateway.ParseNodes(*storageNodes)
			if err != nil {
				return httpserv.Setup{}, err
			}
			var meta []string
			if *metaEndpoints != "" {
				meta = strings.Split(*metaEndpoints, ",")
			}
			g, err := gateway.New(gateway.Config{
				DataDir: *dataDir, MetaEndpoints: meta, StorageNodes: nodes, ChunkSize: *chunkSize, Log: log,
				RepairInterval: *repairInterval, DeepRepair: *deepRepair, GCInterval: *gcInterval, GCGrace: *gcGrace,
			})
			if err != nil {
				return httpserv.Setup{}, err
			}
			log.Info("starting", "raft_metadata", len(meta) > 0, "storage_nodes", len(nodes), "replicas", g.Plane.Replicas())
			return httpserv.Setup{Handler: g.Handler, Health: g.Health, Close: g.Close}, nil
		},
	})
}
