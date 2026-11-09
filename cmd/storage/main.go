// Command storage runs a storage node holding immutable, content-addressed shards.
//
// M0: serves /healthz only. The shard API arrives in M3.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
)

func main() {
	nodeID := flag.String("id", "storage-1", "storage node id")
	dataDir := flag.String("data", "./data/storage", "directory for shard files")

	httpserv.Main(httpserv.Service{
		Name:        "storage",
		DefaultAddr: ":8000",
		Setup: func(ctx context.Context, mux *http.ServeMux, log *slog.Logger) (httpserv.HealthFunc, error) {
			if err := os.MkdirAll(*dataDir, 0o755); err != nil {
				return nil, fmt.Errorf("create data dir: %w", err)
			}
			log.Info("starting", "id", *nodeID, "data", *dataDir)
			return nil, nil
		},
	})
}
