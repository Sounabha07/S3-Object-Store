// Command metadata runs one member of the Raft metadata cluster.
//
// M0: serves /healthz only. Raft and BoltDB arrive in M2.
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
	nodeID := flag.String("id", "meta-1", "raft node id")
	dataDir := flag.String("data", "./data/metadata", "directory for raft log and bolt state")

	httpserv.Main(httpserv.Service{
		Name:        "metadata",
		DefaultAddr: ":7000",
		Setup: func(ctx context.Context, mux *http.ServeMux, log *slog.Logger) (httpserv.HealthFunc, error) {
			if err := os.MkdirAll(*dataDir, 0o755); err != nil {
				return nil, fmt.Errorf("create data dir: %w", err)
			}
			log.Info("starting", "id", *nodeID, "data", *dataDir)
			return nil, nil
		},
	})
}
