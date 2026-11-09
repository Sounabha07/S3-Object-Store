// Command metadata runs one member of the Raft metadata cluster.
//
// M1: serves the health endpoint only. Raft arrives in M2; until then the
// gateway embeds its own BoltDB metadata store.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
)

func main() {
	nodeID := flag.String("id", "meta-1", "raft node id")
	dataDir := flag.String("data", "./data/metadata", "directory for raft log and bolt state")

	httpserv.Main(httpserv.Service{
		Name:        "metadata",
		DefaultAddr: ":7000",
		Setup: func(ctx context.Context, log *slog.Logger) (httpserv.Setup, error) {
			if err := os.MkdirAll(*dataDir, 0o755); err != nil {
				return httpserv.Setup{}, fmt.Errorf("create data dir: %w", err)
			}
			log.Info("starting", "id", *nodeID, "data", *dataDir)
			return httpserv.Setup{}, nil
		},
	})
}
