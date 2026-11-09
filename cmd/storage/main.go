// Command storage runs a storage node holding immutable, content-addressed
// shards, served over the internal HTTP API in internal/storagenode.
package main

import (
	"context"
	"flag"
	"log/slog"

	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
	"github.com/Sounabha07/S3-Object-Store/internal/storagenode"
)

func main() {
	nodeID := flag.String("id", "storage-1", "storage node id")
	dataDir := flag.String("data", "./data/storage", "directory for shard files")

	httpserv.Main(httpserv.Service{
		Name:        "storage",
		DefaultAddr: ":8000",
		Setup: func(ctx context.Context, log *slog.Logger) (httpserv.Setup, error) {
			d, err := disk.Open(*dataDir)
			if err != nil {
				return httpserv.Setup{}, err
			}
			log = log.With("node", *nodeID)
			log.Info("starting", "data", *dataDir)
			return httpserv.Setup{Handler: storagenode.Handler(d, log), Health: d.Health, Close: d.Close}, nil
		},
	})
}
