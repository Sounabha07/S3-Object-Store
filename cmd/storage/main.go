// Command storage runs a storage node holding immutable, content-addressed shards.
//
// M1: serves the health endpoint backed by the local disk store. The
// internal shard API arrives in M3.
package main

import (
	"context"
	"flag"
	"log/slog"

	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
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
			log.Info("starting", "id", *nodeID, "data", *dataDir)
			return httpserv.Setup{Health: d.Health, Close: d.Close}, nil
		},
	})
}
