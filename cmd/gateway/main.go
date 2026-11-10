// Command gateway is the S3-compatible HTTP front end.
//
// M1 runs in single-node mode: metadata in a local BoltDB file and shards on
// local disk, both under -data. Later milestones point the gateway at the
// Raft metadata cluster and remote storage nodes instead.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
	"github.com/Sounabha07/S3-Object-Store/internal/s3api"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
)

func main() {
	dataDir := flag.String("data", "./data/gateway", "directory for the single-node metadata DB and shards")
	chunkSize := flag.Int64("chunk-size", dataplane.DefaultChunkSize, "maximum chunk size in bytes")

	httpserv.Main(httpserv.Service{
		Name:        "gateway",
		DefaultAddr: ":9000",
		Setup: func(ctx context.Context, log *slog.Logger) (httpserv.Setup, error) {
			if err := os.MkdirAll(*dataDir, 0o750); err != nil {
				return httpserv.Setup{}, fmt.Errorf("create data dir: %w", err)
			}
			store, err := disk.Open(filepath.Join(*dataDir, "shards"))
			if err != nil {
				return httpserv.Setup{}, err
			}
			meta, err := boltstore.Open(filepath.Join(*dataDir, "meta.db"))
			if err != nil {
				_ = store.Close()
				return httpserv.Setup{}, err
			}
			srv := s3api.New(s3api.Config{
				Meta: meta,
				Data: dataplane.New(store, "local", *chunkSize),
				Log:  log,
			})
			log.Info("starting", "mode", "single-node", "data", *dataDir, "chunk_size", *chunkSize)
			return httpserv.Setup{
				Handler: srv,
				Health:  store.Health,
				Close:   func() error { return errors.Join(meta.Close(), store.Close()) },
			}, nil
		},
	})
}
