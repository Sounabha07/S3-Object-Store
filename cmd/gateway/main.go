// Command gateway is the S3-compatible HTTP front end.
//
// Metadata: with -meta set, the gateway is a stateless client of the Raft
// metadata cluster; without it, it keeps metadata in a local BoltDB file
// (single-node mode). Shards are still stored on the gateway's local disk
// until distributed storage nodes arrive in M3.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/raftmeta"
	"github.com/Sounabha07/S3-Object-Store/internal/s3api"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
)

func main() {
	dataDir := flag.String("data", "./data/gateway", "directory for shards (and the metadata DB in single-node mode)")
	chunkSize := flag.Int64("chunk-size", dataplane.DefaultChunkSize, "maximum chunk size in bytes")
	metaEndpoints := flag.String("meta", "", "comma-separated metadata node URLs (empty: single-node local metadata)")

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
			closers := []func() error{store.Close}

			var meta metadata.Metadata
			mode := "single-node"
			if *metaEndpoints != "" {
				meta = raftmeta.NewClient(strings.Split(*metaEndpoints, ","), nil)
				mode = "raft-metadata"
			} else {
				local, err := boltstore.Open(filepath.Join(*dataDir, "meta.db"))
				if err != nil {
					_ = store.Close()
					return httpserv.Setup{}, err
				}
				meta = local
				closers = append(closers, local.Close)
			}

			srv := s3api.New(s3api.Config{
				Meta: meta,
				Data: dataplane.New(store, "local", *chunkSize),
				Log:  log,
			})
			log.Info("starting", "metadata", mode, "data", *dataDir, "chunk_size", *chunkSize)
			return httpserv.Setup{
				Handler: srv,
				Health:  store.Health,
				Close: func() error {
					var errs []error
					for _, c := range closers {
						errs = append(errs, c())
					}
					return errors.Join(errs...)
				},
			}, nil
		},
	})
}
