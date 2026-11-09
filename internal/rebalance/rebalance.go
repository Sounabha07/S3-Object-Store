// Package rebalance moves shards so their locations match the placement
// ring, for example after a storage node is added.
//
// For every chunk it compares the nodes recorded in metadata with the nodes
// the ring now chooses. Each shard on a node the ring no longer chooses is
// copied to a chosen node that holds none of the chunk's shards, then its
// location is updated in metadata with a conditional write. The old copy is
// left in place: content-addressed shards may be shared, so removing
// unreferenced shards is garbage collection's job (M5).
//
// Correctness does not depend on rebalancing finishing: until a shard's
// metadata is updated, reads use the old location, which still holds it.
package rebalance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// Stats summarises one pass.
type Stats struct {
	ChunksScanned int
	ShardsMoved   int
	BytesCopied   int64
	Conflicts     int // metadata changed underneath us; skipped safely
	Skipped       int // target down or no source readable; retried next pass
	Errors        int
}

// Rebalancer runs rebalancing passes.
type Rebalancer struct {
	Meta  metadata.Metadata
	Plane *dataplane.Plane
	Log   *slog.Logger
}

type item struct {
	bucket, key, version string
	index                int
	chunk                metadata.Chunk
}

// Run performs one full pass over all chunks.
func (r *Rebalancer) Run(ctx context.Context) (Stats, error) {
	log := r.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	var items []item
	err := r.Meta.WalkChunks(ctx, func(bucket, key, version string, idx int, c metadata.Chunk) error {
		items = append(items, item{bucket, key, version, idx, c})
		return nil
	})
	if err != nil {
		return Stats{}, fmt.Errorf("rebalance: walk metadata: %w", err)
	}
	var st Stats
	for _, it := range items {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		st.ChunksScanned++
		r.chunk(ctx, it, &st, log)
	}
	log.Info("rebalance pass done", "chunks", st.ChunksScanned, "moved", st.ShardsMoved,
		"bytes", st.BytesCopied, "conflicts", st.Conflicts, "skipped", st.Skipped, "errors", st.Errors)
	return st, nil
}

func (r *Rebalancer) chunk(ctx context.Context, it item, st *Stats, log *slog.Logger) {
	key := dataplane.PlacementKey(dataplane.ObjectKey(it.bucket, it.key, it.version), it.index)
	desired, err := r.Plane.Placement().Pick(key, len(it.chunk.Shards))
	if err != nil {
		st.Errors++
		return
	}
	current := make([]placement.NodeID, 0, len(it.chunk.Shards))
	for _, s := range it.chunk.Shards {
		current = append(current, s.Node)
	}
	var targets []placement.NodeID
	for _, n := range desired {
		if !slices.Contains(current, n) {
			targets = append(targets, n)
		}
	}
	health := r.Plane.Health()
	for _, sh := range it.chunk.Shards {
		if slices.Contains(desired, sh.Node) || len(targets) == 0 {
			continue
		}
		target := targets[0]
		if health.IsDown(target) {
			st.Skipped++
			continue
		}
		if err := r.copyShard(ctx, it.chunk, sh, target); err != nil {
			log.Warn("rebalance copy failed", "bucket", it.bucket, "key", it.key, "chunk", it.index, "to", target, "err", err)
			st.Skipped++
			continue
		}
		err := r.Meta.UpdateShardLocation(ctx, metadata.ShardLocationUpdate{
			Bucket: it.bucket, Key: it.key, VersionID: it.version,
			ChunkIndex: it.index, ShardIndex: sh.Index, From: sh.Node, To: target,
		})
		switch {
		case err == nil:
			st.ShardsMoved++
			st.BytesCopied += sh.Size
			targets = targets[1:]
		case errors.Is(err, metadata.ErrShardLocationConflict), errors.Is(err, metadata.ErrNoSuchKey), errors.Is(err, metadata.ErrNoSuchBucket):
			st.Conflicts++ // overwritten, deleted or moved meanwhile: the copy is an orphan for GC
		default:
			log.Warn("rebalance metadata update failed", "err", err)
			st.Errors++
		}
	}
}

// copyShard reads sh (from its node, or from any other shard of the chunk
// with the same content) and stores it on target, verified end to end.
func (r *Rebalancer) copyShard(ctx context.Context, c metadata.Chunk, sh metadata.ShardRef, target placement.NodeID) error {
	dst, ok := r.Plane.Node(target)
	if !ok {
		return fmt.Errorf("unknown node %s", target)
	}
	buf := make([]byte, sh.Size)
	sources := []metadata.ShardRef{sh}
	for _, other := range c.Shards {
		if other.ID == sh.ID && other.Node != sh.Node {
			sources = append(sources, other)
		}
	}
	var errs []error
	for _, src := range sources {
		if err := r.Plane.ReadShard(ctx, src, buf); err != nil {
			errs = append(errs, err)
			continue
		}
		_, err := dst.Put(ctx, &byteReader{b: buf}, sh.ID)
		if errors.Is(err, storage.ErrUnavailable) {
			r.Plane.Health().MarkDown(target)
		}
		return err
	}
	return fmt.Errorf("no readable source: %w", errors.Join(errs...))
}

type byteReader struct {
	b   []byte
	off int
}

func (b *byteReader) Read(p []byte) (int, error) {
	if b.off >= len(b.b) {
		return 0, io.EOF
	}
	n := copy(p, b.b[b.off:])
	b.off += n
	return n, nil
}
