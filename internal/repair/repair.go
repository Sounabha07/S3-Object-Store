// Package repair restores full redundancy for every committed chunk.
//
// A pass scans all chunks and classifies each shard as good or bad. A shard
// is bad if it was never placed, its node is down or unknown, the node
// does not have it, or (in deep mode) its bytes fail SHA-256 verification.
// Bad shards are rebuilt: replicas are copied from a verified good replica,
// and erasure-coded shards are reconstructed from any k verified shards.
// The rebuilt shard is written to a healthy node that holds no other shard
// of the chunk, then moved in metadata with a conditional update, so a
// concurrent overwrite, delete or other repair is never undone. Whatever
// the bad location still holds becomes garbage for GC.
package repair

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync/atomic"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// Stats summarises one pass.
type Stats struct {
	ChunksScanned  int
	ChunksDegraded int // chunks that had at least one bad shard
	ChunksRepaired int // degraded chunks now fully redundant
	ShardsRebuilt  int
	BytesWritten   int64
	Unrecoverable  int // too few good shards left to rebuild
	NoCapacity     int // no eligible healthy node to hold a rebuilt shard
	Conflicts      int // metadata changed during repair; retried next pass
	Errors         int
}

// Repairer runs repair passes. It is safe to run one pass at a time per
// Repairer; passes on different gateways are safe against each other.
type Repairer struct {
	Meta  metadata.Metadata
	Plane *dataplane.Plane
	Log   *slog.Logger
	// Deep reads and verifies every shard instead of only checking that it
	// exists. It finds silent corruption at the cost of reading all data.
	Deep bool

	queue atomic.Int64
}

// QueueDepth is the number of degraded chunks found by the current pass
// that have not been processed yet (0 between passes).
func (r *Repairer) QueueDepth() int64 { return r.queue.Load() }

type item struct {
	bucket, key, version string
	index                int
	chunk                metadata.Chunk
	bad                  []int // indexes into chunk.Shards
}

// Run performs one pass.
func (r *Repairer) Run(ctx context.Context) (Stats, error) {
	log := r.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	var all []item
	err := r.Meta.WalkChunks(ctx, func(bucket, key, version string, idx int, c metadata.Chunk) error {
		all = append(all, item{bucket: bucket, key: key, version: version, index: idx, chunk: c})
		return nil
	})
	if err != nil {
		return Stats{}, fmt.Errorf("repair: walk metadata: %w", err)
	}
	var st Stats
	var queue []item
	for _, it := range all {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		st.ChunksScanned++
		it.bad = r.badShards(ctx, it.chunk)
		if len(it.bad) > 0 {
			st.ChunksDegraded++
			queue = append(queue, it)
		}
	}
	r.queue.Store(int64(len(queue)))
	defer r.queue.Store(0)
	for _, it := range queue {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		r.repairChunk(ctx, it, &st, log)
		r.queue.Add(-1)
	}
	if st.ChunksDegraded > 0 || st.Errors > 0 {
		log.Info("repair pass done", "scanned", st.ChunksScanned, "degraded", st.ChunksDegraded,
			"repaired", st.ChunksRepaired, "shards", st.ShardsRebuilt, "bytes", st.BytesWritten,
			"unrecoverable", st.Unrecoverable, "no_capacity", st.NoCapacity, "conflicts", st.Conflicts, "errors", st.Errors)
	}
	return st, nil
}

// badShards returns the indexes of shards that need rebuilding.
func (r *Repairer) badShards(ctx context.Context, c metadata.Chunk) []int {
	health := r.Plane.Health()
	var bad []int
	var buf []byte
	for i, ref := range c.Shards {
		if ref.Node == "" || health.IsDown(ref.Node) {
			bad = append(bad, i)
			continue
		}
		store, ok := r.Plane.Node(ref.Node)
		if !ok {
			bad = append(bad, i)
			continue
		}
		if r.Deep {
			if int64(cap(buf)) < ref.Size {
				buf = make([]byte, ref.Size)
			}
			if err := r.Plane.ReadShard(ctx, ref, buf[:ref.Size]); err != nil {
				bad = append(bad, i)
			}
			continue
		}
		if _, err := store.Stat(ctx, ref.ID); err != nil {
			if errors.Is(err, storage.ErrUnavailable) {
				health.MarkDown(ref.Node)
			}
			bad = append(bad, i)
		}
	}
	return bad
}

func (r *Repairer) repairChunk(ctx context.Context, it item, st *Stats, log *slog.Logger) {
	c := it.chunk
	contents, err := r.rebuild(ctx, c, it.bad)
	if err != nil {
		if errors.Is(err, errUnrecoverable) {
			st.Unrecoverable++
			log.Error("chunk unrecoverable", "bucket", it.bucket, "key", it.key, "chunk", it.index, "err", err)
		} else {
			st.Errors++
			log.Warn("repair read failed", "bucket", it.bucket, "key", it.key, "chunk", it.index, "err", err)
		}
		return
	}

	// Nodes that must not receive a rebuilt shard: those holding a good
	// shard of this chunk (keeps shards on distinct nodes) and down nodes.
	exclude := r.Plane.Health().Down()
	for i, ref := range c.Shards {
		if !slices.Contains(it.bad, i) && ref.Node != "" {
			exclude = append(exclude, ref.Node)
		}
	}
	key := dataplane.PlacementKey(dataplane.ObjectKey(it.bucket, it.key, it.version), it.index)
	fixed := 0
	for _, i := range it.bad {
		ref := c.Shards[i]
		data := contents[i]
		// If the shard's node is up (it lost or corrupted the shard),
		// rewrite it in place: no metadata change, placement unchanged.
		if r.rewriteInPlace(ctx, ref, data) {
			fixed++
			st.ShardsRebuilt++
			st.BytesWritten += int64(len(data))
			continue
		}
		target, err := r.place(ctx, key, ref.ID, data, append(exclude, ref.Node))
		if err != nil {
			if errors.Is(err, placement.ErrNotEnoughNodes) {
				st.NoCapacity++
			} else {
				st.Errors++
				log.Warn("repair write failed", "bucket", it.bucket, "key", it.key, "chunk", it.index, "shard", i, "err", err)
			}
			continue
		}
		exclude = append(exclude, target)
		err = r.Meta.UpdateShardLocation(ctx, metadata.ShardLocationUpdate{
			Bucket: it.bucket, Key: it.key, VersionID: it.version,
			ChunkIndex: it.index, ShardIndex: i, From: ref.Node, To: target,
		})
		switch {
		case err == nil:
			fixed++
			st.ShardsRebuilt++
			st.BytesWritten += int64(len(data))
		case errors.Is(err, metadata.ErrShardLocationConflict), errors.Is(err, metadata.ErrNoSuchKey), errors.Is(err, metadata.ErrNoSuchBucket):
			st.Conflicts++
		default:
			st.Errors++
			log.Warn("repair metadata update failed", "err", err)
		}
	}
	if fixed == len(it.bad) {
		st.ChunksRepaired++
	}
}

// rewriteInPlace restores a shard on its recorded node when that node is
// reachable. A corrupt copy is deleted first: the store is content
// addressed, so a Put of the same ID would otherwise keep the bad file.
func (r *Repairer) rewriteInPlace(ctx context.Context, ref metadata.ShardRef, data []byte) bool {
	if ref.Node == "" || r.Plane.Health().IsDown(ref.Node) {
		return false
	}
	store, ok := r.Plane.Node(ref.Node)
	if !ok {
		return false
	}
	if err := store.Delete(ctx, ref.ID); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return false
	}
	if _, err := store.Put(ctx, &sliceReader{b: data}, ref.ID); err != nil {
		if errors.Is(err, storage.ErrUnavailable) {
			r.Plane.Health().MarkDown(ref.Node)
		}
		return false
	}
	return true
}

var errUnrecoverable = errors.New("repair: too few good shards")

// rebuild returns the contents of every bad shard, indexed like c.Shards.
func (r *Repairer) rebuild(ctx context.Context, c metadata.Chunk, bad []int) (map[int][]byte, error) {
	out := map[int][]byte{}
	if c.Scheme.IsReplication() {
		for i, ref := range c.Shards {
			if slices.Contains(bad, i) {
				continue
			}
			buf := make([]byte, ref.Size)
			if err := r.Plane.ReadShard(ctx, ref, buf); err != nil {
				continue // try the next replica
			}
			for _, b := range bad {
				out[b] = buf
			}
			return out, nil
		}
		return nil, fmt.Errorf("%w: no readable replica", errUnrecoverable)
	}

	codec, err := r.Plane.Codec(c.Scheme)
	if err != nil {
		return nil, err
	}
	if len(c.Shards) != codec.Total() {
		return nil, fmt.Errorf("chunk has %d shards, scheme needs %d", len(c.Shards), codec.Total())
	}
	shards := make([][]byte, codec.Total())
	good := 0
	for i, ref := range c.Shards {
		if slices.Contains(bad, i) || good == codec.Data() {
			continue
		}
		buf := make([]byte, ref.Size)
		if err := r.Plane.ReadShard(ctx, ref, buf); err != nil {
			continue
		}
		shards[ref.Index] = buf
		good++
	}
	if good < codec.Data() {
		return nil, fmt.Errorf("%w: %d of %d needed shards readable", errUnrecoverable, good, codec.Data())
	}
	if err := codec.Reconstruct(shards); err != nil {
		return nil, err
	}
	for _, b := range bad {
		ref := c.Shards[b]
		data := shards[ref.Index]
		if storage.ComputeShardID(data) != ref.ID {
			return nil, fmt.Errorf("reconstructed shard %d does not match its recorded checksum", ref.Index)
		}
		out[b] = data
	}
	return out, nil
}

// place stores data on the first eligible ring node for key.
func (r *Repairer) place(ctx context.Context, key string, id storage.ShardID, data []byte, exclude []placement.NodeID) (placement.NodeID, error) {
	var errs []error
	for attempt := 0; attempt < 3; attempt++ {
		targets, err := r.Plane.Placement().Pick(key, 1, exclude...)
		if err != nil {
			return "", errors.Join(append(errs, err)...)
		}
		target := targets[0]
		store, _ := r.Plane.Node(target)
		if _, err := store.Put(ctx, &sliceReader{b: data}, id); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", target, err))
			if errors.Is(err, storage.ErrUnavailable) {
				r.Plane.Health().MarkDown(target)
			}
			exclude = append(exclude, target)
			continue
		}
		return target, nil
	}
	return "", errors.Join(errs...)
}

type sliceReader struct {
	b   []byte
	off int
}

func (s *sliceReader) Read(p []byte) (int, error) {
	if s.off >= len(s.b) {
		return 0, io.EOF
	}
	n := copy(p, s.b[s.off:])
	s.off += n
	return n, nil
}
