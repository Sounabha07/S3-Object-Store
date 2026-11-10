// Package gc deletes shards that no metadata references.
//
// A pass is mark-and-sweep:
//
//   - mark: walk every committed chunk and every in-progress multipart part,
//     recording which shard IDs each node is supposed to hold;
//   - sweep: list each healthy node's shards and delete those that are
//     unreferenced and older than the grace period.
//
// Why this is safe:
//
//   - Data is always written before the metadata that references it, so
//     a shard written by an upload that has not committed yet is young and
//     protected by the grace period. The grace period must exceed the
//     longest time between writing a shard and committing its metadata.
//   - Deletes are conditional (storage.ConditionalDeleter): a node deletes
//     only if the shard is still older than the cutoff. A Put that reuses an
//     existing shard (content addressing deduplicates) refreshes its
//     modification time under the same lock, so a shard reused by an
//     in-flight upload is never deleted from under it.
//   - References are per node: a shard moved by repair or rebalancing is
//     garbage on its old node even if the same ID is referenced elsewhere.
package gc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// DefaultGrace is the default minimum age of an unreferenced shard before
// it is deleted.
const DefaultGrace = time.Hour

// Stats summarises one pass.
type Stats struct {
	Referenced     int // (node, shard) references found in metadata
	NodesSwept     int
	NodesSkipped   int // down, or without conditional delete
	ShardsScanned  int
	Deleted        int
	BytesFreed     int64
	SkippedYoung   int // unreferenced but within the grace period
	SkippedChanged int // touched between listing and deletion
	Errors         int
}

// Collector runs GC passes.
type Collector struct {
	Meta  metadata.Metadata
	Plane *dataplane.Plane
	Grace time.Duration // <= 0: DefaultGrace
	Log   *slog.Logger
	Now   func() time.Time // for tests; nil means time.Now
}

type refSet map[placement.NodeID]map[storage.ShardID]bool

func (r refSet) add(c metadata.Chunk) int {
	n := 0
	for _, s := range c.Shards {
		if s.Node == "" {
			continue
		}
		if r[s.Node] == nil {
			r[s.Node] = map[storage.ShardID]bool{}
		}
		r[s.Node][s.ID] = true
		n++
	}
	return n
}

// Run performs one pass.
func (c *Collector) Run(ctx context.Context) (Stats, error) {
	log := c.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	grace := c.Grace
	if grace <= 0 {
		grace = DefaultGrace
	}
	// The cutoff is fixed before marking: anything written after the mark
	// started is younger than it and therefore safe.
	cutoff := now().Add(-grace)

	var st Stats
	refs := refSet{}
	err := c.Meta.WalkChunks(ctx, func(_, _, _ string, _ int, ch metadata.Chunk) error {
		st.Referenced += refs.add(ch)
		return nil
	})
	if err != nil {
		return st, fmt.Errorf("gc: mark objects: %w", err)
	}
	err = c.Meta.WalkPartChunks(ctx, func(_ string, _ int, ch metadata.Chunk) error {
		st.Referenced += refs.add(ch)
		return nil
	})
	if err != nil {
		return st, fmt.Errorf("gc: mark parts: %w", err)
	}

	for id, store := range c.Plane.Nodes() {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		cd, ok := store.(storage.ConditionalDeleter)
		if !ok || c.Plane.Health().IsDown(id) {
			st.NodesSkipped++
			continue
		}
		var garbage []storage.ShardInfo
		err := store.Walk(ctx, func(info storage.ShardInfo) error {
			st.ShardsScanned++
			if refs[id][info.ID] {
				return nil
			}
			if !info.ModTime.Before(cutoff) {
				st.SkippedYoung++
				return nil
			}
			garbage = append(garbage, info)
			return nil
		})
		if err != nil {
			st.Errors++
			log.Warn("gc: listing failed", "node", id, "err", err)
			continue
		}
		st.NodesSwept++
		for _, g := range garbage {
			err := cd.DeleteIfOlder(ctx, g.ID, cutoff)
			switch {
			case err == nil:
				st.Deleted++
				st.BytesFreed += g.Size
			case errors.Is(err, storage.ErrModified):
				st.SkippedChanged++
			case errors.Is(err, storage.ErrNotFound):
				// already gone
			default:
				st.Errors++
				log.Warn("gc: delete failed", "node", id, "shard", g.ID, "err", err)
			}
		}
	}
	if st.Deleted > 0 || st.Errors > 0 {
		log.Info("gc pass done", "referenced", st.Referenced, "scanned", st.ShardsScanned, "deleted", st.Deleted,
			"bytes_freed", st.BytesFreed, "young", st.SkippedYoung, "changed", st.SkippedChanged, "errors", st.Errors)
	}
	return st, nil
}
