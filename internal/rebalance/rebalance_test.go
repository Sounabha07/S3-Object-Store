package rebalance

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
)

var ctx = context.Background()

type env struct {
	meta  *boltstore.Store
	plane *dataplane.Plane
	data  map[string][]byte // key -> content
}

func newDisk(t *testing.T) *disk.Disk {
	t.Helper()
	d, err := disk.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func newEnv(t *testing.T, nodes int) *env {
	t.Helper()
	stores := map[placement.NodeID]storage.Storage{}
	for i := 1; i <= nodes; i++ {
		stores[placement.NodeID(fmt.Sprintf("storage-%d", i))] = newDisk(t)
	}
	p, err := dataplane.NewCluster(dataplane.Config{Nodes: stores, ChunkSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	m, err := boltstore.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if err := m.CreateBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	return &env{meta: m, plane: p, data: map[string][]byte{}}
}

// put stores an object the way the gateway does: data first, then metadata.
func (e *env) put(t *testing.T, key string, size int) {
	t.Helper()
	data := make([]byte, size)
	_, _ = rand.Read(data)
	version := "v-" + key
	res, err := e.plane.Write(ctx, bytes.NewReader(data), -1, dataplane.ObjectKey("b", key, version))
	if err != nil {
		t.Fatal(err)
	}
	err = e.meta.PutObject(ctx, metadata.ObjectMeta{Bucket: "b", Key: key, VersionID: version, Size: res.Size,
		LastModified: time.Now(), Chunks: res.Chunks})
	if err != nil {
		t.Fatal(err)
	}
	e.data[key] = data
}

// check verifies every object reads back and every shard is where metadata
// says. If wantPlaced, each chunk's nodes must equal the ring's choice.
func (e *env) check(t *testing.T, wantPlaced bool) {
	t.Helper()
	for key, want := range e.data {
		obj, err := e.meta.GetObject(ctx, "b", key)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := e.plane.Read(ctx, &buf, obj, 0, obj.Size); err != nil || !bytes.Equal(buf.Bytes(), want) {
			t.Fatalf("%s: read back failed: %v", key, err)
		}
		for i, c := range obj.Chunks {
			var nodes []placement.NodeID
			for _, s := range c.Shards {
				store, _ := e.plane.Node(s.Node)
				if _, err := store.Stat(ctx, s.ID); err != nil {
					t.Fatalf("%s chunk %d: shard missing on recorded node %s: %v", key, i, s.Node, err)
				}
				nodes = append(nodes, s.Node)
			}
			if wantPlaced {
				want, _ := e.plane.Placement().Pick(dataplane.PlacementKey(dataplane.ObjectKey("b", key, obj.VersionID), i), len(c.Shards))
				slices.Sort(nodes)
				slices.Sort(want)
				if !slices.Equal(nodes, want) {
					t.Fatalf("%s chunk %d on %v, ring wants %v", key, i, nodes, want)
				}
			}
		}
	}
}

func TestRebalanceAfterAddingNode(t *testing.T) {
	e := newEnv(t, 6)
	for i := 0; i < 60; i++ {
		e.put(t, fmt.Sprintf("obj-%02d", i), 4096*3+100) // 4 chunks each
	}
	e.check(t, true)

	newNode := newDisk(t)
	e.plane.AddNode("storage-7", newNode)
	r := &Rebalancer{Meta: e.meta, Plane: e.plane}
	st, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	totalShards := 60 * 4 * 3
	frac := float64(st.ShardsMoved) / float64(totalShards)
	t.Logf("moved %d of %d shard placements (%.1f%%), %d bytes", st.ShardsMoved, totalShards, frac*100, st.BytesCopied)
	if st.ShardsMoved == 0 || frac > 0.3 {
		t.Fatalf("moved %d shards (%.1f%%); expected roughly 1/7", st.ShardsMoved, frac*100)
	}
	if st.Errors != 0 || st.Conflicts != 0 || st.Skipped != 0 {
		t.Fatalf("unexpected stats %+v", st)
	}
	e.check(t, true)

	n := 0
	_ = newNode.Walk(ctx, func(storage.ShardInfo) error { n++; return nil })
	if n == 0 {
		t.Fatal("new node holds no shards after rebalancing")
	}

	// A second pass has nothing to do.
	st, err = r.Run(ctx)
	if err != nil || st.ShardsMoved != 0 {
		t.Fatalf("second pass: %+v, %v", st, err)
	}
}

// TestRebalanceCopiesFromAnotherReplica: when the shard that must move is
// missing on its own node, the copy comes from another replica.
func TestRebalanceCopiesFromAnotherReplica(t *testing.T) {
	e := newEnv(t, 3)
	for i := 0; i < 20; i++ {
		e.put(t, fmt.Sprint("k", i), 1000)
	}
	for i := 4; i <= 9; i++ {
		e.plane.AddNode(placement.NodeID(fmt.Sprintf("storage-%d", i)), newDisk(t))
	}
	// Find a replica the ring no longer wants, and delete it from its node.
	var victimKey string
	var victim metadata.ShardRef
	for key := range e.data {
		obj, _ := e.meta.GetObject(ctx, "b", key)
		want, _ := e.plane.Placement().Pick(dataplane.PlacementKey(dataplane.ObjectKey("b", key, obj.VersionID), 0), 3)
		for _, s := range obj.Chunks[0].Shards {
			if !slices.Contains(want, s.Node) {
				victimKey, victim = key, s
				break
			}
		}
		if victimKey != "" {
			break
		}
	}
	if victimKey == "" {
		t.Fatal("no replica needs to move; test setup is ineffective")
	}
	store, _ := e.plane.Node(victim.Node)
	if err := store.Delete(ctx, victim.ID); err != nil {
		t.Fatal(err)
	}

	r := &Rebalancer{Meta: e.meta, Plane: e.plane}
	st, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Skipped != 0 || st.Errors != 0 {
		t.Fatalf("stats %+v", st)
	}
	obj, _ := e.meta.GetObject(ctx, "b", victimKey)
	moved := obj.Chunks[0].Shards[victim.Index]
	if moved.Node == victim.Node {
		t.Fatal("replica whose source copy was missing was not moved")
	}
	dst, _ := e.plane.Node(moved.Node)
	if _, err := dst.Stat(ctx, moved.ID); err != nil {
		t.Fatalf("moved replica missing on %s: %v", moved.Node, err)
	}
	e.check(t, true)
}

// TestRebalanceSkipsOverwrittenObject: if an object is overwritten while a
// pass is in flight, the pass must not apply its stale move.
func TestRebalanceSkipsOverwrittenObject(t *testing.T) {
	e := newEnv(t, 3)
	e.put(t, "k", 1000)
	e.plane.AddNode("storage-4", newDisk(t))
	e.plane.AddNode("storage-5", newDisk(t))

	stale := &staleMeta{Store: e.meta}
	r := &Rebalancer{Meta: stale, Plane: e.plane}
	// Overwrite the object after the walk but before any move.
	stale.afterWalk = func() { e.put(t, "k", 2000) }
	st, err := r.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.ShardsMoved != 0 || st.Conflicts == 0 {
		t.Fatalf("stats %+v: stale moves must be rejected as conflicts", st)
	}
	e.check(t, false)
}

type staleMeta struct {
	*boltstore.Store
	afterWalk func()
}

func (s *staleMeta) WalkChunks(ctx context.Context, fn func(string, string, string, int, metadata.Chunk) error) error {
	err := s.Store.WalkChunks(ctx, fn)
	if s.afterWalk != nil {
		s.afterWalk()
	}
	return err
}
