package repair

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
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

// node is a disk store that can be switched off (ErrUnavailable).
type node struct {
	storage.Storage
	dir  string
	down atomic.Bool
}

func (n *node) err() error {
	if n.down.Load() {
		return fmt.Errorf("%w: down", storage.ErrUnavailable)
	}
	return nil
}
func (n *node) Put(c context.Context, r io.Reader, id storage.ShardID) (storage.ShardInfo, error) {
	if err := n.err(); err != nil {
		return storage.ShardInfo{}, err
	}
	return n.Storage.Put(c, r, id)
}
func (n *node) Get(c context.Context, id storage.ShardID) (io.ReadCloser, error) {
	if err := n.err(); err != nil {
		return nil, err
	}
	return n.Storage.Get(c, id)
}
func (n *node) Stat(c context.Context, id storage.ShardID) (storage.ShardInfo, error) {
	if err := n.err(); err != nil {
		return storage.ShardInfo{}, err
	}
	return n.Storage.Stat(c, id)
}

type env struct {
	t     *testing.T
	meta  *boltstore.Store
	plane *dataplane.Plane
	nodes map[placement.NodeID]*node
	ids   []placement.NodeID
	data  map[string][]byte
}

func newDisk(t *testing.T, dir string) *disk.Disk {
	t.Helper()
	d, err := disk.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func newEnv(t *testing.T) *env { return newEnvN(t, 6) }

func newEnvN(t *testing.T, nodes int) *env {
	t.Helper()
	e := &env{t: t, nodes: map[placement.NodeID]*node{}, data: map[string][]byte{}}
	stores := map[placement.NodeID]storage.Storage{}
	for i := 1; i <= nodes; i++ {
		id := placement.NodeID(fmt.Sprintf("storage-%d", i))
		dir := t.TempDir()
		n := &node{Storage: newDisk(t, dir), dir: dir}
		e.nodes[id], stores[id] = n, n
		e.ids = append(e.ids, id)
	}
	p, err := dataplane.NewCluster(dataplane.Config{Nodes: stores, ChunkSize: 64 << 10, SmallObjectThreshold: 32 << 10})
	if err != nil {
		t.Fatal(err)
	}
	e.plane = p
	m, err := boltstore.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	e.meta = m
	if err := m.CreateBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) put(key string, size int) {
	e.t.Helper()
	data := make([]byte, size)
	_, _ = rand.Read(data)
	v := "v-" + key
	res, err := e.plane.Write(ctx, bytes.NewReader(data), int64(size), dataplane.ObjectKey("b", key, v))
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.meta.PutObject(ctx, metadata.ObjectMeta{Bucket: "b", Key: key, VersionID: v, Size: res.Size, LastModified: time.Now(), Chunks: res.Chunks}); err != nil {
		e.t.Fatal(err)
	}
	e.data[key] = data
}

func (e *env) populate() {
	for i := 0; i < 10; i++ {
		e.put(fmt.Sprint("ec-", i), 200_000) // erasure coded, 4 chunks
		e.put(fmt.Sprint("rep-", i), 10_000) // replicated
	}
}

func (e *env) repairer(deep bool) *Repairer {
	return &Repairer{Meta: e.meta, Plane: e.plane, Deep: deep}
}

// fullyRedundant checks every chunk has every shard present and verified,
// on distinct, healthy nodes.
func (e *env) fullyRedundant() {
	e.t.Helper()
	err := e.meta.WalkChunks(ctx, func(bucket, key, version string, idx int, c metadata.Chunk) error {
		seen := map[placement.NodeID]bool{}
		for _, ref := range c.Shards {
			if ref.Node == "" || e.nodes[ref.Node].down.Load() {
				return fmt.Errorf("%s chunk %d shard %d on %q", key, idx, ref.Index, ref.Node)
			}
			if seen[ref.Node] {
				return fmt.Errorf("%s chunk %d has two shards on %s", key, idx, ref.Node)
			}
			seen[ref.Node] = true
			buf := make([]byte, ref.Size)
			if err := e.plane.ReadShard(ctx, ref, buf); err != nil {
				return fmt.Errorf("%s chunk %d shard %d: %v", key, idx, ref.Index, err)
			}
		}
		return nil
	})
	if err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) readAll() {
	e.t.Helper()
	for key, want := range e.data {
		obj, err := e.meta.GetObject(ctx, "b", key)
		if err != nil {
			e.t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := e.plane.Read(ctx, &buf, obj, 0, obj.Size); err != nil || !bytes.Equal(buf.Bytes(), want) {
			e.t.Fatalf("%s: %v", key, err)
		}
	}
}

func (e *env) setDown(ids ...placement.NodeID) {
	for _, id := range e.ids {
		e.nodes[id].down.Store(false)
		e.plane.Health().MarkUp(id)
	}
	for _, id := range ids {
		e.nodes[id].down.Store(true)
		e.plane.Health().MarkDown(id)
	}
}

// TestRepairRestoresRedundancyAfterNodeWiped: a node loses all its data
// (disk replaced). One pass rebuilds every lost shard elsewhere, after
// which the data survives two further node failures.
func TestRepairRestoresRedundancyAfterNodeWiped(t *testing.T) {
	e := newEnv(t)
	e.populate()
	victim := e.ids[2]
	n := e.nodes[victim]
	if err := os.RemoveAll(filepath.Join(n.dir, "shards")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(n.dir, "shards"), 0o750); err != nil {
		t.Fatal(err)
	}

	st, err := e.repairer(false).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("repair stats: %+v", st)
	if st.ChunksDegraded == 0 || st.ChunksRepaired != st.ChunksDegraded || st.Unrecoverable != 0 || st.Errors != 0 || st.Conflicts != 0 {
		t.Fatalf("stats %+v", st)
	}
	e.fullyRedundant()

	// Full redundancy means any two other nodes can now fail.
	others := []placement.NodeID{}
	for _, id := range e.ids {
		if id != victim {
			others = append(others, id)
		}
	}
	e.setDown(others[0], others[1])
	e.readAll()
	e.setDown()

	st, err = e.repairer(false).Run(ctx)
	if err != nil || st.ChunksDegraded != 0 {
		t.Fatalf("second pass: %+v, %v", st, err)
	}
}

// TestRepairMovesShardsOffDownNode treats a down node's shards as lost and
// rebuilds them on the remaining nodes. With 7 nodes there is room to keep
// all 6 erasure-coded shards on distinct nodes.
func TestRepairMovesShardsOffDownNode(t *testing.T) {
	e := newEnvN(t, 7)
	e.populate()
	dead := e.ids[0]
	e.setDown(dead)
	st, err := e.repairer(false).Run(ctx)
	if err != nil || st.Unrecoverable != 0 || st.Errors != 0 || st.NoCapacity != 0 || st.ChunksRepaired != st.ChunksDegraded || st.ChunksDegraded == 0 {
		t.Fatalf("stats %+v, %v", st, err)
	}
	e.fullyRedundant()
	e.readAll()
}

// TestRepairWithoutSpareNode: with 6 nodes and one down there is no 6th
// distinct node for an erasure-coded chunk. Repair reports that as a
// capacity shortfall (not an error) and still fixes replicated chunks.
func TestRepairWithoutSpareNode(t *testing.T) {
	e := newEnv(t)
	e.populate()
	e.setDown(e.ids[0])
	st, err := e.repairer(false).Run(ctx)
	if err != nil || st.Errors != 0 || st.Unrecoverable != 0 || st.NoCapacity == 0 {
		t.Fatalf("stats %+v, %v", st, err)
	}
	e.readAll()
}

// TestDeepRepairFindsCorruption: silent corruption is invisible to the
// existence check but found and fixed by a deep pass.
func TestDeepRepairFindsCorruption(t *testing.T) {
	e := newEnv(t)
	e.put("ec", 64<<10)
	obj, _ := e.meta.GetObject(ctx, "b", "ec")
	ref := obj.Chunks[0].Shards[1]
	h := ref.ID.String()
	p := filepath.Join(e.nodes[ref.Node].dir, "shards", h[0:2], h[2:4], h)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 1
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	if st, _ := e.repairer(false).Run(ctx); st.ChunksDegraded != 0 {
		t.Fatalf("shallow pass should not read data: %+v", st)
	}
	st, err := e.repairer(true).Run(ctx)
	if err != nil || st.ShardsRebuilt != 1 || st.ChunksRepaired != 1 {
		t.Fatalf("deep pass: %+v, %v", st, err)
	}
	obj, _ = e.meta.GetObject(ctx, "b", "ec")
	e.fullyRedundant() // the shard verifies again (rewritten in place)
	// Two more failures among the chunk's nodes are survivable again.
	e.setDown(obj.Chunks[0].Shards[0].Node, obj.Chunks[0].Shards[2].Node)
	e.readAll()
}

// TestRepairPlacesUnplacedShards: a chunk written while a node was down
// has one unplaced shard; repair places it.
func TestRepairPlacesUnplacedShards(t *testing.T) {
	e := newEnv(t)
	e.setDown(e.ids[5])
	e.put("ec", 100_000)
	e.setDown()
	obj, _ := e.meta.GetObject(ctx, "b", "ec")
	unplaced := 0
	for _, c := range obj.Chunks {
		for _, s := range c.Shards {
			if s.Node == "" {
				unplaced++
			}
		}
	}
	if unplaced == 0 {
		t.Fatal("setup: expected unplaced shards")
	}
	st, err := e.repairer(false).Run(ctx)
	if err != nil || st.ShardsRebuilt != unplaced {
		t.Fatalf("stats %+v, %v (unplaced %d)", st, err, unplaced)
	}
	e.fullyRedundant()
}

func TestRepairReportsUnrecoverable(t *testing.T) {
	e := newEnv(t)
	e.put("ec", 64<<10)
	obj, _ := e.meta.GetObject(ctx, "b", "ec")
	for _, ref := range obj.Chunks[0].Shards[:3] {
		if err := e.nodes[ref.Node].Delete(ctx, ref.ID); err != nil {
			t.Fatal(err)
		}
	}
	st, err := e.repairer(false).Run(ctx)
	if err != nil || st.Unrecoverable != 1 || st.ShardsRebuilt != 0 {
		t.Fatalf("stats %+v, %v", st, err)
	}
}

// TestRepairNeverUndoesNewerWrites: if the object is overwritten while a
// pass is running, the pass's moves are rejected.
func TestRepairNeverUndoesNewerWrites(t *testing.T) {
	e := newEnv(t)
	e.put("k", 10_000)
	obj, _ := e.meta.GetObject(ctx, "b", "k")
	e.setDown(obj.Chunks[0].Shards[0].Node) // forces a relocation, i.e. a metadata update
	r := e.repairer(false)
	r.Meta = &overwriteDuringRepair{Store: e.meta, overwrite: func() { e.put("k", 20_000) }}
	st, err := r.Run(ctx)
	if err != nil || st.Conflicts == 0 || st.ShardsRebuilt != 0 {
		t.Fatalf("stats %+v, %v", st, err)
	}
	e.readAll() // the newer version is intact
}

type overwriteDuringRepair struct {
	*boltstore.Store
	overwrite func()
}

func (o *overwriteDuringRepair) WalkChunks(c context.Context, fn func(string, string, string, int, metadata.Chunk) error) error {
	err := o.Store.WalkChunks(c, fn)
	o.overwrite()
	return err
}

func TestQueueDepthResetsAfterPass(t *testing.T) {
	e := newEnv(t)
	e.populate()
	e.setDown(e.ids[3])
	r := e.repairer(false)
	if _, err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if r.QueueDepth() != 0 {
		t.Fatalf("queue depth %d after pass", r.QueueDepth())
	}
}
