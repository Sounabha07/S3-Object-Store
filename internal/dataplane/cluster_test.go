package dataplane

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/storagetest"
)

// switchable is a store that can be "killed": while down, every operation
// fails with storage.ErrUnavailable, as a network client would.
type switchable struct {
	storage.Storage
	dir  string
	down atomic.Bool
}

func (s *switchable) err() error {
	if s.down.Load() {
		return fmt.Errorf("%w: node is down", storage.ErrUnavailable)
	}
	return nil
}

func (s *switchable) Put(ctx context.Context, r io.Reader, id storage.ShardID) (storage.ShardInfo, error) {
	if err := s.err(); err != nil {
		return storage.ShardInfo{}, err
	}
	return s.Storage.Put(ctx, r, id)
}

func (s *switchable) Get(ctx context.Context, id storage.ShardID) (io.ReadCloser, error) {
	if err := s.err(); err != nil {
		return nil, err
	}
	return s.Storage.Get(ctx, id)
}

func (s *switchable) Stat(ctx context.Context, id storage.ShardID) (storage.ShardInfo, error) {
	if err := s.err(); err != nil {
		return storage.ShardInfo{}, err
	}
	return s.Storage.Stat(ctx, id)
}

func (s *switchable) Health(ctx context.Context) error {
	if err := s.err(); err != nil {
		return err
	}
	return s.Storage.Health(ctx)
}

type testCluster struct {
	plane *Plane
	nodes map[placement.NodeID]*switchable
	ids   []placement.NodeID
}

// newTestCluster builds a replication-only cluster (see newECCluster).
func newTestCluster(t *testing.T, n int, chunkSize int64) *testCluster {
	t.Helper()
	return newCluster(t, n, Config{ChunkSize: chunkSize, DisableErasure: true})
}

func newCluster(t *testing.T, n int, cfg Config) *testCluster {
	t.Helper()
	tc := &testCluster{nodes: map[placement.NodeID]*switchable{}}
	stores := map[placement.NodeID]storage.Storage{}
	for i := 1; i <= n; i++ {
		id := placement.NodeID(fmt.Sprintf("storage-%d", i))
		dir := t.TempDir()
		d, err := disk.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = d.Close() })
		s := &switchable{Storage: d, dir: dir}
		tc.nodes[id], stores[id] = s, s
		tc.ids = append(tc.ids, id)
	}
	cfg.Nodes = stores
	p, err := NewCluster(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tc.plane = p
	return tc
}

func (tc *testCluster) write(t *testing.T, data []byte, key string) metadata.ObjectMeta {
	t.Helper()
	res, err := tc.plane.Write(ctx, bytes.NewReader(data), -1, key)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	return metadata.ObjectMeta{Size: res.Size, Chunks: res.Chunks}
}

func (tc *testCluster) readAll(obj metadata.ObjectMeta) ([]byte, error) {
	var buf bytes.Buffer
	err := tc.plane.Read(ctx, &buf, obj, 0, obj.Size)
	return buf.Bytes(), err
}

func TestReplicaPlacement(t *testing.T) {
	tc := newTestCluster(t, 6, 1000)
	obj := tc.write(t, storagetest.RandomBytes(t, 4500), "b/k/v1")
	if len(obj.Chunks) != 5 {
		t.Fatalf("got %d chunks", len(obj.Chunks))
	}
	for i, c := range obj.Chunks {
		if c.Scheme != metadata.Replication3 || len(c.Shards) != 3 {
			t.Fatalf("chunk %d: scheme %+v with %d shards", i, c.Scheme, len(c.Shards))
		}
		want, _ := tc.plane.Placement().Pick(PlacementKey("b/k/v1", i), 3)
		var got []placement.NodeID
		for j, s := range c.Shards {
			got = append(got, s.Node)
			if s.Index != j || s.ID != c.Shards[0].ID {
				t.Fatalf("chunk %d shard %d: %+v", i, j, s)
			}
			if _, err := tc.nodes[s.Node].Stat(ctx, s.ID); err != nil {
				t.Fatalf("chunk %d replica on %s missing: %v", i, s.Node, err)
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("chunk %d on %v, ring says %v", i, got, want)
		}
	}
}

// TestReadWithAnyOneNodeDown kills each node in turn.
func TestReadWithAnyOneNodeDown(t *testing.T) {
	tc := newTestCluster(t, 6, 1000)
	data := storagetest.RandomBytes(t, 20_000)
	obj := tc.write(t, data, "b/k/v1")
	for _, id := range tc.ids {
		tc.nodes[id].down.Store(true)
		got, err := tc.readAll(obj)
		tc.nodes[id].down.Store(false)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("read with %s down: %v", id, err)
		}
	}
}

// TestReadWithTwoReplicasGone: 3x replication tolerates losing any two
// copies of a chunk; losing all three fails cleanly.
func TestReadWithTwoReplicasGone(t *testing.T) {
	tc := newTestCluster(t, 6, 1000)
	data := storagetest.RandomBytes(t, 1000)
	obj := tc.write(t, data, "b/k/v1")
	shards := obj.Chunks[0].Shards
	for skip := 0; skip < 3; skip++ {
		for i, s := range shards {
			tc.nodes[s.Node].down.Store(i != skip)
		}
		if got, err := tc.readAll(obj); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("only replica %d alive: %v", skip, err)
		}
	}
	for _, s := range shards {
		tc.nodes[s.Node].down.Store(true)
	}
	if _, err := tc.readAll(obj); !errors.Is(err, storage.ErrUnavailable) {
		t.Fatalf("all replicas down: err = %v, want ErrUnavailable", err)
	}
}

func TestCorruptReplicaIsSkipped(t *testing.T) {
	tc := newTestCluster(t, 6, 1000)
	data := storagetest.RandomBytes(t, 1000)
	obj := tc.write(t, data, "b/k/v1")
	first := obj.Chunks[0].Shards[0]
	h := first.ID.String()
	p := filepath.Join(tc.nodes[first.Node].dir, "shards", h[0:2], h[2:4], h)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[10] ^= 1
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := tc.readAll(obj); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read with one corrupt replica: %v", err)
	}
	// Corrupt every replica: the error says so.
	for _, s := range obj.Chunks[0].Shards[1:] {
		if err := tc.nodes[s.Node].Delete(ctx, s.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tc.readAll(obj); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt among the causes", err)
	}
}

// TestWriteAvoidsDownNode: with a node down, every chunk still gets three
// replicas, none of them on the down node.
func TestWriteAvoidsDownNode(t *testing.T) {
	tc := newTestCluster(t, 6, 1000)
	dead := tc.ids[2]
	tc.nodes[dead].down.Store(true)
	data := storagetest.RandomBytes(t, 30_000)
	obj := tc.write(t, data, "b/k/v1")
	for i, c := range obj.Chunks {
		if len(c.Shards) != 3 {
			t.Fatalf("chunk %d has %d replicas", i, len(c.Shards))
		}
		for _, s := range c.Shards {
			if s.Node == dead {
				t.Fatalf("chunk %d placed on down node", i)
			}
		}
	}
	if !tc.plane.Health().IsDown(dead) {
		t.Fatal("failed node was not marked down")
	}
	tc.nodes[dead].down.Store(false)
	if got, err := tc.readAll(obj); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back: %v", err)
	}
}

func TestWriteFailsWithoutEnoughNodes(t *testing.T) {
	tc := newTestCluster(t, 4, 1000)
	tc.nodes[tc.ids[0]].down.Store(true)
	tc.nodes[tc.ids[1]].down.Store(true)
	_, err := tc.plane.Write(ctx, bytes.NewReader(make([]byte, 100)), -1, "b/k/v1")
	if !errors.Is(err, ErrInsufficientNodes) {
		t.Fatalf("err = %v, want ErrInsufficientNodes", err)
	}
}

func TestHealthProbe(t *testing.T) {
	tc := newTestCluster(t, 3, 1000)
	tc.nodes[tc.ids[1]].down.Store(true)
	stores := map[placement.NodeID]storage.Storage{}
	for id, s := range tc.nodes {
		stores[id] = s
	}
	h := tc.plane.Health()
	h.Probe(ctx, stores)
	if !h.IsDown(tc.ids[1]) || h.IsDown(tc.ids[0]) {
		t.Fatalf("down = %v", h.Down())
	}
	tc.nodes[tc.ids[1]].down.Store(false)
	h.Probe(ctx, stores)
	if h.IsDown(tc.ids[1]) {
		t.Fatal("recovered node still marked down")
	}
}

func TestNewClusterValidation(t *testing.T) {
	if _, err := NewCluster(Config{}); err == nil {
		t.Fatal("no nodes accepted")
	}
	tc := newTestCluster(t, 2, 0)
	stores := map[placement.NodeID]storage.Storage{}
	for id, s := range tc.nodes {
		stores[id] = s
	}
	if _, err := NewCluster(Config{Nodes: stores, Replicas: 3}); err == nil {
		t.Fatal("3 replicas on 2 nodes accepted")
	}
	p, err := NewCluster(Config{Nodes: stores})
	if err != nil || p.Replicas() != 2 {
		t.Fatalf("default replicas on 2 nodes = %d, %v", p.Replicas(), err)
	}
}
