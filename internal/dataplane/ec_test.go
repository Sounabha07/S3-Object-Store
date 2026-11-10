package dataplane

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/storagetest"
)

const ecChunk = 64 << 10 // small chunks keep the exhaustive tests fast

func newECCluster(t *testing.T) *testCluster {
	t.Helper()
	return newCluster(t, 6, Config{ChunkSize: ecChunk, SmallObjectThreshold: 32 << 10})
}

// writeSized stores data with its size declared, as the gateway does.
func (tc *testCluster) writeSized(t *testing.T, data []byte, key string) metadata.ObjectMeta {
	t.Helper()
	res, err := tc.plane.Write(ctx, bytes.NewReader(data), int64(len(data)), key)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	return metadata.ObjectMeta{Size: res.Size, Chunks: res.Chunks}
}

func combos(n, r int) [][]int {
	var out [][]int
	var rec func(start int, cur []int)
	rec = func(start int, cur []int) {
		if len(cur) == r {
			out = append(out, append([]int(nil), cur...))
			return
		}
		for i := start; i < n; i++ {
			rec(i+1, append(cur, i))
		}
	}
	rec(0, nil)
	return out
}

func (tc *testCluster) setDown(ids []placement.NodeID) {
	for _, id := range tc.ids {
		tc.nodes[id].down.Store(false)
	}
	for _, id := range ids {
		tc.nodes[id].down.Store(true)
	}
	for _, id := range tc.ids {
		tc.plane.Health().MarkUp(id) // start each case without remembered failures
	}
}

func TestSchemeSelection(t *testing.T) {
	tc := newECCluster(t)
	small := tc.writeSized(t, storagetest.RandomBytes(t, 1000), "b/small/v")
	if small.Chunks[0].Scheme != metadata.Replication3 {
		t.Fatalf("small object scheme %+v, want 3x replication", small.Chunks[0].Scheme)
	}
	large := tc.writeSized(t, storagetest.RandomBytes(t, 3*ecChunk+5), "b/large/v")
	for i, c := range large.Chunks {
		if c.Scheme != metadata.EC42 || len(c.Shards) != 6 {
			t.Fatalf("chunk %d: %+v with %d shards", i, c.Scheme, len(c.Shards))
		}
		want, _ := tc.plane.Placement().Pick(PlacementKey("b/large/v", i), 6)
		seen := map[placement.NodeID]bool{}
		for j, s := range c.Shards {
			if s.Index != j || s.Node != want[j] || seen[s.Node] {
				t.Fatalf("chunk %d shard %d on %s (ring %v)", i, j, s.Node, want)
			}
			seen[s.Node] = true
			if s.Size != int64((c.Size+3)/4) {
				t.Fatalf("chunk %d shard %d size %d for chunk of %d", i, j, s.Size, c.Size)
			}
		}
	}
	// Unknown size (streaming) uses erasure coding too.
	res, err := tc.plane.Write(ctx, bytes.NewReader(make([]byte, 100)), -1, "b/stream/v")
	if err != nil || res.Chunks[0].Scheme != metadata.EC42 {
		t.Fatalf("unknown-size write: %+v, %v", res.Chunks, err)
	}
}

// TestReadWithAnyTwoNodesDown: every one of the 15 pairs of the 6 nodes.
func TestReadWithAnyTwoNodesDown(t *testing.T) {
	tc := newECCluster(t)
	data := storagetest.RandomBytes(t, 3*ecChunk+777)
	obj := tc.writeSized(t, data, "b/k/v")
	for _, pair := range combos(6, 2) {
		tc.setDown([]placement.NodeID{tc.ids[pair[0]], tc.ids[pair[1]]})
		got, err := tc.readAll(obj)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("nodes %v down: %v", pair, err)
		}
		// A range spanning a chunk boundary too.
		var buf bytes.Buffer
		if err := tc.plane.Read(ctx, &buf, obj, ecChunk-10, 20); err != nil || !bytes.Equal(buf.Bytes(), data[ecChunk-10:ecChunk+10]) {
			t.Fatalf("nodes %v down, range read: %v", pair, err)
		}
	}
}

// TestReadFailsCleanlyWithThreeShardsGone: every one of the 20 triples of a
// chunk's shard nodes. The read must fail with ErrChunkUnavailable and must
// not emit any bytes of that chunk.
func TestReadFailsCleanlyWithThreeShardsGone(t *testing.T) {
	tc := newECCluster(t)
	data := storagetest.RandomBytes(t, ecChunk) // exactly one chunk
	obj := tc.writeSized(t, data, "b/k/v")
	shards := obj.Chunks[0].Shards
	for _, triple := range combos(6, 3) {
		var down []placement.NodeID
		for _, i := range triple {
			down = append(down, shards[i].Node)
		}
		tc.setDown(down)
		got, err := tc.readAll(obj)
		if !errors.Is(err, ErrChunkUnavailable) || !errors.Is(err, storage.ErrUnavailable) {
			t.Fatalf("shards %v down: err = %v", triple, err)
		}
		if len(got) != 0 {
			t.Fatalf("shards %v down: %d bytes emitted before failing", triple, len(got))
		}
	}
}

func ecShardPath(tc *testCluster, ref metadata.ShardRef) string {
	h := ref.ID.String()
	return filepath.Join(tc.nodes[ref.Node].dir, "shards", h[0:2], h[2:4], h)
}

func corrupt(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0x40
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestCorruptShardsReconstructed: corrupted shards are detected by their
// checksum and rebuilt from the others, up to the parity count.
func TestCorruptShardsReconstructed(t *testing.T) {
	tc := newECCluster(t)
	data := storagetest.RandomBytes(t, ecChunk)
	obj := tc.writeSized(t, data, "b/k/v")
	shards := obj.Chunks[0].Shards

	corrupt(t, ecShardPath(tc, shards[0])) // a data shard
	if got, err := tc.readAll(obj); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("one corrupt data shard: %v", err)
	}
	corrupt(t, ecShardPath(tc, shards[2])) // a second data shard
	if got, err := tc.readAll(obj); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("two corrupt data shards: %v", err)
	}
	corrupt(t, ecShardPath(tc, shards[4])) // a parity shard: now 3 bad
	_, err := tc.readAll(obj)
	if !errors.Is(err, ErrChunkUnavailable) || !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("three corrupt shards: err = %v, want ErrChunkUnavailable caused by ErrCorrupt", err)
	}
}

// TestECWriteWithNodesDown: with one of six nodes down a chunk is stored
// as 5 shards (one recorded as unplaced, for repair) and stays readable
// even after one more node fails. With two down, writes are refused.
func TestECWriteWithNodesDown(t *testing.T) {
	tc := newECCluster(t)
	tc.setDown([]placement.NodeID{tc.ids[0]})
	data := storagetest.RandomBytes(t, 2*ecChunk)
	obj := tc.writeSized(t, data, "b/k/v")
	for i, c := range obj.Chunks {
		placed := 0
		for _, s := range c.Shards {
			if s.Node == tc.ids[0] {
				t.Fatalf("chunk %d: shard on down node", i)
			}
			if s.Node != "" {
				placed++
			}
		}
		if placed != 5 {
			t.Fatalf("chunk %d: %d shards placed, want 5", i, placed)
		}
	}
	tc.setDown([]placement.NodeID{tc.ids[0], obj.Chunks[0].Shards[0].Node})
	if got, err := tc.readAll(obj); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read with a further node down: %v", err)
	}

	tc.setDown([]placement.NodeID{tc.ids[0], tc.ids[1]})
	_, err := tc.plane.Write(ctx, bytes.NewReader(data), int64(len(data)), "b/k2/v")
	if !errors.Is(err, ErrInsufficientNodes) {
		t.Fatalf("write with 2 of 6 nodes down: err = %v, want ErrInsufficientNodes", err)
	}
}

// diskBytes sums the size of every shard file on every node.
func diskBytes(t *testing.T, tc *testCluster) int64 {
	t.Helper()
	var total int64
	for _, id := range tc.ids {
		_ = tc.nodes[id].Walk(ctx, func(i storage.ShardInfo) error { total += i.Size; return nil })
	}
	return total
}

// TestStorageOverhead measures bytes on disk per logical byte for both
// schemes (identical chunks dedupe, so random data is used).
func TestStorageOverhead(t *testing.T) {
	ec := newECCluster(t)
	logical := int64(8 * ecChunk) // whole chunks: no padding
	ec.writeSized(t, storagetest.RandomBytes(t, int(logical)), "b/ec/v")
	if got := float64(diskBytes(t, ec)) / float64(logical); got != 1.5 {
		t.Fatalf("RS(4,2) overhead %.3f, want 1.5", got)
	}
	rep := newTestCluster(t, 6, ecChunk)
	rep.write(t, storagetest.RandomBytes(t, int(logical)), "b/rep/v")
	if got := float64(diskBytes(t, rep)) / float64(logical); got != 3 {
		t.Fatalf("3x replication overhead %.3f, want 3", got)
	}
}
