package gc

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
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
	t     *testing.T
	meta  *boltstore.Store
	plane *dataplane.Plane
	disks map[placement.NodeID]*disk.Disk
	dirs  map[placement.NodeID]string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, disks: map[placement.NodeID]*disk.Disk{}, dirs: map[placement.NodeID]string{}}
	stores := map[placement.NodeID]storage.Storage{}
	for i := 1; i <= 6; i++ {
		id := placement.NodeID(fmt.Sprintf("storage-%d", i))
		dir := t.TempDir()
		d, err := disk.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = d.Close() })
		e.disks[id], e.dirs[id], stores[id] = d, dir, d
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

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// write stores data without committing metadata (an "in-flight" upload).
func (e *env) write(key string, data []byte) dataplane.WriteResult {
	e.t.Helper()
	res, err := e.plane.Write(ctx, bytes.NewReader(data), int64(len(data)), dataplane.ObjectKey("b", key, "v-"+key))
	if err != nil {
		e.t.Fatal(err)
	}
	return res
}

func (e *env) put(key string, data []byte) {
	e.t.Helper()
	res := e.write(key, data)
	err := e.meta.PutObject(ctx, metadata.ObjectMeta{Bucket: "b", Key: key, VersionID: "v-" + key + fmt.Sprint(time.Now().UnixNano()),
		Size: res.Size, LastModified: time.Now(), Chunks: res.Chunks})
	if err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) shardCount() int {
	n := 0
	for _, d := range e.disks {
		_ = d.Walk(ctx, func(storage.ShardInfo) error { n++; return nil })
	}
	return n
}

func (e *env) referenced() int {
	n := 0
	_ = e.meta.WalkChunks(ctx, func(_, _, _ string, _ int, c metadata.Chunk) error {
		for _, s := range c.Shards {
			if s.Node != "" {
				n++
			}
		}
		return nil
	})
	return n
}

func (e *env) read(key string) []byte {
	e.t.Helper()
	obj, err := e.meta.GetObject(ctx, "b", key)
	if err != nil {
		e.t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := e.plane.Read(ctx, &buf, obj, 0, obj.Size); err != nil {
		e.t.Fatalf("read %s: %v", key, err)
	}
	return buf.Bytes()
}

// collector returns a GC whose clock is `ahead` in the future, so shards
// written now look `ahead` old.
func (e *env) collector(ahead time.Duration) *Collector {
	return &Collector{Meta: e.meta, Plane: e.plane, Grace: time.Hour, Now: func() time.Time { return time.Now().Add(ahead) }}
}

func TestOrphansCollectedOnlyAfterGrace(t *testing.T) {
	e := newEnv(t)
	keep := randomBytes(100_000)
	e.put("keep", keep)
	e.write("failed-upload", randomBytes(100_000)) // shards, but no metadata
	before := e.shardCount()

	st, err := e.collector(0).Run(ctx)
	if err != nil || st.Deleted != 0 || st.SkippedYoung == 0 {
		t.Fatalf("within grace: %+v, %v", st, err)
	}
	st, err = e.collector(2 * time.Hour).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Deleted == 0 || e.shardCount() != e.referenced() || e.shardCount() >= before {
		t.Fatalf("after grace: %+v, %d shards on disk, %d referenced", st, e.shardCount(), e.referenced())
	}
	if !bytes.Equal(e.read("keep"), keep) {
		t.Fatal("referenced object damaged by GC")
	}
}

func TestOverwrittenAndDeletedObjectsCollected(t *testing.T) {
	e := newEnv(t)
	e.put("k", randomBytes(100_000))
	newest := randomBytes(100_000)
	e.put("k", newest) // the first version's shards are now unreferenced
	e.put("gone", randomBytes(10_000))
	if err := e.meta.DeleteObject(ctx, "b", "gone"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.collector(2 * time.Hour).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := e.shardCount(), e.referenced(); got != want {
		t.Fatalf("%d shards on disk, %d referenced", got, want)
	}
	if !bytes.Equal(e.read("k"), newest) {
		t.Fatal("current version damaged")
	}
}

// TestSharedShardKeptWhileReferenced: identical content is stored once; it
// must survive until no object references it.
func TestSharedShardKeptWhileReferenced(t *testing.T) {
	e := newEnv(t)
	data := randomBytes(10_000) // replicated: identical shard IDs
	e.put("a", data)
	e.put("b", data)
	if err := e.meta.DeleteObject(ctx, "b", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.collector(2 * time.Hour).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e.read("b"), data) {
		t.Fatal("shared shard deleted while still referenced")
	}
}

func TestInProgressMultipartPartsProtected(t *testing.T) {
	e := newEnv(t)
	id, err := e.meta.CreateMultipart(ctx, "b", "big", metadata.UploadMeta{})
	if err != nil {
		t.Fatal(err)
	}
	data := randomBytes(100_000)
	res := e.write("part-1", data)
	sum := [16]byte(res.MD5)
	if err := e.meta.PutPart(ctx, id, metadata.PartMeta{PartNumber: 1, Size: res.Size, ETag: fmt.Sprintf(`"%x"`, sum), Chunks: res.Chunks}); err != nil {
		t.Fatal(err)
	}
	if st, err := e.collector(2 * time.Hour).Run(ctx); err != nil || st.Deleted != 0 {
		t.Fatalf("in-progress part collected: %+v, %v", st, err)
	}
	if _, err := e.meta.CompleteMultipart(ctx, id, metadata.CompleteRequest{
		Parts: []metadata.CompletedPart{{PartNumber: 1, ETag: fmt.Sprintf(`"%x"`, sum)}}, VersionID: "v",
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e.read("big"), data) {
		t.Fatal("completed upload lost its data")
	}
}

// TestReusedOrphanNotDeleted: an old orphan shard is reused (deduplicated)
// by a new upload that has not committed yet. The reuse refreshes the
// shard, so GC must not delete it, and the upload commits intact.
func TestReusedOrphanNotDeleted(t *testing.T) {
	e := newEnv(t)
	data := randomBytes(10_000)
	res := e.write("abandoned", data) // orphan
	// Age the orphan's files well beyond the grace period.
	old := time.Now().Add(-3 * time.Hour)
	for _, s := range res.Chunks[0].Shards {
		h := s.ID.String()
		p := filepath.Join(e.dirs[s.Node], "shards", h[0:2], h[2:4], h)
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}
	// A new upload of the same bytes, placed on the same nodes (same
	// placement key), reuses those shards...
	res2 := e.write("abandoned", data)
	for i := range res2.Chunks[0].Shards {
		if res2.Chunks[0].Shards[i] != res.Chunks[0].Shards[i] {
			t.Fatal("setup: second write was not placed on the same nodes")
		}
	}
	// ...GC runs before the new upload commits...
	if st, err := e.collector(0).Run(ctx); err != nil || st.Deleted != 0 {
		t.Fatalf("reused shard deleted: %+v, %v", st, err)
	}
	// ...and the commit finds its data.
	if err := e.meta.PutObject(ctx, metadata.ObjectMeta{Bucket: "b", Key: "new", VersionID: "v2", Size: res2.Size, Chunks: res2.Chunks}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e.read("new"), data) {
		t.Fatal("upload that reused an orphan shard lost its data")
	}
}

func TestDownNodeSkipped(t *testing.T) {
	e := newEnv(t)
	e.write("orphan", randomBytes(100_000))
	e.plane.Health().MarkDown("storage-1")
	st, err := e.collector(2 * time.Hour).Run(ctx)
	if err != nil || st.NodesSkipped != 1 || st.NodesSwept != 5 {
		t.Fatalf("stats %+v, %v", st, err)
	}
}
