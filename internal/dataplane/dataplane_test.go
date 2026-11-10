package dataplane

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/storagetest"
)

var ctx = context.Background()

func newPlane(t *testing.T, chunkSize int64) (*Plane, *disk.Disk, string) {
	t.Helper()
	root := t.TempDir()
	d, err := disk.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return New(d, "local", chunkSize), d, root
}

func write(t *testing.T, p *Plane, data []byte) metadata.ObjectMeta {
	t.Helper()
	res, err := p.Write(ctx, bytes.NewReader(data), -1, "b/k/v")
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	return metadata.ObjectMeta{Size: res.Size, Chunks: res.Chunks}
}

func read(p *Plane, obj metadata.ObjectMeta, off, n int64) ([]byte, error) {
	var buf bytes.Buffer
	err := p.Read(ctx, &buf, obj, off, n)
	return buf.Bytes(), err
}

func TestWriteChunking(t *testing.T) {
	const chunk = 1000
	cases := []struct {
		size, wantChunks int
	}{
		{0, 0}, {1, 1}, {999, 1}, {1000, 1}, {1001, 2}, {2000, 2}, {4321, 5},
	}
	for _, c := range cases {
		p, _, _ := newPlane(t, chunk)
		data := storagetest.RandomBytes(t, c.size)
		res, err := p.Write(ctx, bytes.NewReader(data), -1, "b/k/v")
		if err != nil {
			t.Fatalf("size %d: %v", c.size, err)
		}
		if res.Size != int64(c.size) || len(res.Chunks) != c.wantChunks {
			t.Fatalf("size %d: got size %d, %d chunks; want %d chunks", c.size, res.Size, len(res.Chunks), c.wantChunks)
		}
		if res.MD5 != md5.Sum(data) {
			t.Fatalf("size %d: MD5 mismatch", c.size)
		}
		var off int64
		for i, ch := range res.Chunks {
			if ch.Offset != off || ch.Size <= 0 || ch.Size > chunk {
				t.Fatalf("size %d chunk %d: offset %d size %d", c.size, i, ch.Offset, ch.Size)
			}
			if ch.Scheme != metadata.Single || len(ch.Shards) != 1 || ch.Shards[0].Node != "local" {
				t.Fatalf("size %d chunk %d: bad layout %+v", c.size, i, ch)
			}
			off += ch.Size
		}
	}
}

func TestReadAllRanges(t *testing.T) {
	const chunk = 7
	p, _, _ := newPlane(t, chunk)
	data := storagetest.RandomBytes(t, 30)
	obj := write(t, p, data)
	for off := 0; off <= len(data); off++ {
		for n := 0; off+n <= len(data); n++ {
			got, err := read(p, obj, int64(off), int64(n))
			if err != nil {
				t.Fatalf("Read(%d, %d): %v", off, n, err)
			}
			if !bytes.Equal(got, data[off:off+n]) {
				t.Fatalf("Read(%d, %d) returned wrong bytes", off, n)
			}
		}
	}
}

func TestReadLargeObjectDefaultChunk(t *testing.T) {
	p, _, _ := newPlane(t, 0)
	data := storagetest.RandomBytes(t, 2*DefaultChunkSize+12345)
	obj := write(t, p, data)
	if len(obj.Chunks) != 3 {
		t.Fatalf("got %d chunks, want 3", len(obj.Chunks))
	}
	got, err := read(p, obj, 0, obj.Size)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("full read: %v", err)
	}
	// A range spanning a chunk boundary.
	off := int64(DefaultChunkSize - 10)
	got, err = read(p, obj, off, 20)
	if err != nil || !bytes.Equal(got, data[off:off+20]) {
		t.Fatalf("boundary read: %v", err)
	}
}

func TestReadOutOfBounds(t *testing.T) {
	p, _, _ := newPlane(t, 10)
	obj := write(t, p, []byte("hello"))
	for _, r := range [][2]int64{{-1, 1}, {0, 6}, {5, 1}, {0, -1}} {
		if _, err := read(p, obj, r[0], r[1]); err == nil {
			t.Fatalf("Read(%d, %d) succeeded", r[0], r[1])
		}
	}
}

func TestIdenticalChunksDeduplicated(t *testing.T) {
	p, d, _ := newPlane(t, 100)
	block := storagetest.RandomBytes(t, 100)
	data := bytes.Repeat(block, 5)
	obj := write(t, p, data)
	if len(obj.Chunks) != 5 {
		t.Fatalf("got %d chunks", len(obj.Chunks))
	}
	n := 0
	_ = d.Walk(ctx, func(storage.ShardInfo) error { n++; return nil })
	if n != 1 {
		t.Fatalf("%d shards on disk, want 1 (content addressing)", n)
	}
	got, err := read(p, obj, 0, obj.Size)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read: %v", err)
	}
}

func shardPath(root string, id storage.ShardID) string {
	h := id.String()
	return filepath.Join(root, "shards", h[0:2], h[2:4], h)
}

func TestCorruptShardFailsBeforeWriting(t *testing.T) {
	p, _, root := newPlane(t, 10)
	data := storagetest.RandomBytes(t, 25)
	obj := write(t, p, data)

	// Corrupt the second chunk.
	path := shardPath(root, obj.Chunks[1].Shards[0].ID)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := read(p, obj, 0, obj.Size)
	if !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	// Only the first, intact chunk may have been written.
	if !bytes.Equal(got, data[:10]) {
		t.Fatalf("wrote %d bytes before failing, want exactly the first chunk", len(got))
	}
	// A range entirely inside the intact chunk still works.
	if got, err := read(p, obj, 2, 5); err != nil || !bytes.Equal(got, data[2:7]) {
		t.Fatalf("read of intact chunk: %v", err)
	}
}

func TestTruncatedShardDetected(t *testing.T) {
	p, _, root := newPlane(t, 10)
	obj := write(t, p, storagetest.RandomBytes(t, 10))
	path := shardPath(root, obj.Chunks[0].Shards[0].ID)
	if err := os.Truncate(path, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := read(p, obj, 0, 1); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestMissingShard(t *testing.T) {
	p, d, _ := newPlane(t, 10)
	obj := write(t, p, []byte("hello"))
	if err := d.Delete(ctx, obj.Chunks[0].Shards[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := read(p, obj, 0, 5); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestWriteSizeLimit(t *testing.T) {
	p, _, _ := newPlane(t, 10)
	if _, err := p.Write(ctx, bytes.NewReader(make([]byte, 25)), 25, "b/k/v"); err != nil {
		t.Fatalf("at limit: %v", err)
	}
	for _, size := range []int{26, 35} {
		if _, err := p.Write(ctx, bytes.NewReader(make([]byte, size)), 25, "b/k/v"); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("size %d over limit 25: err = %v, want ErrTooLarge", size, err)
		}
	}
}

type failAfter struct {
	r io.Reader
	n int
}

func (f *failAfter) Read(b []byte) (int, error) {
	if f.n <= 0 {
		return 0, errors.New("connection reset")
	}
	if len(b) > f.n {
		b = b[:f.n]
	}
	n, err := f.r.Read(b)
	f.n -= n
	return n, err
}

func TestWriteBodyError(t *testing.T) {
	p, _, _ := newPlane(t, 10)
	_, err := p.Write(ctx, &failAfter{r: bytes.NewReader(make([]byte, 100)), n: 15}, -1, "b/k/v")
	if err == nil {
		t.Fatal("Write with failing body succeeded")
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("client gone") }

func TestReadWriterError(t *testing.T) {
	p, _, _ := newPlane(t, 10)
	obj := write(t, p, []byte("hello"))
	if err := p.Read(ctx, errWriter{}, obj, 0, 5); err == nil {
		t.Fatal("Read into failing writer succeeded")
	}
}
