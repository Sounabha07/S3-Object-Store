package disk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Sounabha07/S3-Object-Store/internal/storage"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/storagetest"
)

func newDisk(t *testing.T) *Disk {
	t.Helper()
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage { return newDisk(t) })
}

func TestCorruptedShardDetected(t *testing.T) {
	cases := map[string]func(b []byte) []byte{
		"bit flip":  func(b []byte) []byte { b[len(b)/2] ^= 0x01; return b },
		"truncated": func(b []byte) []byte { return b[:len(b)-1] },
		"extended":  func(b []byte) []byte { return append(b, 0) },
		"emptied":   func(b []byte) []byte { return nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			d := newDisk(t)
			data := storagetest.RandomBytes(t, 64<<10)
			info := storagetest.MustPut(t, d, data)

			p := d.path(info.ID)
			onDisk, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, mutate(onDisk), 0o644); err != nil {
				t.Fatal(err)
			}

			_, err = storagetest.ReadAll(d, info.ID)
			if !errors.Is(err, storage.ErrCorrupt) {
				t.Fatalf("read of corrupted shard: err = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestVerifyingReaderStickyError(t *testing.T) {
	d := newDisk(t)
	info := storagetest.MustPut(t, d, []byte("hello world"))
	if err := os.WriteFile(d.path(info.ID), []byte("hello WORLD"), 0o644); err != nil {
		t.Fatal(err)
	}
	rc, err := d.Get(context.Background(), info.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if _, err := io.ReadAll(rc); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("first read: %v", err)
	}
	if _, err := rc.Read(make([]byte, 1)); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("read after corruption: %v, want sticky ErrCorrupt", err)
	}
}

func TestOpenClearsTmp(t *testing.T) {
	root := t.TempDir()
	if _, err := Open(root); err != nil {
		t.Fatal(err)
	}
	leftover := filepath.Join(root, "tmp", "put-crashed")
	if err := os.WriteFile(leftover, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(leftover); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("leftover temp file survived reopen: %v", err)
	}
}

func TestShardsSurviveReopen(t *testing.T) {
	root := t.TempDir()
	d, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	data := storagetest.RandomBytes(t, 4096)
	info := storagetest.MustPut(t, d, data)

	d2, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := storagetest.ReadAll(d2, info.ID)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read after reopen: %v", err)
	}
}

func TestFailedPutLeavesNoTempFiles(t *testing.T) {
	d := newDisk(t)
	failing := io.MultiReader(bytes.NewReader([]byte("partial")), errReader{})
	if _, err := d.Put(context.Background(), failing, storage.ZeroShardID); err == nil {
		t.Fatal("Put with failing reader succeeded")
	}
	entries, err := os.ReadDir(d.tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("tmp dir has %d entries after failed put", len(entries))
	}
}

func TestWalkSkipsForeignFiles(t *testing.T) {
	d := newDisk(t)
	storagetest.MustPut(t, d, []byte("a"))
	if err := os.WriteFile(filepath.Join(d.shardsDir, "README"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	n := 0
	if err := d.Walk(context.Background(), func(storage.ShardInfo) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("Walk visited %d shards, want 1", n)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("client went away") }
