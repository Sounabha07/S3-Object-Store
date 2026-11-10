package disk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Sounabha07/S3-Object-Store/internal/storage"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/storagetest"
)

func openDisk(t *testing.T, dir string) *Disk {
	t.Helper()
	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func newDisk(t *testing.T) *Disk { return openDisk(t, t.TempDir()) }

// abs returns the absolute on-disk path of a shard, for tests that tamper
// with files directly.
func (d *Disk) abs(id storage.ShardID) string {
	return filepath.Join(d.dir, filepath.FromSlash(shardPath(id)))
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

			p := d.abs(info.ID)
			onDisk, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, mutate(onDisk), 0o600); err != nil {
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
	if err := os.WriteFile(d.abs(info.ID), []byte("hello WORLD"), 0o600); err != nil {
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
	d, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	_ = d.Close()
	leftover := filepath.Join(root, "tmp", "put-crashed")
	if err := os.WriteFile(leftover, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	openDisk(t, root)
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
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := storagetest.ReadAll(openDisk(t, root), info.ID)
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
	entries, err := os.ReadDir(filepath.Join(d.dir, tmpDir))
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
	if err := os.WriteFile(filepath.Join(d.dir, shardsDir, "README"), []byte("x"), 0o600); err != nil {
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

// TestPermissions guards gosec G301/G302: shard data must not be readable or
// writable by other local users. The previous code created directories 0755.
func TestPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not meaningful on Windows")
	}
	dir := filepath.Join(t.TempDir(), "fresh") // created by Open itself
	d := openDisk(t, dir)
	info := storagetest.MustPut(t, d, []byte("secret object data"))

	shard := d.abs(info.ID)
	paths := []string{dir, filepath.Join(dir, shardsDir), filepath.Join(dir, tmpDir),
		filepath.Dir(filepath.Dir(shard)), filepath.Dir(shard)}
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if perm := fi.Mode().Perm(); perm&0o027 != 0 {
			t.Errorf("directory %s has mode %o; group write or any other-user access is not allowed", p, perm)
		}
	}
	fi, err := os.Stat(shard)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		t.Errorf("shard file has mode %o, want no group/other access", perm)
	}
}

// TestSymlinkOutsideRootRefused guards gosec G304: a symlink planted in the
// data directory must not let the store read a file outside it, even when
// the target's contents hash correctly. Plain os.Open (the previous code)
// follows the link.
func TestSymlinkOutsideRootRefused(t *testing.T) {
	d := newDisk(t)
	data := []byte("shard contents")
	info := storagetest.MustPut(t, d, data)

	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, data, 0o600); err != nil {
		t.Fatal(err)
	}
	shard := d.abs(info.ID)
	if err := os.Remove(shard); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, shard); err != nil {
		t.Skipf("cannot create symlinks here: %v", err)
	}

	if got, err := storagetest.ReadAll(d, info.ID); err == nil {
		t.Fatalf("read %q through a symlink escaping the data directory", got)
	}
	if _, err := d.Stat(context.Background(), info.ID); err == nil {
		t.Fatal("Stat followed a symlink escaping the data directory")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("client went away") }
