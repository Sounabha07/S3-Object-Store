// Package disk implements storage.Storage on a local filesystem.
//
// Layout under the root directory:
//
//	shards/ab/cd/abcd...ef   one file per shard, named by its SHA-256 hex
//	tmp/                     in-progress writes; cleared on startup
//
// A Put writes to tmp/, fsyncs, then hard-links into place and fsyncs the
// directory, so a shard is either fully present or absent after a crash.
package disk

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// Disk is a filesystem-backed shard store.
type Disk struct {
	root      string
	shardsDir string
	tmpDir    string
}

var _ storage.Storage = (*Disk)(nil)

// Open prepares root for use, creating directories and removing any
// leftover temporary files from a previous crash.
func Open(root string) (*Disk, error) {
	d := &Disk{
		root:      root,
		shardsDir: filepath.Join(root, "shards"),
		tmpDir:    filepath.Join(root, "tmp"),
	}
	if err := os.RemoveAll(d.tmpDir); err != nil {
		return nil, fmt.Errorf("disk: clear tmp: %w", err)
	}
	for _, dir := range []string{d.shardsDir, d.tmpDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("disk: %w", err)
		}
	}
	return d, nil
}

func (d *Disk) path(id storage.ShardID) string {
	h := id.String()
	return filepath.Join(d.shardsDir, h[0:2], h[2:4], h)
}

// Put implements storage.Storage.
func (d *Disk) Put(ctx context.Context, r io.Reader, expected storage.ShardID) (storage.ShardInfo, error) {
	tmp, err := os.CreateTemp(d.tmpDir, "put-*")
	if err != nil {
		return storage.ShardInfo{}, fmt.Errorf("disk: create temp: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		// After a successful link the shard lives under its final name, so
		// the temp name is always removed.
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), ctxReader{ctx, r})
	if err != nil {
		return storage.ShardInfo{}, fmt.Errorf("disk: write: %w", err)
	}
	var id storage.ShardID
	h.Sum(id[:0])
	if !expected.IsZero() && id != expected {
		return storage.ShardInfo{}, fmt.Errorf("%w: got %s want %s", storage.ErrCorrupt, id, expected)
	}

	final := d.path(id)
	if info, err := d.statPath(id, final); err == nil {
		return info, nil // already stored; content addressing makes this idempotent
	}

	if err := tmp.Sync(); err != nil {
		return storage.ShardInfo{}, fmt.Errorf("disk: fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return storage.ShardInfo{}, fmt.Errorf("disk: close: %w", err)
	}
	if err := d.mkdirSynced(filepath.Dir(final)); err != nil {
		return storage.ShardInfo{}, err
	}
	// Publish with a hard link rather than a rename: a link never replaces an
	// existing file, so a shard that readers may have open is never swapped
	// underneath them (on Windows that makes concurrent opens fail with a
	// sharing violation). If a concurrent Put of identical content got there
	// first, the existing file is already the correct shard.
	if err := os.Link(tmpName, final); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return d.statPath(id, final)
		}
		return storage.ShardInfo{}, fmt.Errorf("disk: link: %w", err)
	}
	if err := syncDir(filepath.Dir(final)); err != nil {
		return storage.ShardInfo{}, err
	}
	fi, err := os.Stat(final)
	if err != nil {
		return storage.ShardInfo{}, fmt.Errorf("disk: stat after put: %w", err)
	}
	return storage.ShardInfo{ID: id, Size: n, ModTime: fi.ModTime()}, nil
}

// mkdirSynced creates dir (and parents up to shardsDir), fsyncing each parent
// whose entries changed so the new directory survives a crash.
func (d *Disk) mkdirSynced(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	parent := filepath.Dir(dir)
	if parent != d.shardsDir {
		if err := d.mkdirSynced(parent); err != nil {
			return err
		}
	}
	if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("disk: mkdir: %w", err)
	}
	return syncDir(parent)
}

// syncDir fsyncs a directory so that renames and creations in it are durable.
// Windows does not support fsync on directories; NTFS journals metadata.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("disk: open dir: %w", err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("disk: fsync dir: %w", err)
	}
	return nil
}

// Get implements storage.Storage. The returned reader verifies the SHA-256
// and returns storage.ErrCorrupt instead of io.EOF on mismatch.
func (d *Disk) Get(ctx context.Context, id storage.ShardID) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(d.path(id))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("disk: open: %w", err)
	}
	return &verifyingReader{f: f, h: sha256.New(), want: id}, nil
}

// Stat implements storage.Storage.
func (d *Disk) Stat(ctx context.Context, id storage.ShardID) (storage.ShardInfo, error) {
	if err := ctx.Err(); err != nil {
		return storage.ShardInfo{}, err
	}
	return d.statPath(id, d.path(id))
}

func (d *Disk) statPath(id storage.ShardID, p string) (storage.ShardInfo, error) {
	fi, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return storage.ShardInfo{}, storage.ErrNotFound
		}
		return storage.ShardInfo{}, fmt.Errorf("disk: stat: %w", err)
	}
	return storage.ShardInfo{ID: id, Size: fi.Size(), ModTime: fi.ModTime()}, nil
}

// Delete implements storage.Storage.
func (d *Disk) Delete(ctx context.Context, id storage.ShardID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(d.path(id)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return storage.ErrNotFound
		}
		return fmt.Errorf("disk: delete: %w", err)
	}
	return nil
}

// Walk implements storage.Storage. Files whose names are not valid shard IDs
// are skipped.
func (d *Disk) Walk(ctx context.Context, fn func(storage.ShardInfo) error) error {
	return filepath.WalkDir(d.shardsDir, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}
		id, perr := storage.ParseShardID(e.Name())
		if perr != nil {
			return nil
		}
		fi, err := e.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // deleted during the walk
			}
			return err
		}
		return fn(storage.ShardInfo{ID: id, Size: fi.Size(), ModTime: fi.ModTime()})
	})
}

// Health implements storage.Storage by creating and removing a temp file.
func (d *Disk) Health(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.CreateTemp(d.tmpDir, "health-*")
	if err != nil {
		return fmt.Errorf("disk: health: %w", err)
	}
	name := f.Name()
	_ = f.Close()
	return os.Remove(name)
}

// ctxReader stops a copy when the context is cancelled.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

type verifyingReader struct {
	f    *os.File
	h    hash.Hash
	want storage.ShardID
	err  error
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	if v.err != nil {
		return 0, v.err
	}
	n, err := v.f.Read(p)
	v.h.Write(p[:n])
	if errors.Is(err, io.EOF) {
		var got storage.ShardID
		v.h.Sum(got[:0])
		if got != v.want {
			v.err = fmt.Errorf("%w: %s", storage.ErrCorrupt, v.want)
			return n, v.err
		}
		v.err = io.EOF
	} else if err != nil {
		v.err = err
	}
	return n, err
}

func (v *verifyingReader) Close() error { return v.f.Close() }
