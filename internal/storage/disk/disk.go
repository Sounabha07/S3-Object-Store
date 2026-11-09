// Package disk implements storage.Storage on a local filesystem.
//
// Layout under the root directory:
//
//	shards/ab/cd/abcd...ef   one file per shard, named by its SHA-256 hex
//	tmp/                     in-progress writes; cleared on startup
//
// A Put writes to tmp/, fsyncs, then hard-links into place and fsyncs the
// directory, so a shard is either fully present or absent after a crash.
//
// All file access goes through an os.Root opened on the data directory, so
// no path (including one reached through a symlink planted inside the
// directory) can resolve outside it. Directories are created 0750 and files
// 0600: stored object data is never readable by other local users.
package disk

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"runtime"

	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

const (
	dirPerm  = 0o750
	filePerm = 0o600

	shardsDir = "shards"
	tmpDir    = "tmp"
)

// Disk is a filesystem-backed shard store.
type Disk struct {
	dir  string // absolute path of the root, for messages and tests
	root *os.Root
}

var _ storage.Storage = (*Disk)(nil)

// Open prepares dir for use, creating directories and removing any leftover
// temporary files from a previous crash. Call Close when done.
func Open(dir string) (*Disk, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("disk: create %s: %w", dir, err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("disk: open root %s: %w", dir, err)
	}
	d := &Disk{dir: dir, root: root}
	if err := root.RemoveAll(tmpDir); err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("disk: clear tmp: %w", err)
	}
	for _, sub := range []string{shardsDir, tmpDir} {
		if err := root.MkdirAll(sub, dirPerm); err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("disk: %w", err)
		}
	}
	return d, nil
}

// Close releases the root directory handle.
func (d *Disk) Close() error { return d.root.Close() }

// shardPath returns the shard's path relative to the root. Its only input is
// a ShardID, so it is always "shards/<2 hex>/<2 hex>/<64 hex>".
func shardPath(id storage.ShardID) string {
	h := id.String()
	return path.Join(shardsDir, h[0:2], h[2:4], h)
}

// Put implements storage.Storage.
func (d *Disk) Put(ctx context.Context, r io.Reader, expected storage.ShardID) (storage.ShardInfo, error) {
	tmpName, tmp, err := d.createTemp("put-")
	if err != nil {
		return storage.ShardInfo{}, err
	}
	defer func() {
		// After a successful link the shard lives under its final name, so
		// the temp name is always removed. Close errors on this path are
		// irrelevant: the explicit Close below is the one that is checked.
		_ = tmp.Close()
		_ = d.root.Remove(tmpName)
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

	final := shardPath(id)
	if info, err := d.stat(id); err == nil {
		return info, nil // already stored; content addressing makes this idempotent
	}

	if err := tmp.Sync(); err != nil {
		return storage.ShardInfo{}, fmt.Errorf("disk: fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return storage.ShardInfo{}, fmt.Errorf("disk: close: %w", err)
	}
	if err := d.mkdirSynced(path.Dir(final)); err != nil {
		return storage.ShardInfo{}, err
	}
	// Publish with a hard link rather than a rename: a link never replaces an
	// existing file, so a shard that readers may have open is never swapped
	// underneath them (on Windows that makes concurrent opens fail with a
	// sharing violation). If a concurrent Put of identical content got there
	// first, the existing file is already the correct shard.
	if err := d.root.Link(tmpName, final); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return d.stat(id)
		}
		return storage.ShardInfo{}, fmt.Errorf("disk: link: %w", err)
	}
	if err := d.syncDir(path.Dir(final)); err != nil {
		return storage.ShardInfo{}, err
	}
	fi, err := d.root.Stat(final)
	if err != nil {
		return storage.ShardInfo{}, fmt.Errorf("disk: stat after put: %w", err)
	}
	return storage.ShardInfo{ID: id, Size: n, ModTime: fi.ModTime()}, nil
}

// createTemp creates a new, exclusive 0600 file under tmp/.
func (d *Disk) createTemp(prefix string) (string, *os.File, error) {
	for range 10 {
		var b [12]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", nil, fmt.Errorf("disk: temp name: %w", err)
		}
		name := path.Join(tmpDir, prefix+hex.EncodeToString(b[:]))
		f, err := d.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, filePerm)
		if err == nil {
			return name, f, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return "", nil, fmt.Errorf("disk: create temp: %w", err)
		}
	}
	return "", nil, errors.New("disk: could not create a unique temp file")
}

// mkdirSynced creates dir (and its parents below shards/), fsyncing each
// parent whose entries changed so the new directory survives a crash.
func (d *Disk) mkdirSynced(dir string) error {
	if _, err := d.root.Stat(dir); err == nil {
		return nil
	}
	parent := path.Dir(dir)
	if parent != shardsDir {
		if err := d.mkdirSynced(parent); err != nil {
			return err
		}
	}
	if err := d.root.Mkdir(dir, dirPerm); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("disk: mkdir: %w", err)
	}
	return d.syncDir(parent)
}

// syncDir fsyncs a directory so that links and creations in it are durable.
// Windows does not support fsync on directories; NTFS journals metadata.
func (d *Disk) syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := d.root.Open(dir)
	if err != nil {
		return fmt.Errorf("disk: open dir: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("disk: fsync dir: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("disk: close dir: %w", err)
	}
	return nil
}

// Get implements storage.Storage. The returned reader verifies the SHA-256
// and returns storage.ErrCorrupt instead of io.EOF on mismatch.
func (d *Disk) Get(ctx context.Context, id storage.ShardID) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := d.root.Open(shardPath(id))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("disk: open: %w", err)
	}
	return storage.NewVerifyingReader(f, id), nil
}

// Stat implements storage.Storage.
func (d *Disk) Stat(ctx context.Context, id storage.ShardID) (storage.ShardInfo, error) {
	if err := ctx.Err(); err != nil {
		return storage.ShardInfo{}, err
	}
	return d.stat(id)
}

func (d *Disk) stat(id storage.ShardID) (storage.ShardInfo, error) {
	fi, err := d.root.Stat(shardPath(id))
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
	if err := d.root.Remove(shardPath(id)); err != nil {
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
	return fs.WalkDir(d.root.FS(), shardsDir, func(p string, e fs.DirEntry, err error) error {
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
	name, f, err := d.createTemp("health-")
	if err != nil {
		return fmt.Errorf("disk: health: %w", err)
	}
	cerr := f.Close()
	return errors.Join(cerr, d.root.Remove(name))
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
