// Package boltstore implements metadata.Metadata on a local BoltDB file.
//
// It is used two ways:
//
//   - directly, as the gateway's metadata backend in single-node mode;
//   - as the state machine store underneath Raft (internal/metadata/raftmeta).
//
// Every mutation is expressed as a Command and applied through Apply, so the
// single-node path and the Raft path execute identical code. Apply is
// deterministic: its outcome depends only on the current state and the
// command (timestamps travel inside the command), which is what Raft
// requires of a state machine.
//
// Bolt layout:
//
//	buckets/            name -> JSON BucketInfo
//	objects/<bucket>/   key  -> JSON ObjectMeta  (one nested bucket per S3 bucket)
//	system/             "applied_index" -> uint64 (last Raft index applied)
package boltstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
)

var (
	bktBuckets = []byte("buckets")
	bktObjects = []byte("objects")
	bktSystem  = []byte("system")
	bktUploads = []byte("uploads") // uploadID -> JSON metadata.UploadInfo
	bktParts   = []byte("parts")   // uploadID/ -> partNumber (4-byte BE) -> JSON metadata.PartMeta

	keyAppliedIndex = []byte("applied_index")
)

// ErrStorage wraps failures of the underlying database (as opposed to
// command results such as metadata.ErrNoSuchBucket). A Raft state machine
// that sees it must stop rather than continue with diverged state.
var ErrStorage = errors.New("boltstore: storage failure")

// MaxListKeys is the largest page ListObjects returns (the S3 limit).
const MaxListKeys = 1000

// Op names a mutation.
type Op string

// Mutations understood by Apply.
const (
	OpCreateBucket Op = "CreateBucket"
	OpDeleteBucket Op = "DeleteBucket"
	OpPutObject    Op = "PutObject"
	OpDeleteObject Op = "DeleteObject"

	OpUpdateShardLocation Op = "UpdateShardLocation"

	OpCreateMultipart   Op = "CreateMultipart"
	OpPutPart           Op = "PutPart"
	OpCompleteMultipart Op = "CompleteMultipart"
	OpAbortMultipart    Op = "AbortMultipart"
)

// Command is one deterministic mutation. It is the payload of a Raft log
// entry, so it must carry everything Apply needs, including timestamps.
type Command struct {
	Op     Op                   `json:"op"`
	Bucket string               `json:"bucket,omitempty"`
	Key    string               `json:"key,omitempty"`
	Time   time.Time            `json:"time,omitzero"`
	Object *metadata.ObjectMeta `json:"object,omitempty"`

	Update *metadata.ShardLocationUpdate `json:"update,omitempty"`

	UploadID string                    `json:"upload_id,omitempty"`
	Upload   *metadata.UploadMeta      `json:"upload,omitempty"`
	Part     *metadata.PartMeta        `json:"part,omitempty"`
	Complete *metadata.CompleteRequest `json:"complete,omitempty"`
}

// Validate checks a command before it is proposed, so malformed commands
// never reach the replicated log.
func (c Command) Validate() error {
	switch c.Op {
	case OpCreateBucket:
		if c.Bucket == "" || !utf8.ValidString(c.Bucket) {
			return fmt.Errorf("%w: invalid bucket name", metadata.ErrInvalidArgument)
		}
	case OpCreateMultipart:
		if c.Bucket == "" || c.Key == "" || !utf8.ValidString(c.Key) || c.UploadID == "" {
			return fmt.Errorf("%w: incomplete multipart upload", metadata.ErrInvalidArgument)
		}
	case OpPutPart:
		if c.UploadID == "" || c.Part == nil || c.Part.PartNumber < 1 || c.Part.PartNumber > metadata.MaxPartNumber {
			return fmt.Errorf("%w: invalid part", metadata.ErrInvalidArgument)
		}
	case OpCompleteMultipart:
		if c.UploadID == "" || c.Complete == nil || len(c.Complete.Parts) == 0 || c.Complete.VersionID == "" {
			return fmt.Errorf("%w: incomplete CompleteMultipart", metadata.ErrInvalidArgument)
		}
	case OpAbortMultipart:
		if c.UploadID == "" {
			return fmt.Errorf("%w: missing upload id", metadata.ErrInvalidArgument)
		}
	case OpUpdateShardLocation:
		if c.Update == nil || c.Update.Bucket == "" || c.Update.Key == "" || c.Update.To == "" {
			return fmt.Errorf("%w: incomplete shard location update", metadata.ErrInvalidArgument)
		}
	case OpDeleteBucket, OpDeleteObject:
		if c.Bucket == "" {
			return fmt.Errorf("%w: missing bucket", metadata.ErrInvalidArgument)
		}
	case OpPutObject:
		if c.Object == nil {
			return fmt.Errorf("%w: missing object", metadata.ErrInvalidArgument)
		}
		if c.Object.Key == "" || !utf8.ValidString(c.Object.Key) {
			return fmt.Errorf("%w: key must be non-empty valid UTF-8", metadata.ErrInvalidArgument)
		}
	default:
		return fmt.Errorf("%w: unknown op %q", metadata.ErrInvalidArgument, c.Op)
	}
	return nil
}

// Store is a BoltDB-backed metadata store.
type Store struct {
	path string
	now  func() time.Time

	// mu guards db. Normal operations hold it for reading; Restore holds it
	// for writing while it swaps the database file.
	mu sync.RWMutex
	db *bolt.DB
}

var _ metadata.Metadata = (*Store)(nil)

// Options tune a Store.
type Options struct {
	// NoSync skips fsync on commit. Only for tests and bulk loading: a
	// crash can lose recent commits.
	NoSync bool
}

// Open opens (creating if needed) the database at path.
func Open(path string) (*Store, error) { return OpenWithOptions(path, Options{}) }

// OpenWithOptions is Open with options.
func OpenWithOptions(path string, o Options) (*Store, error) {
	db, err := openDB(path)
	if err != nil {
		return nil, err
	}
	db.NoSync = o.NoSync
	return &Store{path: path, db: db, now: func() time.Time { return time.Now().UTC() }}, nil
}

func openDB(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("boltstore: open %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bktBuckets, bktObjects, bktSystem, bktUploads, bktParts} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("boltstore: init: %w", err)
	}
	return db, nil
}

// Close closes the database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

func (s *Store) view(fn func(tx *bolt.Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.View(fn)
}

// Apply executes cmd. When index is non-zero it is the Raft log index of
// the command: commands at or below the stored applied index are skipped
// (they were applied before a restart), and the new index is recorded in
// the same transaction as the mutation, so state and index never diverge.
//
// The returned error is the command's result (for example
// metadata.ErrBucketNotEmpty). A command that fails this way changes no
// state but still advances the applied index.
func (s *Store) Apply(index uint64, cmd Command) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result error
	err := s.db.Update(func(tx *bolt.Tx) error {
		if index > 0 {
			sys := tx.Bucket(bktSystem)
			if index <= appliedIndex(sys) {
				return nil
			}
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], index)
			if err := sys.Put(keyAppliedIndex, b[:]); err != nil {
				return err
			}
		}
		var err error
		result, err = applyTx(tx, cmd)
		return err
	})
	if err != nil {
		return fmt.Errorf("%w: apply %s: %w", ErrStorage, cmd.Op, err)
	}
	return result
}

// applyTx performs one command inside tx. It returns (result, nil) for
// outcomes that belong to the command, and a non-nil second error only for
// storage failures, which abort the transaction. Commands check every
// precondition before writing, so a non-nil result means nothing changed.
func applyTx(tx *bolt.Tx, cmd Command) (result, fatal error) {
	if err := cmd.Validate(); err != nil {
		return err, nil
	}
	buckets := tx.Bucket(bktBuckets)
	objects := tx.Bucket(bktObjects)
	switch cmd.Op {
	case OpCreateBucket:
		if buckets.Get([]byte(cmd.Bucket)) != nil {
			return metadata.ErrBucketAlreadyExists, nil
		}
		v, err := json.Marshal(metadata.BucketInfo{Name: cmd.Bucket, CreatedAt: cmd.Time})
		if err != nil {
			return nil, err
		}
		if err := buckets.Put([]byte(cmd.Bucket), v); err != nil {
			return nil, err
		}
		_, err = objects.CreateBucket([]byte(cmd.Bucket))
		return nil, err

	case OpDeleteBucket:
		if buckets.Get([]byte(cmd.Bucket)) == nil {
			return metadata.ErrNoSuchBucket, nil
		}
		if b := objects.Bucket([]byte(cmd.Bucket)); b != nil {
			if k, _ := b.Cursor().First(); k != nil {
				return metadata.ErrBucketNotEmpty, nil
			}
			if err := objects.DeleteBucket([]byte(cmd.Bucket)); err != nil {
				return nil, err
			}
		}
		// In-progress uploads into the bucket go with it; their parts'
		// shards become garbage for GC.
		if err := abortBucketUploads(tx, cmd.Bucket); err != nil {
			return nil, err
		}
		return nil, buckets.Delete([]byte(cmd.Bucket))

	case OpPutObject:
		b := objects.Bucket([]byte(cmd.Object.Bucket))
		if b == nil {
			return metadata.ErrNoSuchBucket, nil
		}
		v, err := json.Marshal(cmd.Object)
		if err != nil {
			return nil, err
		}
		return nil, b.Put([]byte(cmd.Object.Key), v)

	case OpDeleteObject:
		b := objects.Bucket([]byte(cmd.Bucket))
		if b == nil {
			return metadata.ErrNoSuchBucket, nil
		}
		if cmd.Key == "" {
			return nil, nil
		}
		return nil, b.Delete([]byte(cmd.Key))

	case OpUpdateShardLocation:
		return updateShardLocation(objects, cmd.Update)
	case OpCreateMultipart:
		return createMultipart(tx, cmd)
	case OpPutPart:
		return putPart(tx, cmd)
	case OpCompleteMultipart:
		return completeMultipart(tx, cmd)
	case OpAbortMultipart:
		return abortMultipart(tx, cmd.UploadID)
	}
	return fmt.Errorf("%w: unknown op %q", metadata.ErrInvalidArgument, cmd.Op), nil
}

// updateShardLocation moves one shard reference from u.From to u.To. It is
// conditional: if the object was overwritten (different version) or the
// shard no longer lives on u.From, nothing changes and
// ErrShardLocationConflict is returned, so a slow repair or rebalance can
// never undo a newer write or a newer move.
func updateShardLocation(objects *bolt.Bucket, u *metadata.ShardLocationUpdate) (result, fatal error) {
	b := objects.Bucket([]byte(u.Bucket))
	if b == nil {
		return metadata.ErrNoSuchBucket, nil
	}
	v := b.Get([]byte(u.Key))
	if v == nil {
		return metadata.ErrNoSuchKey, nil
	}
	var obj metadata.ObjectMeta
	if err := json.Unmarshal(v, &obj); err != nil {
		return nil, err
	}
	if obj.VersionID != u.VersionID {
		return fmt.Errorf("%w: object version changed", metadata.ErrShardLocationConflict), nil
	}
	if u.ChunkIndex < 0 || u.ChunkIndex >= len(obj.Chunks) {
		return fmt.Errorf("%w: chunk index %d out of range", metadata.ErrInvalidArgument, u.ChunkIndex), nil
	}
	shards := obj.Chunks[u.ChunkIndex].Shards
	if u.ShardIndex < 0 || u.ShardIndex >= len(shards) {
		return fmt.Errorf("%w: shard index %d out of range", metadata.ErrInvalidArgument, u.ShardIndex), nil
	}
	if shards[u.ShardIndex].Node != u.From {
		return fmt.Errorf("%w: shard is on %s, not %s", metadata.ErrShardLocationConflict, shards[u.ShardIndex].Node, u.From), nil
	}
	shards[u.ShardIndex].Node = u.To
	nv, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return nil, b.Put([]byte(u.Key), nv)
}

func appliedIndex(sys *bolt.Bucket) uint64 {
	v := sys.Get(keyAppliedIndex)
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

// AppliedIndex returns the last Raft index recorded by Apply (0 if none).
func (s *Store) AppliedIndex() (uint64, error) {
	var idx uint64
	err := s.view(func(tx *bolt.Tx) error {
		idx = appliedIndex(tx.Bucket(bktSystem))
		return nil
	})
	return idx, err
}

// Snapshot writes a consistent copy of the whole database to w. Writers are
// not blocked while it runs (bolt read transactions are MVCC snapshots).
func (s *Store) Snapshot(w io.Writer) error {
	return s.view(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
}

// Restore replaces the database with a snapshot produced by Snapshot. The
// new file is written and fsynced beside the old one, then renamed over it,
// so a crash leaves either the old or the new database, never a mix.
func (s *Store) Restore(r io.Reader) error {
	tmp := s.path + ".restore"
	if err := writeFileSynced(tmp, r); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// Check the snapshot opens before touching the live database.
	check, err := openDB(tmp)
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("boltstore: restore: invalid snapshot: %w", err)
	}
	if err := check.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("boltstore: restore: close: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		// Reopen the old database so the store stays usable.
		db, oerr := openDB(s.path)
		if oerr == nil {
			s.db = db
		}
		return errors.Join(fmt.Errorf("boltstore: restore: rename: %w", err), oerr)
	}
	db, err := openDB(s.path)
	if err != nil {
		return fmt.Errorf("boltstore: restore: reopen: %w", err)
	}
	s.db = db
	return nil
}

func writeFileSynced(path string, r io.Reader) error {
	f, err := os.OpenFile(filepath.Clean(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("boltstore: restore: create: %w", err)
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return fmt.Errorf("boltstore: restore: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("boltstore: restore: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("boltstore: restore: close: %w", err)
	}
	return nil
}

// --- metadata.Metadata (single-node mode) ---

// CreateBucket implements metadata.Metadata.
func (s *Store) CreateBucket(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Apply(0, Command{Op: OpCreateBucket, Bucket: name, Time: s.now()})
}

// DeleteBucket implements metadata.Metadata.
func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" {
		return metadata.ErrNoSuchBucket
	}
	return s.Apply(0, Command{Op: OpDeleteBucket, Bucket: name})
}

// PutObject implements metadata.Metadata.
func (s *Store) PutObject(ctx context.Context, obj metadata.ObjectMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Apply(0, Command{Op: OpPutObject, Object: &obj})
}

// DeleteObject implements metadata.Metadata. Deleting a missing key succeeds.
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if bucket == "" {
		return metadata.ErrNoSuchBucket
	}
	return s.Apply(0, Command{Op: OpDeleteObject, Bucket: bucket, Key: key})
}

// HeadBucket implements metadata.Metadata.
func (s *Store) HeadBucket(ctx context.Context, name string) (metadata.BucketInfo, error) {
	var info metadata.BucketInfo
	if err := ctx.Err(); err != nil {
		return info, err
	}
	err := s.view(func(tx *bolt.Tx) error {
		v := tx.Bucket(bktBuckets).Get([]byte(name))
		if v == nil {
			return metadata.ErrNoSuchBucket
		}
		return json.Unmarshal(v, &info)
	})
	return info, err
}

// ListBuckets implements metadata.Metadata. Results are sorted by name.
func (s *Store) ListBuckets(ctx context.Context) ([]metadata.BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []metadata.BucketInfo
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(bktBuckets).ForEach(func(_, v []byte) error {
			var info metadata.BucketInfo
			if err := json.Unmarshal(v, &info); err != nil {
				return err
			}
			out = append(out, info)
			return nil
		})
	})
	return out, err
}

// GetObject implements metadata.Metadata.
func (s *Store) GetObject(ctx context.Context, bucket, key string) (metadata.ObjectMeta, error) {
	var obj metadata.ObjectMeta
	if err := ctx.Err(); err != nil {
		return obj, err
	}
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktObjects).Bucket([]byte(bucket))
		if b == nil {
			return metadata.ErrNoSuchBucket
		}
		v := b.Get([]byte(key))
		if v == nil {
			return metadata.ErrNoSuchKey
		}
		return json.Unmarshal(v, &obj)
	})
	return obj, err
}

// ListObjects implements metadata.Metadata (ListObjectsV2 semantics).
func (s *Store) ListObjects(ctx context.Context, bucket string, opts metadata.ListOptions) (metadata.ListResult, error) {
	var res metadata.ListResult
	if err := ctx.Err(); err != nil {
		return res, err
	}
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktObjects).Bucket([]byte(bucket))
		if b == nil {
			return metadata.ErrNoSuchBucket
		}
		var err error
		res, err = list(b.Cursor(), opts)
		return err
	})
	return res, err
}

func list(c *bolt.Cursor, opts metadata.ListOptions) (metadata.ListResult, error) {
	var res metadata.ListResult
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 || maxKeys > MaxListKeys {
		maxKeys = MaxListKeys
	}
	prefix := []byte(opts.Prefix)
	delim := []byte(opts.Delimiter)

	// Work out where to start. A continuation token takes precedence over
	// StartAfter, as in S3.
	seek := prefix
	var skip []byte // skip a key exactly equal to this (exclusive start)
	if opts.StartAfter != "" && opts.StartAfter >= opts.Prefix {
		seek, skip = []byte(opts.StartAfter), []byte(opts.StartAfter)
	}
	if opts.ContinuationToken != "" {
		tok, err := decodeToken(opts.ContinuationToken)
		if err != nil {
			return res, err
		}
		if tok.isPrefix {
			seek, skip = prefixEnd([]byte(tok.value)), nil
			if seek == nil {
				return res, nil // nothing can sort after this prefix
			}
		} else {
			seek, skip = []byte(tok.value), []byte(tok.value)
		}
	}

	var last token
	count := 0
	k, v := c.Seek(seek)
	for k != nil {
		if skip != nil && bytes.Equal(k, skip) {
			k, v = c.Next()
			continue
		}
		if !bytes.HasPrefix(k, prefix) {
			break // keys are sorted, so no later key can match
		}
		if len(delim) > 0 {
			if i := bytes.Index(k[len(prefix):], delim); i >= 0 {
				cp := k[:len(prefix)+i+len(delim)]
				if count == maxKeys {
					res.IsTruncated = true
					break
				}
				res.CommonPrefixes = append(res.CommonPrefixes, string(cp))
				count++
				last = token{isPrefix: true, value: string(cp)}
				next := prefixEnd(cp)
				if next == nil {
					break
				}
				k, v = c.Seek(next)
				continue
			}
		}
		if count == maxKeys {
			res.IsTruncated = true
			break
		}
		var obj metadata.ObjectMeta
		if err := json.Unmarshal(v, &obj); err != nil {
			return res, fmt.Errorf("boltstore: decode %q: %w", k, err)
		}
		res.Objects = append(res.Objects, metadata.ObjectSummary{
			Key: string(k), Size: obj.Size, ETag: obj.ETag, LastModified: obj.LastModified,
		})
		count++
		last = token{value: string(k)}
		k, v = c.Next()
	}
	if res.IsTruncated {
		res.NextContinuationToken = last.encode()
	}
	return res, nil
}

// prefixEnd returns the smallest key greater than every key with prefix p,
// or nil if no such key exists (p is all 0xff bytes).
func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// token is the decoded form of a continuation token: the last key or common
// prefix returned on the previous page.
type token struct {
	isPrefix bool
	value    string
}

func (t token) encode() string {
	kind := "k"
	if t.isPrefix {
		kind = "p"
	}
	return base64.RawURLEncoding.EncodeToString([]byte(kind + ":" + t.value))
}

func decodeToken(s string) (token, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return token{}, fmt.Errorf("%w: continuation token", metadata.ErrInvalidArgument)
	}
	kind, value, ok := strings.Cut(string(b), ":")
	if !ok || value == "" || (kind != "k" && kind != "p") {
		return token{}, fmt.Errorf("%w: continuation token", metadata.ErrInvalidArgument)
	}
	return token{isPrefix: kind == "p", value: value}, nil
}

// UpdateShardLocation implements metadata.Metadata (see updateShardLocation).
func (s *Store) UpdateShardLocation(ctx context.Context, u metadata.ShardLocationUpdate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Apply(0, Command{Op: OpUpdateShardLocation, Update: &u})
}

// WalkChunks implements metadata.Metadata. It runs inside one read
// transaction, so fn sees a consistent snapshot.
func (s *Store) WalkChunks(ctx context.Context, fn func(bucket, key, versionID string, chunkIndex int, c metadata.Chunk) error) error {
	return s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(bktObjects).ForEachBucket(func(bucket []byte) error {
			return tx.Bucket(bktObjects).Bucket(bucket).ForEach(func(k, v []byte) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				var obj metadata.ObjectMeta
				if err := json.Unmarshal(v, &obj); err != nil {
					return err
				}
				for i, c := range obj.Chunks {
					if err := fn(obj.Bucket, obj.Key, obj.VersionID, i, c); err != nil {
						return err
					}
				}
				return nil
			})
		})
	})
}
