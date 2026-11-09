// Package boltstore implements metadata.Metadata on a local BoltDB file.
//
// In M1 it is the gateway's metadata backend directly; in M2 it becomes the
// state machine store underneath Raft.
//
// Bolt layout:
//
//	buckets/            name -> JSON BucketInfo
//	objects/<bucket>/   key  -> JSON ObjectMeta  (one nested bucket per S3 bucket)
package boltstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
)

var (
	bktBuckets = []byte("buckets")
	bktObjects = []byte("objects")
)

// MaxListKeys is the largest page ListObjects returns (the S3 limit).
const MaxListKeys = 1000

// Store is a BoltDB-backed metadata store.
type Store struct {
	db  *bolt.DB
	now func() time.Time
}

var _ metadata.Metadata = (*Store)(nil)

// Open opens (creating if needed) the database at path.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("boltstore: open %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bktBuckets, bktObjects} {
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
	return &Store{db: db, now: func() time.Time { return time.Now().UTC() }}, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// CreateBucket implements metadata.Metadata.
func (s *Store) CreateBucket(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" || !utf8.ValidString(name) {
		return fmt.Errorf("%w: invalid bucket name", metadata.ErrInvalidArgument)
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		buckets := tx.Bucket(bktBuckets)
		if buckets.Get([]byte(name)) != nil {
			return metadata.ErrBucketAlreadyExists
		}
		v, err := json.Marshal(metadata.BucketInfo{Name: name, CreatedAt: s.now()})
		if err != nil {
			return err
		}
		if err := buckets.Put([]byte(name), v); err != nil {
			return err
		}
		_, err = tx.Bucket(bktObjects).CreateBucket([]byte(name))
		return err
	})
}

// DeleteBucket implements metadata.Metadata.
func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		buckets := tx.Bucket(bktBuckets)
		if buckets.Get([]byte(name)) == nil {
			return metadata.ErrNoSuchBucket
		}
		objs := tx.Bucket(bktObjects)
		if b := objs.Bucket([]byte(name)); b != nil {
			if k, _ := b.Cursor().First(); k != nil {
				return metadata.ErrBucketNotEmpty
			}
			if err := objs.DeleteBucket([]byte(name)); err != nil {
				return err
			}
		}
		return buckets.Delete([]byte(name))
	})
}

// HeadBucket implements metadata.Metadata.
func (s *Store) HeadBucket(ctx context.Context, name string) (metadata.BucketInfo, error) {
	var info metadata.BucketInfo
	if err := ctx.Err(); err != nil {
		return info, err
	}
	err := s.db.View(func(tx *bolt.Tx) error {
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
	err := s.db.View(func(tx *bolt.Tx) error {
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

// PutObject implements metadata.Metadata.
func (s *Store) PutObject(ctx context.Context, obj metadata.ObjectMeta) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if obj.Key == "" || !utf8.ValidString(obj.Key) {
		return fmt.Errorf("%w: key must be non-empty valid UTF-8", metadata.ErrInvalidArgument)
	}
	v, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktObjects).Bucket([]byte(obj.Bucket))
		if b == nil {
			return metadata.ErrNoSuchBucket
		}
		return b.Put([]byte(obj.Key), v)
	})
}

// GetObject implements metadata.Metadata.
func (s *Store) GetObject(ctx context.Context, bucket, key string) (metadata.ObjectMeta, error) {
	var obj metadata.ObjectMeta
	if err := ctx.Err(); err != nil {
		return obj, err
	}
	err := s.db.View(func(tx *bolt.Tx) error {
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

// DeleteObject implements metadata.Metadata. Deleting a missing key succeeds.
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktObjects).Bucket([]byte(bucket))
		if b == nil {
			return metadata.ErrNoSuchBucket
		}
		if key == "" {
			return nil
		}
		return b.Delete([]byte(key))
	})
}

// ListObjects implements metadata.Metadata (ListObjectsV2 semantics).
func (s *Store) ListObjects(ctx context.Context, bucket string, opts metadata.ListOptions) (metadata.ListResult, error) {
	var res metadata.ListResult
	if err := ctx.Err(); err != nil {
		return res, err
	}
	err := s.db.View(func(tx *bolt.Tx) error {
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

// CreateMultipart is not implemented until M5.
func (s *Store) CreateMultipart(context.Context, string, string, metadata.UploadMeta) (string, error) {
	return "", metadata.ErrNotImplemented
}

// PutPart is not implemented until M5.
func (s *Store) PutPart(context.Context, string, metadata.PartMeta) error {
	return metadata.ErrNotImplemented
}

// ListParts is not implemented until M5.
func (s *Store) ListParts(context.Context, string, metadata.ListPartsOptions) (metadata.ListPartsResult, error) {
	return metadata.ListPartsResult{}, metadata.ErrNotImplemented
}

// CompleteMultipart is not implemented until M5.
func (s *Store) CompleteMultipart(context.Context, string, []metadata.CompletedPart) (metadata.ObjectMeta, error) {
	return metadata.ObjectMeta{}, metadata.ErrNotImplemented
}

// AbortMultipart is not implemented until M5.
func (s *Store) AbortMultipart(context.Context, string) error {
	return metadata.ErrNotImplemented
}

// UpdateShardLocation is not implemented until M3.
func (s *Store) UpdateShardLocation(context.Context, metadata.ShardLocationUpdate) error {
	return metadata.ErrNotImplemented
}

// WalkChunks implements metadata.Metadata. It runs inside one read
// transaction, so fn sees a consistent snapshot.
func (s *Store) WalkChunks(ctx context.Context, fn func(bucket, key, versionID string, chunkIndex int, c metadata.Chunk) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
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
