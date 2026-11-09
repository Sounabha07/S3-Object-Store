// Package storage defines the content-addressed shard store used by storage
// nodes. Shards are immutable byte blobs named by the SHA-256 of their
// contents. The same interface is implemented by the local disk backend and
// by the HTTP client that talks to a remote storage node.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"
)

// ShardID is the SHA-256 digest of a shard's bytes.
type ShardID [sha256.Size]byte

// ZeroShardID is the zero value; passed to Put it means "no expected hash".
var ZeroShardID ShardID

var (
	// ErrNotFound is returned when a shard does not exist on this store.
	ErrNotFound = errors.New("storage: shard not found")
	// ErrCorrupt is returned when shard bytes do not match their ShardID.
	// Callers must treat a corrupt shard exactly like a missing one.
	ErrCorrupt = errors.New("storage: shard checksum mismatch")
	// ErrInvalidShardID is returned by ParseShardID for malformed input.
	ErrInvalidShardID = errors.New("storage: invalid shard id")
)

// ComputeShardID returns the ShardID for b.
func ComputeShardID(b []byte) ShardID { return sha256.Sum256(b) }

// String returns the lowercase hex encoding of the ID.
func (id ShardID) String() string { return hex.EncodeToString(id[:]) }

// IsZero reports whether id is the zero value.
func (id ShardID) IsZero() bool { return id == ZeroShardID }

// ParseShardID parses a 64-character lowercase or uppercase hex string.
func ParseShardID(s string) (ShardID, error) {
	var id ShardID
	if len(s) != hex.EncodedLen(len(id)) {
		return id, fmt.Errorf("%w: length %d", ErrInvalidShardID, len(s))
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return ShardID{}, fmt.Errorf("%w: %v", ErrInvalidShardID, err)
	}
	return id, nil
}

// MarshalText implements encoding.TextMarshaler (used by JSON encoding).
func (id ShardID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *ShardID) UnmarshalText(b []byte) error {
	parsed, err := ParseShardID(string(b))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// ErrModified is returned by DeleteIfOlder when the shard was written or
// touched at or after the cutoff.
var ErrModified = errors.New("storage: shard modified since cutoff")

// ConditionalDeleter is implemented by stores that can delete a shard only
// if it is older than a cutoff, atomically with respect to Put. Garbage
// collection relies on it: a Put that reuses an existing (deduplicated)
// shard refreshes its modification time, so a shard that an in-flight
// upload just reused is never deleted.
type ConditionalDeleter interface {
	DeleteIfOlder(ctx context.Context, id ShardID, cutoff time.Time) error
}

// ShardInfo describes a stored shard.
type ShardInfo struct {
	ID      ShardID
	Size    int64
	ModTime time.Time // used by GC to enforce a grace period
}

// Storage is a durable, content-addressed shard store.
//
// Implementations must be safe for concurrent use.
type Storage interface {
	// Put streams r to durable storage. It must fsync the data (and the
	// directory entry) before returning success. If expected is non-zero and
	// the computed hash differs, Put stores nothing and returns ErrCorrupt.
	// Putting a shard that already exists is a successful no-op.
	Put(ctx context.Context, r io.Reader, expected ShardID) (ShardInfo, error)

	// Get streams the whole shard. The SHA-256 is verified as bytes are read;
	// on mismatch the final Read returns ErrCorrupt instead of io.EOF.
	Get(ctx context.Context, id ShardID) (io.ReadCloser, error)

	// Stat returns shard metadata without reading its contents.
	Stat(ctx context.Context, id ShardID) (ShardInfo, error)

	// Delete removes a shard. Only garbage collection should call this,
	// because shards may be shared by several objects.
	Delete(ctx context.Context, id ShardID) error

	// Walk calls fn for every shard on the store, in no particular order.
	// Returning an error from fn stops the walk and returns that error.
	Walk(ctx context.Context, fn func(ShardInfo) error) error

	// Health returns nil if the store can currently serve reads and writes.
	Health(ctx context.Context) error
}
