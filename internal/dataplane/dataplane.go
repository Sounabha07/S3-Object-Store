// Package dataplane turns object byte streams into stored chunks and back.
//
// An object is split into chunks of at most ChunkSize bytes. Each chunk is
// stored as one or more shards; in single-node mode (M1) that is exactly one
// shard on the local store. Later milestones replace the single shard with
// 3x replication or Reed-Solomon 4+2 behind the same Write/Read API.
//
// Memory use is bounded: writes stream through to storage without buffering
// a chunk, and reads hold at most one chunk in memory at a time so that the
// shard's checksum is verified before any of its bytes reach the client.
package dataplane

import (
	"bufio"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// DefaultChunkSize is the maximum logical size of one chunk.
const DefaultChunkSize = 4 << 20

// ErrTooLarge is returned by Write when the body exceeds the size limit.
var ErrTooLarge = errors.New("dataplane: object too large")

// Plane reads and writes objects against a single storage backend.
type Plane struct {
	store     storage.Storage
	node      placement.NodeID
	chunkSize int64
	bufs      sync.Pool // *[]byte of len chunkSize
}

// New returns a Plane that stores every chunk as one shard on store, recorded
// as living on node. chunkSize <= 0 selects DefaultChunkSize.
func New(store storage.Storage, node placement.NodeID, chunkSize int64) *Plane {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	p := &Plane{store: store, node: node, chunkSize: chunkSize}
	p.bufs.New = func() any {
		b := make([]byte, chunkSize)
		return &b
	}
	return p
}

// WriteResult describes a stored object body.
type WriteResult struct {
	Size   int64
	MD5    [md5.Size]byte
	Chunks []metadata.Chunk
}

// Write stores everything read from r, up to maxSize bytes (maxSize < 0
// means unlimited). If Write fails, shards already stored are left for
// garbage collection; nothing references them.
func (p *Plane) Write(ctx context.Context, r io.Reader, maxSize int64) (WriteResult, error) {
	var res WriteResult
	h := md5.New()
	src := r
	if maxSize >= 0 {
		src = io.LimitReader(r, maxSize+1)
	}
	br := bufio.NewReaderSize(io.TeeReader(src, h), 64<<10)

	for {
		if _, err := br.Peek(1); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return res, fmt.Errorf("dataplane: read body: %w", err)
		}
		if maxSize >= 0 && res.Size >= maxSize {
			return res, ErrTooLarge
		}
		info, err := p.store.Put(ctx, io.LimitReader(br, p.chunkSize), storage.ZeroShardID)
		if err != nil {
			return res, fmt.Errorf("dataplane: store chunk %d: %w", len(res.Chunks), err)
		}
		res.Chunks = append(res.Chunks, metadata.Chunk{
			Offset: res.Size,
			Size:   info.Size,
			Scheme: metadata.Single,
			Shards: []metadata.ShardRef{{Index: 0, ID: info.ID, Size: info.Size, Node: p.node}},
		})
		res.Size += info.Size
	}
	if maxSize >= 0 && res.Size > maxSize {
		return res, ErrTooLarge
	}
	copy(res.MD5[:], h.Sum(nil))
	return res, nil
}

// Read writes bytes [off, off+length) of obj to w. Each chunk is read and
// verified in full before any of it is written, so a corrupt or missing shard
// is reported before its bytes are sent. An error after some chunks were
// written means w received a prefix of the requested range.
func (p *Plane) Read(ctx context.Context, w io.Writer, obj metadata.ObjectMeta, off, length int64) error {
	if off < 0 || length < 0 || off+length > obj.Size {
		return fmt.Errorf("dataplane: range [%d, %d) outside object of size %d", off, off+length, obj.Size)
	}
	end := off + length
	for i, c := range obj.Chunks {
		cEnd := c.Offset + c.Size
		if cEnd <= off || c.Offset >= end || c.Size == 0 {
			continue
		}
		data, release, err := p.readChunk(ctx, c)
		if err != nil {
			return fmt.Errorf("dataplane: chunk %d: %w", i, err)
		}
		from := max(off, c.Offset) - c.Offset
		to := min(end, cEnd) - c.Offset
		_, err = w.Write(data[from:to])
		release()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Plane) readChunk(ctx context.Context, c metadata.Chunk) ([]byte, func(), error) {
	if len(c.Shards) == 0 {
		return nil, nil, fmt.Errorf("no shards recorded")
	}
	if c.Size > p.chunkSize {
		return nil, nil, fmt.Errorf("chunk size %d exceeds buffer %d", c.Size, p.chunkSize)
	}
	ref := c.Shards[0]
	bp := p.bufs.Get().(*[]byte)
	release := func() { p.bufs.Put(bp) }
	buf := (*bp)[:c.Size]
	if err := p.readShard(ctx, ref, buf); err != nil {
		release()
		return nil, nil, err
	}
	return buf, release, nil
}

// readShard fills buf with the shard and confirms the shard ends exactly
// there, which is when the storage layer verifies the checksum.
func (p *Plane) readShard(ctx context.Context, ref metadata.ShardRef, buf []byte) error {
	rc, err := p.store.Get(ctx, ref.ID)
	if err != nil {
		return err
	}
	defer rc.Close()
	if _, err := io.ReadFull(rc, buf); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: shard %s shorter than %d bytes", storage.ErrCorrupt, ref.ID, len(buf))
		}
		return err
	}
	var extra [1]byte
	for {
		n, err := rc.Read(extra[:])
		if n > 0 {
			return fmt.Errorf("%w: shard %s longer than %d bytes", storage.ErrCorrupt, ref.ID, len(buf))
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
