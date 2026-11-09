// Package dataplane turns object byte streams into stored chunks and back.
//
// An object is split into chunks of at most ChunkSize bytes. Each chunk is
// stored as Replicas identical shards on distinct storage nodes chosen by
// the placement ring. (Erasure coding arrives in M4 behind the same API.)
//
// Memory use is bounded per request: a writer holds one chunk at a time
// (it is needed for every replica), and a reader holds one chunk at a time
// so the shard's checksum is verified before any of its bytes reach the
// client. The whole object is never buffered.
package dataplane

import (
	"bufio"
	"context"
	"crypto/md5" // #nosec G501 -- S3 protocol: ETag and Content-MD5 are defined as MD5; integrity relies on SHA-256
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"sync"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// DefaultChunkSize is the maximum logical size of one chunk.
const DefaultChunkSize = 4 << 20

// DefaultReplicas is the number of copies of each chunk in a cluster.
const DefaultReplicas = 3

var (
	// ErrTooLarge is returned by Write when the body exceeds the size limit.
	ErrTooLarge = errors.New("dataplane: object too large")
	// ErrInsufficientNodes means a chunk could not be stored with the
	// required redundancy because too few nodes accepted it.
	ErrInsufficientNodes = errors.New("dataplane: not enough healthy storage nodes")
)

// Config configures a Plane.
type Config struct {
	Nodes     map[placement.NodeID]storage.Storage
	Placement placement.Placement // nil: a ring of all Nodes
	Health    *Health             // nil: a new tracker with default cooldown
	ChunkSize int64               // <= 0: DefaultChunkSize
	Replicas  int                 // <= 0: min(DefaultReplicas, len(Nodes))
}

// Plane reads and writes objects across a set of storage nodes.
type Plane struct {
	mu        sync.RWMutex // guards nodes
	nodes     map[placement.NodeID]storage.Storage
	ring      placement.Placement
	health    *Health
	chunkSize int64
	replicas  int
	bufs      sync.Pool // *[]byte of len chunkSize
}

// NewCluster returns a Plane over cfg.Nodes.
func NewCluster(cfg Config) (*Plane, error) {
	if len(cfg.Nodes) == 0 {
		return nil, errors.New("dataplane: no storage nodes")
	}
	nodes := make(map[placement.NodeID]storage.Storage, len(cfg.Nodes))
	for id, s := range cfg.Nodes {
		nodes[id] = s
	}
	p := &Plane{nodes: nodes, ring: cfg.Placement, health: cfg.Health, chunkSize: cfg.ChunkSize, replicas: cfg.Replicas}
	if p.ring == nil {
		ids := make([]placement.NodeID, 0, len(cfg.Nodes))
		for id := range cfg.Nodes {
			ids = append(ids, id)
		}
		p.ring = placement.NewRing(ids...)
	}
	if p.health == nil {
		p.health = NewHealth(0)
	}
	if p.chunkSize <= 0 {
		p.chunkSize = DefaultChunkSize
	}
	if p.replicas <= 0 {
		p.replicas = min(DefaultReplicas, len(cfg.Nodes))
	}
	if p.replicas > len(cfg.Nodes) {
		return nil, fmt.Errorf("dataplane: %d replicas need at least %d nodes, have %d", p.replicas, p.replicas, len(cfg.Nodes))
	}
	p.bufs.New = func() any {
		b := make([]byte, p.chunkSize)
		return &b
	}
	return p, nil
}

// New returns a single-node Plane: every chunk is one shard on store,
// recorded as living on node. chunkSize <= 0 selects DefaultChunkSize.
func New(store storage.Storage, node placement.NodeID, chunkSize int64) *Plane {
	p, err := NewCluster(Config{Nodes: map[placement.NodeID]storage.Storage{node: store}, ChunkSize: chunkSize, Replicas: 1})
	if err != nil {
		panic(err) // unreachable: one node, one replica
	}
	return p
}

// Nodes returns a copy of the storage nodes by ID.
func (p *Plane) Nodes() map[placement.NodeID]storage.Storage {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[placement.NodeID]storage.Storage, len(p.nodes))
	for id, s := range p.nodes {
		out[id] = s
	}
	return out
}

// Node returns one storage node.
func (p *Plane) Node(id placement.NodeID) (storage.Storage, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s, ok := p.nodes[id]
	return s, ok
}

// AddNode adds a storage node and puts it on the placement ring. Existing
// data stays where metadata says it is; run the rebalancer to move shards
// onto the new node.
func (p *Plane) AddNode(id placement.NodeID, store storage.Storage) {
	p.mu.Lock()
	p.nodes[id] = store
	p.mu.Unlock()
	p.ring.Add(id, 0)
}

// Placement returns the ring used for new writes.
func (p *Plane) Placement() placement.Placement { return p.ring }

// Health returns the node health tracker.
func (p *Plane) Health() *Health { return p.health }

// Replicas returns the number of copies written per chunk.
func (p *Plane) Replicas() int { return p.replicas }

// ObjectKey identifies one version of an object for placement.
func ObjectKey(bucket, key, versionID string) string {
	return bucket + "/" + key + "/" + versionID
}

// PlacementKey is the ring key of one chunk of an object version.
func PlacementKey(objectKey string, chunkIndex int) string {
	return objectKey + "/" + strconv.Itoa(chunkIndex)
}

// WriteResult describes a stored object body.
type WriteResult struct {
	Size   int64
	MD5    [md5.Size]byte
	Chunks []metadata.Chunk
}

// Write stores everything read from r, up to maxSize bytes (maxSize < 0
// means unlimited). objectKey (see ObjectKey) determines placement. If
// Write fails, shards already stored are left for garbage collection;
// nothing references them.
func (p *Plane) Write(ctx context.Context, r io.Reader, maxSize int64, objectKey string) (WriteResult, error) {
	var res WriteResult
	h := md5.New() // #nosec G401 -- computes the S3 ETag (MD5 by spec), not used for security
	src := r
	if maxSize >= 0 {
		src = io.LimitReader(r, maxSize+1)
	}
	br := bufio.NewReaderSize(io.TeeReader(src, h), 64<<10)

	bp := p.bufs.Get().(*[]byte)
	defer p.bufs.Put(bp)
	for idx := 0; ; idx++ {
		n, err := io.ReadFull(br, *bp)
		if errors.Is(err, io.EOF) {
			break // no more data
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return res, fmt.Errorf("dataplane: read body: %w", err)
		}
		if maxSize >= 0 && res.Size+int64(n) > maxSize {
			return res, ErrTooLarge
		}
		chunk, perr := p.putChunk(ctx, (*bp)[:n], PlacementKey(objectKey, idx))
		if perr != nil {
			return res, fmt.Errorf("dataplane: store chunk %d: %w", idx, perr)
		}
		chunk.Offset = res.Size
		res.Chunks = append(res.Chunks, chunk)
		res.Size += int64(n)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break // short final chunk
		}
	}
	copy(res.MD5[:], h.Sum(nil))
	return res, nil
}

// putChunk stores data as p.replicas copies on distinct nodes. A node that
// fails is replaced by the next eligible node on the ring.
func (p *Plane) putChunk(ctx context.Context, data []byte, key string) (metadata.Chunk, error) {
	id := storage.ShardID(sha256.Sum256(data))
	size := int64(len(data))
	tried := p.health.Down()
	var stored []placement.NodeID
	var errs []error
	for len(stored) < p.replicas {
		targets, err := p.ring.Pick(key, p.replicas-len(stored), tried...)
		if err != nil {
			errs = append(errs, err)
			return metadata.Chunk{}, fmt.Errorf("%w: stored %d of %d copies: %w", ErrInsufficientNodes, len(stored), p.replicas, errors.Join(errs...))
		}
		results := make([]error, len(targets))
		var wg sync.WaitGroup
		for i, node := range targets {
			wg.Add(1)
			go func() {
				defer wg.Done()
				store, _ := p.Node(node)
				_, results[i] = store.Put(ctx, bytesReader(data), id)
			}()
		}
		wg.Wait()
		for i, node := range targets {
			tried = append(tried, node)
			if results[i] != nil {
				errs = append(errs, fmt.Errorf("%s: %w", node, results[i]))
				if errors.Is(results[i], storage.ErrUnavailable) {
					p.health.MarkDown(node)
				}
				continue
			}
			stored = append(stored, node)
		}
		if err := ctx.Err(); err != nil {
			return metadata.Chunk{}, err
		}
	}
	chunk := metadata.Chunk{Size: size, Scheme: metadata.Scheme{Data: 1, Parity: p.replicas - 1}}
	for i, node := range stored {
		chunk.Shards = append(chunk.Shards, metadata.ShardRef{Index: i, ID: id, Size: size, Node: node})
	}
	return chunk, nil
}

// Read writes bytes [off, off+length) of obj to w. Each chunk is read and
// verified in full before any of it is written, so a corrupt or missing
// shard is replaced by another replica before its bytes are sent. An error
// after some chunks were written means w received a prefix of the range.
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
		data, release, err := p.ReadChunk(ctx, c)
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

// ReadChunk returns the verified contents of chunk c in a pooled buffer;
// call release when done with it. Replicas are tried healthy-first.
func (p *Plane) ReadChunk(ctx context.Context, c metadata.Chunk) (data []byte, release func(), err error) {
	if len(c.Shards) == 0 {
		return nil, nil, errors.New("no shards recorded")
	}
	if c.Size > p.chunkSize {
		return nil, nil, fmt.Errorf("chunk size %d exceeds buffer %d", c.Size, p.chunkSize)
	}
	bp := p.bufs.Get().(*[]byte)
	release = func() { p.bufs.Put(bp) }
	buf := (*bp)[:c.Size]

	order := slices.Clone(c.Shards)
	slices.SortStableFunc(order, func(a, b metadata.ShardRef) int {
		da, db := p.health.IsDown(a.Node), p.health.IsDown(b.Node)
		switch {
		case da == db:
			return 0
		case db:
			return -1
		}
		return 1
	})
	var errs []error
	for _, ref := range order {
		if err := p.ReadShard(ctx, ref, buf); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", ref.Node, err))
			if ctx.Err() != nil {
				break
			}
			continue
		}
		return buf, release, nil
	}
	release()
	return nil, nil, errors.Join(errs...)
}

// ReadShard fills buf with shard ref from its node and confirms the shard
// ends exactly there, which is when the checksum is verified.
func (p *Plane) ReadShard(ctx context.Context, ref metadata.ShardRef, buf []byte) error {
	store, ok := p.Node(ref.Node)
	if !ok {
		return fmt.Errorf("%w: unknown node %q", storage.ErrUnavailable, ref.Node)
	}
	rc, err := store.Get(ctx, ref.ID)
	if err != nil {
		if errors.Is(err, storage.ErrUnavailable) {
			p.health.MarkDown(ref.Node)
		}
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

// bytesReader returns a fresh reader per replica so parallel puts don't
// share a read offset.
func bytesReader(b []byte) io.Reader { return &sliceReader{b: b} }

type sliceReader struct {
	b   []byte
	off int
}

func (s *sliceReader) Read(p []byte) (int, error) {
	if s.off >= len(s.b) {
		return 0, io.EOF
	}
	n := copy(p, s.b[s.off:])
	s.off += n
	return n, nil
}
