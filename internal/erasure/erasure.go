// Package erasure wraps Reed-Solomon coding (klauspost/reedsolomon) for
// fixed-size chunks: k data shards plus m parity shards, any k of which are
// enough to rebuild the chunk.
package erasure

import (
	"errors"
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// ErrTooFewShards means fewer than k shards were available to reconstruct.
var ErrTooFewShards = errors.New("erasure: too few shards to reconstruct")

// Codec encodes and reconstructs chunks with k data and m parity shards.
type Codec struct {
	k, m int
	enc  reedsolomon.Encoder
}

// New returns a codec. k and m must be at least 1.
func New(k, m int) (*Codec, error) {
	if k < 1 || m < 1 {
		return nil, fmt.Errorf("erasure: invalid scheme %d+%d", k, m)
	}
	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, fmt.Errorf("erasure: %w", err)
	}
	return &Codec{k: k, m: m, enc: enc}, nil
}

// Data returns k, Parity returns m, Total returns k+m.
func (c *Codec) Data() int   { return c.k }
func (c *Codec) Parity() int { return c.m }
func (c *Codec) Total() int  { return c.k + c.m }

// ShardSize is the size of every shard for a chunk of n bytes. The last
// data shard is zero-padded up to it.
func (c *Codec) ShardSize(n int) int { return max(1, (n+c.k-1)/c.k) }

// Encode splits data into k data shards and computes m parity shards,
// all as consecutive slices of buf. buf must hold Total()*ShardSize(len(data))
// bytes; its previous contents are irrelevant.
func (c *Codec) Encode(data, buf []byte) ([][]byte, error) {
	size := c.ShardSize(len(data))
	need := c.Total() * size
	if len(buf) < need {
		return nil, fmt.Errorf("erasure: buffer of %d bytes, need %d", len(buf), need)
	}
	buf = buf[:need]
	n := copy(buf, data)
	clear(buf[n : c.k*size]) // padding of the last data shard
	shards := make([][]byte, c.Total())
	for i := range shards {
		shards[i] = buf[i*size : (i+1)*size : (i+1)*size]
	}
	if err := c.enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("erasure: encode: %w", err)
	}
	return shards, nil
}

// ReconstructData fills in missing data shards. shards has Total()
// entries; missing ones are nil (or zero-length) and present ones must all
// have the same size. Parity shards that are missing are left missing.
func (c *Codec) ReconstructData(shards [][]byte) error {
	if len(shards) != c.Total() {
		return fmt.Errorf("erasure: got %d shards, want %d", len(shards), c.Total())
	}
	present := 0
	for _, s := range shards {
		if len(s) > 0 {
			present++
		}
	}
	if present < c.k {
		return fmt.Errorf("%w: have %d of %d needed", ErrTooFewShards, present, c.k)
	}
	if err := c.enc.ReconstructData(shards); err != nil {
		return fmt.Errorf("erasure: reconstruct: %w", err)
	}
	return nil
}

// Reconstruct fills in every missing shard, data and parity (used by repair).
func (c *Codec) Reconstruct(shards [][]byte) error {
	if len(shards) != c.Total() {
		return fmt.Errorf("erasure: got %d shards, want %d", len(shards), c.Total())
	}
	present := 0
	for _, s := range shards {
		if len(s) > 0 {
			present++
		}
	}
	if present < c.k {
		return fmt.Errorf("%w: have %d of %d needed", ErrTooFewShards, present, c.k)
	}
	if err := c.enc.Reconstruct(shards); err != nil {
		return fmt.Errorf("erasure: reconstruct: %w", err)
	}
	return nil
}

// Join copies the first n bytes of the data shards into dst, which must
// hold n bytes. All data shards must be present.
func (c *Codec) Join(dst []byte, shards [][]byte, n int) error {
	if len(dst) < n {
		return fmt.Errorf("erasure: destination of %d bytes, need %d", len(dst), n)
	}
	off := 0
	for i := 0; i < c.k && off < n; i++ {
		if len(shards[i]) == 0 {
			return fmt.Errorf("erasure: data shard %d missing", i)
		}
		off += copy(dst[off:n], shards[i])
	}
	if off != n {
		return fmt.Errorf("erasure: shards hold %d bytes, need %d", off, n)
	}
	return nil
}
