package erasure

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
)

func randomBytes(t testing.TB, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// subsets returns every subset of {0..n-1} of size r.
func subsets(n, r int) [][]int {
	var out [][]int
	var rec func(start int, cur []int)
	rec = func(start int, cur []int) {
		if len(cur) == r {
			out = append(out, append([]int(nil), cur...))
			return
		}
		for i := start; i < n; i++ {
			rec(i+1, append(cur, i))
		}
	}
	rec(0, nil)
	return out
}

func encode(t *testing.T, c *Codec, data []byte) [][]byte {
	t.Helper()
	buf := make([]byte, c.Total()*c.ShardSize(len(data)))
	shards, err := c.Encode(data, buf)
	if err != nil {
		t.Fatal(err)
	}
	// Return independent copies so tests can drop or damage shards freely.
	out := make([][]byte, len(shards))
	for i, s := range shards {
		out[i] = bytes.Clone(s)
	}
	return out
}

func decode(c *Codec, shards [][]byte, n int) ([]byte, error) {
	if err := c.ReconstructData(shards); err != nil {
		return nil, err
	}
	out := make([]byte, n)
	return out, c.Join(out, shards, n)
}

func TestAnyTwoMissingRecoverable(t *testing.T) {
	c, err := New(4, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{1, 3, 4, 5, 1000, 1 << 20, 4<<20 - 1} {
		data := randomBytes(t, size)
		full := encode(t, c, data)
		for r := 0; r <= 2; r++ {
			for _, missing := range subsets(6, r) {
				shards := make([][]byte, 6)
				for i := range full {
					shards[i] = bytes.Clone(full[i])
				}
				for _, i := range missing {
					shards[i] = nil
				}
				got, err := decode(c, shards, size)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("size %d missing %v: %v", size, missing, err)
				}
			}
		}
	}
}

func TestThreeMissingFails(t *testing.T) {
	c, _ := New(4, 2)
	data := randomBytes(t, 10_000)
	full := encode(t, c, data)
	combos := subsets(6, 3)
	if len(combos) != 20 {
		t.Fatalf("expected 20 combinations, got %d", len(combos))
	}
	for _, missing := range combos {
		shards := make([][]byte, 6)
		copy(shards, full)
		for _, i := range missing {
			shards[i] = nil
		}
		if _, err := decode(c, shards, len(data)); !errors.Is(err, ErrTooFewShards) {
			t.Fatalf("missing %v: err = %v, want ErrTooFewShards", missing, err)
		}
	}
}

func TestReconstructRebuildsParity(t *testing.T) {
	c, _ := New(4, 2)
	full := encode(t, c, randomBytes(t, 5000))
	shards := make([][]byte, 6)
	for i := range full {
		shards[i] = bytes.Clone(full[i])
	}
	shards[1], shards[5] = nil, nil // one data, one parity
	if err := c.Reconstruct(shards); err != nil {
		t.Fatal(err)
	}
	for i := range full {
		if !bytes.Equal(shards[i], full[i]) {
			t.Fatalf("shard %d rebuilt incorrectly", i)
		}
	}
}

func TestShardSizeAndOverhead(t *testing.T) {
	c, _ := New(4, 2)
	cases := map[int]int{1: 1, 4: 1, 5: 2, 4 << 20: 1 << 20, 4<<20 + 1: 1<<20 + 1}
	for n, want := range cases {
		if got := c.ShardSize(n); got != want {
			t.Errorf("ShardSize(%d) = %d, want %d", n, got, want)
		}
	}
	// Stored bytes for a full 4 MiB chunk are exactly 1.5x the data.
	n := 4 << 20
	if stored := c.Total() * c.ShardSize(n); float64(stored)/float64(n) != 1.5 {
		t.Fatalf("overhead %.3f, want 1.5", float64(stored)/float64(n))
	}
}

func TestEncodeIgnoresStaleBuffer(t *testing.T) {
	c, _ := New(4, 2)
	data := randomBytes(t, 13) // last data shard padded
	buf := bytes.Repeat([]byte{0xAA}, c.Total()*c.ShardSize(len(data)))
	shards, err := c.Encode(data, buf)
	if err != nil {
		t.Fatal(err)
	}
	clean := encode(t, c, data)
	for i := range shards {
		if !bytes.Equal(shards[i], clean[i]) {
			t.Fatalf("shard %d depends on previous buffer contents", i)
		}
	}
}

func TestErrors(t *testing.T) {
	if _, err := New(0, 2); err == nil {
		t.Error("k=0 accepted")
	}
	c, _ := New(4, 2)
	if _, err := c.Encode(make([]byte, 100), make([]byte, 10)); err == nil {
		t.Error("short buffer accepted")
	}
	if err := c.ReconstructData(make([][]byte, 5)); err == nil {
		t.Error("wrong shard count accepted")
	}
	if err := c.Join(make([]byte, 1), [][]byte{nil, nil, nil, nil, nil, nil}, 2); err == nil {
		t.Error("short destination accepted")
	}
}

func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte("hello erasure coding"), uint8(3))
	f.Fuzz(func(t *testing.T, data []byte, drop uint8) {
		if len(data) == 0 {
			return
		}
		c, _ := New(4, 2)
		full := encode(t, c, data)
		combos := subsets(6, 2)
		missing := combos[int(drop)%len(combos)]
		for _, i := range missing {
			full[i] = nil
		}
		got, err := decode(c, full, len(data))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("missing %v: %v", missing, err)
		}
	})
}
