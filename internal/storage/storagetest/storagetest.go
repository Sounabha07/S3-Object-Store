// Package storagetest is a conformance suite for storage.Storage
// implementations. The disk backend runs it now; the remote client will run
// the same suite against a storage node in M3.
package storagetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// Run executes the suite. newStore must return an empty store.
func Run(t *testing.T, newStore func(t *testing.T) storage.Storage) {
	t.Run("PutGetRoundTrip", func(t *testing.T) { testRoundTrip(t, newStore(t)) })
	t.Run("EmptyShard", func(t *testing.T) { testEmpty(t, newStore(t)) })
	t.Run("PutIsIdempotent", func(t *testing.T) { testIdempotent(t, newStore(t)) })
	t.Run("PutExpectedMismatch", func(t *testing.T) { testExpectedMismatch(t, newStore(t)) })
	t.Run("PutExpectedMatch", func(t *testing.T) { testExpectedMatch(t, newStore(t)) })
	t.Run("NotFound", func(t *testing.T) { testNotFound(t, newStore(t)) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, newStore(t)) })
	t.Run("Walk", func(t *testing.T) { testWalk(t, newStore(t)) })
	t.Run("ConcurrentPutsSameContent", func(t *testing.T) { testConcurrent(t, newStore(t)) })
	t.Run("CancelledContext", func(t *testing.T) { testCancelled(t, newStore(t)) })
	t.Run("Health", func(t *testing.T) {
		if err := newStore(t).Health(context.Background()); err != nil {
			t.Fatalf("Health: %v", err)
		}
	})
}

// RandomBytes returns n cryptographically random bytes.
func RandomBytes(t testing.TB, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// MustPut stores b and returns its info.
func MustPut(t testing.TB, s storage.Storage, b []byte) storage.ShardInfo {
	t.Helper()
	info, err := s.Put(context.Background(), bytes.NewReader(b), storage.ZeroShardID)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	return info
}

// ReadAll reads a whole shard, returning the read error (if any).
func ReadAll(s storage.Storage, id storage.ShardID) ([]byte, error) {
	rc, err := s.Get(context.Background(), id)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func testRoundTrip(t *testing.T, s storage.Storage) {
	for _, n := range []int{1, 4096, 1 << 20, 3<<20 + 17} {
		data := RandomBytes(t, n)
		info := MustPut(t, s, data)
		if info.ID != storage.ComputeShardID(data) {
			t.Fatalf("size %d: id %s, want %s", n, info.ID, storage.ComputeShardID(data))
		}
		if info.Size != int64(n) {
			t.Fatalf("size %d: info.Size = %d", n, info.Size)
		}
		got, err := ReadAll(s, info.ID)
		if err != nil {
			t.Fatalf("size %d: read: %v", n, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("size %d: content mismatch", n)
		}
		st, err := s.Stat(context.Background(), info.ID)
		if err != nil || st.Size != int64(n) {
			t.Fatalf("size %d: Stat = %+v, %v", n, st, err)
		}
	}
}

func testEmpty(t *testing.T, s storage.Storage) {
	info := MustPut(t, s, nil)
	got, err := ReadAll(s, info.ID)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty shard read = %d bytes, %v", len(got), err)
	}
}

func testIdempotent(t *testing.T, s storage.Storage) {
	data := RandomBytes(t, 1000)
	a := MustPut(t, s, data)
	b := MustPut(t, s, data)
	if a.ID != b.ID {
		t.Fatalf("ids differ: %s vs %s", a.ID, b.ID)
	}
	n := 0
	_ = s.Walk(context.Background(), func(storage.ShardInfo) error { n++; return nil })
	if n != 1 {
		t.Fatalf("Walk found %d shards after duplicate put, want 1", n)
	}
}

func testExpectedMismatch(t *testing.T, s storage.Storage) {
	data := RandomBytes(t, 100)
	wrong := storage.ComputeShardID([]byte("something else"))
	_, err := s.Put(context.Background(), bytes.NewReader(data), wrong)
	if !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("Put with wrong expected id: err = %v, want ErrCorrupt", err)
	}
	for _, id := range []storage.ShardID{wrong, storage.ComputeShardID(data)} {
		if _, err := s.Stat(context.Background(), id); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("shard %s stored after rejected put: %v", id, err)
		}
	}
}

func testExpectedMatch(t *testing.T, s storage.Storage) {
	data := RandomBytes(t, 100)
	id := storage.ComputeShardID(data)
	info, err := s.Put(context.Background(), bytes.NewReader(data), id)
	if err != nil || info.ID != id {
		t.Fatalf("Put with correct expected id = %+v, %v", info, err)
	}
}

func testNotFound(t *testing.T, s storage.Storage) {
	id := storage.ComputeShardID([]byte("missing"))
	if _, err := s.Get(context.Background(), id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get missing: %v", err)
	}
	if _, err := s.Stat(context.Background(), id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Stat missing: %v", err)
	}
	if err := s.Delete(context.Background(), id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Delete missing: %v", err)
	}
}

func testDelete(t *testing.T, s storage.Storage) {
	info := MustPut(t, s, RandomBytes(t, 10))
	if err := s.Delete(context.Background(), info.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(context.Background(), info.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get after delete: %v", err)
	}
}

func testWalk(t *testing.T, s storage.Storage) {
	want := map[storage.ShardID]int64{}
	for i := 0; i < 50; i++ {
		info := MustPut(t, s, RandomBytes(t, i+1))
		want[info.ID] = info.Size
	}
	got := map[storage.ShardID]int64{}
	err := s.Walk(context.Background(), func(i storage.ShardInfo) error {
		if _, dup := got[i.ID]; dup {
			t.Errorf("Walk visited %s twice", i.ID)
		}
		got[i.ID] = i.Size
		return nil
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("Walk found %d shards, want %d", len(got), len(want))
	}
	for id, size := range want {
		if got[id] != size {
			t.Fatalf("Walk size for %s = %d, want %d", id, got[id], size)
		}
	}

	stop := errors.New("stop")
	calls := 0
	if err := s.Walk(context.Background(), func(storage.ShardInfo) error { calls++; return stop }); !errors.Is(err, stop) || calls != 1 {
		t.Fatalf("Walk early stop: err=%v calls=%d", err, calls)
	}
}

func testConcurrent(t *testing.T, s storage.Storage) {
	data := RandomBytes(t, 256<<10)
	want := storage.ComputeShardID(data)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, err := s.Put(context.Background(), bytes.NewReader(data), storage.ZeroShardID)
			if err == nil && info.ID != want {
				err = errors.New("wrong id")
			}
			if err == nil {
				var got []byte
				got, err = ReadAll(s, want)
				if err == nil && !bytes.Equal(got, data) {
					err = errors.New("content mismatch")
				}
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent put/get: %v", err)
		}
	}
}

func testCancelled(t *testing.T, s storage.Storage) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	data := RandomBytes(t, 100)
	if _, err := s.Put(ctx, bytes.NewReader(data), storage.ZeroShardID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put with cancelled ctx: %v", err)
	}
	if _, err := s.Stat(context.Background(), storage.ComputeShardID(data)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("shard stored despite cancelled put: %v", err)
	}
}
