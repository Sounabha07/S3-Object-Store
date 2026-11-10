package raftmeta

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
)

// fakeLeader applies commands to a local store, standing in for a cluster
// so the client's retry logic can be tested deterministically.
type fakeLeader struct {
	store *boltstore.Store
	// before is called first on every request; returning true means it has
	// handled (or sabotaged) the request.
	before func(w http.ResponseWriter, r *http.Request, n int64) bool
	calls  atomic.Int64
}

func newFakeLeader(t *testing.T) (*fakeLeader, *httptest.Server) {
	t.Helper()
	s, err := boltstore.Open(filepath.Join(t.TempDir(), "fsm.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	f := &fakeLeader{store: s}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := f.calls.Add(1)
		if f.before != nil && f.before(w, r, n) {
			return
		}
		switch r.URL.Path {
		case "/v1/apply":
			var cmd boltstore.Command
			_ = json.NewDecoder(r.Body).Decode(&cmd)
			if err := s.Apply(uint64(n), cmd); err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, struct{}{})
		case "/v1/buckets":
			res, err := s.ListBuckets(r.Context())
			respond(w, res, err)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

// applyThenDrop applies the request for real, then kills the connection
// before responding: the client cannot know the write succeeded.
func applyThenDrop(f *fakeLeader) func(http.ResponseWriter, *http.Request, int64) bool {
	return func(w http.ResponseWriter, r *http.Request, n int64) bool {
		if n != 1 {
			return false
		}
		var cmd boltstore.Command
		_ = json.NewDecoder(r.Body).Decode(&cmd)
		_ = f.store.Apply(uint64(n), cmd)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return true
	}
}

func TestClientUncertainCreateBucketIsIdempotent(t *testing.T) {
	f, srv := newFakeLeader(t)
	f.before = applyThenDrop(f)
	c := NewClient([]string{srv.URL}, nil)
	if err := c.CreateBucket(bg, "b"); err != nil {
		t.Fatalf("CreateBucket whose first attempt succeeded silently: %v", err)
	}
	if f.calls.Load() < 2 {
		t.Fatal("client did not retry")
	}
	// Without uncertainty, a duplicate is still reported.
	if err := c.CreateBucket(bg, "b"); !errors.Is(err, metadata.ErrBucketAlreadyExists) {
		t.Fatalf("duplicate CreateBucket: %v", err)
	}
}

func TestClientUncertainDeleteBucketIsIdempotent(t *testing.T) {
	f, srv := newFakeLeader(t)
	c := NewClient([]string{srv.URL}, nil)
	if err := c.CreateBucket(bg, "b"); err != nil {
		t.Fatal(err)
	}
	f.calls.Store(0)
	f.before = applyThenDrop(f)
	if err := c.DeleteBucket(bg, "b"); err != nil {
		t.Fatalf("DeleteBucket whose first attempt succeeded silently: %v", err)
	}
}

func TestClientRetriesUnavailable(t *testing.T) {
	f, srv := newFakeLeader(t)
	f.before = func(w http.ResponseWriter, r *http.Request, n int64) bool {
		if n <= 3 {
			writeError(w, metadata.ErrUnavailable)
			return true
		}
		return false
	}
	c := NewClient([]string{srv.URL}, nil)
	if _, err := c.ListBuckets(bg); err != nil {
		t.Fatalf("ListBuckets after transient unavailability: %v", err)
	}
	if f.calls.Load() != 4 {
		t.Fatalf("calls = %d, want 4", f.calls.Load())
	}
}

func TestClientFollowsLeaderHint(t *testing.T) {
	_, leader := newFakeLeader(t)
	var followerCalls atomic.Int64
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followerCalls.Add(1)
		writeError(w, &notLeaderError{leader: leader.URL})
	}))
	defer follower.Close()

	c := NewClient([]string{follower.URL}, nil)
	for i := 0; i < 3; i++ {
		if _, err := c.ListBuckets(bg); err != nil {
			t.Fatal(err)
		}
	}
	if n := followerCalls.Load(); n != 1 {
		t.Fatalf("follower contacted %d times; the leader should be cached after the first redirect", n)
	}
}

func TestClientGivesUpAtDeadline(t *testing.T) {
	down := httptest.NewServer(http.NotFoundHandler())
	url := down.URL
	down.Close() // nothing listens here any more
	c := NewClient([]string{url}, nil)
	ctx, cancel := context.WithTimeout(bg, 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := c.PutObject(ctx, metadata.ObjectMeta{Bucket: "b", Key: "k"})
	if !errors.Is(err, metadata.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %v to give up", d)
	}
}

func TestClientDomainErrorsAreNotRetried(t *testing.T) {
	f, srv := newFakeLeader(t)
	c := NewClient([]string{srv.URL}, nil)
	if err := c.DeleteBucket(bg, "missing"); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("err = %v", err)
	}
	if f.calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", f.calls.Load())
	}
}

func TestClientRejectsInvalidCommandsLocally(t *testing.T) {
	f, srv := newFakeLeader(t)
	c := NewClient([]string{srv.URL}, nil)
	if err := c.PutObject(bg, metadata.ObjectMeta{Bucket: "b"}); !errors.Is(err, metadata.ErrInvalidArgument) {
		t.Fatalf("err = %v", err)
	}
	if f.calls.Load() != 0 {
		t.Fatal("invalid command was sent to the server")
	}
}

func TestClientDetectsTruncatedChunkStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"bucket":"b","key":"k","chunk":{"Offset":0,"Size":1}}` + "\n"))
		// no completion marker
	}))
	defer srv.Close()
	c := NewClient([]string{srv.URL}, nil)
	visited := 0
	err := c.WalkChunks(bg, func(string, string, string, int, metadata.Chunk) error { visited++; return nil })
	if !errors.Is(err, metadata.ErrUnavailable) || visited != 1 {
		t.Fatalf("visited %d, err %v; want 1 and ErrUnavailable", visited, err)
	}
}

func TestErrorCodesRoundTrip(t *testing.T) {
	for _, e := range errorCodes {
		rec := httptest.NewRecorder()
		writeError(rec, e.err)
		var we wireError
		if err := json.NewDecoder(rec.Body).Decode(&we); err != nil {
			t.Fatal(err)
		}
		if got := codeError(we.Code); !errors.Is(got, e.err) {
			t.Errorf("%v encoded as %q decodes to %v", e.err, we.Code, got)
		}
	}
	if errorCode(errors.New("other")) != codeInternal || codeError("nope") != nil {
		t.Error("unknown errors must map to Internal / nil")
	}
	rec := httptest.NewRecorder()
	writeError(rec, errOutcomeUnknown)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), codeOutcomeUnknown) {
		t.Errorf("outcome unknown encoded as %d %s", rec.Code, rec.Body.String())
	}
}

// TestClientConcurrentLeaderCache exercises the leader cache from many
// goroutines while redirects and connection failures keep changing it: the
// follower alternately points at a dead "leader" (as after a crash) and the
// real one. Under -race it catches the client caching a pointer to its own
// loop variable, which it then reassigns after the dead leader fails.
func TestClientConcurrentLeaderCache(t *testing.T) {
	_, leader := newFakeLeader(t)
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	var redirects atomic.Int64
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hint := leader.URL
		if redirects.Add(1)%2 == 0 {
			hint = deadURL
		}
		writeError(w, &notLeaderError{leader: hint})
	}))
	defer follower.Close()

	c := NewClient([]string{deadURL, follower.URL, leader.URL}, nil)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if i%5 == 0 {
					c.forget(leader.URL) // force re-discovery
				}
				if _, err := c.ListBuckets(bg); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestClientSkipsUnresponsiveNode reproduces what a stopped Docker container
// looks like: connections are accepted (or dropped) but never answered.
// Before the per-attempt timeout, one such attempt used up the whole call
// deadline and the call failed although a healthy leader was reachable.
func TestClientSkipsUnresponsiveNode(t *testing.T) {
	_, leader := newFakeLeader(t)
	hole, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer hole.Close()
	go func() {
		for {
			conn, err := hole.Accept()
			if err != nil {
				return
			}
			defer conn.Close() // hold the connection open, never respond
		}
	}()

	c := NewClient([]string{"http://" + hole.Addr().String(), leader.URL}, nil)
	c.AttemptTimeout = 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	start := time.Now()
	c.next.Store(^uint32(0)) // make the first pick the unresponsive node
	if _, err := c.ListBuckets(ctx); err != nil {
		t.Fatalf("ListBuckets with one unresponsive node: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %v; the unresponsive node was not abandoned after AttemptTimeout", d)
	}
}

// TestClientDoesNotRetryDeadNodeImmediately: when the cached leader stops
// answering, the next attempt goes to a different node. The old rotation
// (by attempt number) could hit the same dead node twice, doubling the
// stall after a leader crash.
func TestClientDoesNotRetryDeadNodeImmediately(t *testing.T) {
	_, a := newFakeLeader(t)
	_, c2 := newFakeLeader(t)
	hole, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer hole.Close()
	var accepted atomic.Int64
	go func() {
		for {
			conn, err := hole.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			defer conn.Close()
		}
	}()
	dead := "http://" + hole.Addr().String()
	cl := NewClient([]string{a.URL, dead, c2.URL}, nil)
	cl.AttemptTimeout = 300 * time.Millisecond
	cl.setLeader(dead) // the leader we knew about just crashed
	start := time.Now()
	if _, err := cl.ListBuckets(bg); err != nil {
		t.Fatal(err)
	}
	if n := accepted.Load(); n != 1 {
		t.Fatalf("dead node contacted %d times, want 1", n)
	}
	if d := time.Since(start); d > 550*time.Millisecond {
		t.Fatalf("took %v; expected one attempt timeout plus a fast retry", d)
	}
}
