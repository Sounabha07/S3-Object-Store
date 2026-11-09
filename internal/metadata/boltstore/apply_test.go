package boltstore

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
)

var t0 = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

func obj(bucket, key, version string) *metadata.ObjectMeta {
	return &metadata.ObjectMeta{Bucket: bucket, Key: key, VersionID: version, Size: 1, ETag: `"e"`, LastModified: t0}
}

// script is a command sequence whose results depend on state, so replaying
// it onto the wrong starting state would produce a different outcome.
var script = []Command{
	{Op: OpCreateBucket, Bucket: "b", Time: t0},
	{Op: OpPutObject, Object: obj("b", "k1", "v1")},
	{Op: OpDeleteBucket, Bucket: "b"}, // fails: not empty
	{Op: OpDeleteObject, Bucket: "b", Key: "k1"},
	{Op: OpDeleteBucket, Bucket: "b"}, // succeeds
	{Op: OpCreateBucket, Bucket: "b", Time: t0.Add(time.Hour)},
	{Op: OpPutObject, Object: obj("b", "k2", "v2")},
	{Op: OpCreateBucket, Bucket: "b", Time: t0}, // fails: exists
}

var scriptResults = []error{nil, nil, metadata.ErrBucketNotEmpty, nil, nil, nil, nil, metadata.ErrBucketAlreadyExists}

func runScript(t *testing.T, s *Store, from uint64) {
	t.Helper()
	for i, c := range script {
		idx := uint64(i + 1)
		if idx < from {
			continue
		}
		if err := s.Apply(idx, c); !errors.Is(err, scriptResults[i]) {
			t.Fatalf("command %d (%s): err = %v, want %v", idx, c.Op, err, scriptResults[i])
		}
	}
}

// dump returns a comparable view of the whole state.
func dump(t *testing.T, s *Store) string {
	t.Helper()
	var b bytes.Buffer
	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, bk := range buckets {
		fmt.Fprintf(&b, "bucket %s %s\n", bk.Name, bk.CreatedAt.Format(time.RFC3339))
		res, err := s.ListObjects(ctx, bk.Name, metadata.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range res.Objects {
			full, err := s.GetObject(ctx, bk.Name, o.Key)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&b, "  %s %s\n", o.Key, full.VersionID)
		}
	}
	idx, err := s.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(&b, "applied %d\n", idx)
	return b.String()
}

func TestApplyScript(t *testing.T) {
	s := newStore(t)
	runScript(t, s, 1)
	want := "bucket b " + t0.Add(time.Hour).Format(time.RFC3339) + "\n  k2 v2\napplied 8\n"
	if got := dump(t, s); got != want {
		t.Fatalf("state:\n%s\nwant:\n%s", got, want)
	}
}

// TestApplyReplayIsSkipped models a restart: Raft replays log entries the
// state machine already applied. They must be no-ops, or the replay could
// diverge (here: deleting the re-created bucket's contents again).
func TestApplyReplayIsSkipped(t *testing.T) {
	s := newStore(t)
	runScript(t, s, 1)
	before := dump(t, s)
	for i, c := range script {
		if err := s.Apply(uint64(i+1), c); err != nil {
			t.Fatalf("replayed command %d returned %v; replays must be silent no-ops", i+1, err)
		}
	}
	if after := dump(t, s); after != before {
		t.Fatalf("replay changed state:\n%s\nwas:\n%s", after, before)
	}
}

func TestApplyIndexSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	runScript(t, s, 1)
	want := dump(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if idx, _ := s.AppliedIndex(); idx != uint64(len(script)) {
		t.Fatalf("applied index after reopen = %d", idx)
	}
	if got := dump(t, s); got != want {
		t.Fatalf("state after reopen differs")
	}
}

// TestApplyDeterministic: two stores fed the same commands end identical.
func TestApplyDeterministic(t *testing.T) {
	a, b := newStore(t), newStore(t)
	runScript(t, a, 1)
	runScript(t, b, 1)
	if dump(t, a) != dump(t, b) {
		t.Fatal("same command sequence produced different states")
	}
}

func TestApplyRejectsInvalidCommands(t *testing.T) {
	s := newStore(t)
	cases := []Command{
		{Op: "Bogus"},
		{Op: OpCreateBucket},
		{Op: OpCreateBucket, Bucket: "bad\xff"},
		{Op: OpPutObject},
		{Op: OpPutObject, Object: &metadata.ObjectMeta{Bucket: "b"}},
		{Op: OpDeleteBucket},
	}
	for i, c := range cases {
		if err := s.Apply(uint64(i+1), c); !errors.Is(err, metadata.ErrInvalidArgument) {
			t.Errorf("%+v: err = %v, want ErrInvalidArgument", c, err)
		}
	}
	// Rejected commands still consume their log index.
	if idx, _ := s.AppliedIndex(); idx != uint64(len(cases)) {
		t.Fatalf("applied index = %d, want %d", idx, len(cases))
	}
}

func TestSnapshotRestore(t *testing.T) {
	src := newStore(t)
	runScript(t, src, 1)
	var snap bytes.Buffer
	if err := src.Snapshot(&snap); err != nil {
		t.Fatal(err)
	}

	dst := newStore(t)
	mustCreate(t, dst, "stale") // state that the restore must replace
	if err := dst.Restore(bytes.NewReader(snap.Bytes())); err != nil {
		t.Fatal(err)
	}
	if got, want := dump(t, dst), dump(t, src); got != want {
		t.Fatalf("restored state:\n%s\nwant:\n%s", got, want)
	}
	// The restored store keeps working, including index-based skipping.
	if err := dst.Apply(3, Command{Op: OpCreateBucket, Bucket: "late", Time: t0}); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.HeadBucket(ctx, "late"); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatal("command at an index below the snapshot was applied after restore")
	}
	if err := dst.Apply(9, Command{Op: OpCreateBucket, Bucket: "late", Time: t0}); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.HeadBucket(ctx, "late"); err != nil {
		t.Fatalf("command after the snapshot index not applied: %v", err)
	}
}

func TestRestoreRejectsGarbageAndKeepsState(t *testing.T) {
	s := newStore(t)
	mustCreate(t, s, "keep")
	if err := s.Restore(bytes.NewReader([]byte("definitely not a bolt file"))); err == nil {
		t.Fatal("Restore accepted garbage")
	}
	if _, err := s.HeadBucket(ctx, "keep"); err != nil {
		t.Fatalf("state lost after rejected restore: %v", err)
	}
	if _, err := os.Stat(s.path + ".restore"); !os.IsNotExist(err) {
		t.Fatalf("temporary restore file left behind: %v", err)
	}
}

func TestSnapshotDuringWrites(t *testing.T) {
	s := newStore(t)
	mustCreate(t, s, "b")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			mustPut(t, s, "b", fmt.Sprint("k", i))
		}
	}()
	var snap bytes.Buffer
	if err := s.Snapshot(&snap); err != nil {
		t.Fatal(err)
	}
	<-done
	restored := newStore(t)
	if err := restored.Restore(&snap); err != nil {
		t.Fatalf("snapshot taken during writes is not restorable: %v", err)
	}
	if _, err := restored.HeadBucket(ctx, "b"); err != nil {
		t.Fatalf("restored snapshot lost the bucket: %v", err)
	}
}
