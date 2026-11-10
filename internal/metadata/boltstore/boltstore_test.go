package boltstore

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/metadatatest"
)

var ctx = context.Background()

func newStore(t testing.TB) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustCreate(t testing.TB, s *Store, name string) { metadatatest.MustCreateBucket(t, s, name) }

func mustPut(t testing.TB, s *Store, bucket, key string) metadata.ObjectMeta {
	return metadatatest.MustPut(t, s, bucket, key)
}

func TestConformance(t *testing.T) {
	metadatatest.Run(t, func(t *testing.T) metadata.Metadata { return newStore(t) })
}

func TestPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, s, "b")
	want := mustPut(t, s, "b", "k")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.GetObject(ctx, "b", "k")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("after reopen: %+v, %v", got, err)
	}
}

func TestPrefixEnd(t *testing.T) {
	cases := []struct{ in, want []byte }{
		{[]byte("a"), []byte("b")},
		{[]byte("a/"), []byte("a0")},
		{[]byte{'a', 0xff}, []byte("b")},
		{[]byte{0xff, 0xff}, nil},
		{nil, nil},
	}
	for _, c := range cases {
		if got := prefixEnd(c.in); !slices.Equal(got, c.want) {
			t.Errorf("prefixEnd(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// FuzzListPagination checks the invariant that paginating through any
// listing yields exactly the reference listing: every key or common prefix
// appears exactly once, in order.
func FuzzListPagination(f *testing.F) {
	f.Add("a/b,a/c,b,c/d/e", "", "/", uint8(1), "")
	f.Add("x,xy,xyz", "x", "y", uint8(2), "x")
	f.Add("\U0010FFFF\U0010FFFF,\U0010FFFF,a", "", "\U0010FFFF", uint8(1), "")
	f.Fuzz(func(t *testing.T, keyList, prefix, delim string, maxKeys uint8, startAfter string) {
		s := newStore(t)
		mustCreate(t, s, "b")
		seen := map[string]bool{}
		var keys []string
		for _, k := range strings.Split(keyList, ",") {
			if k == "" || !utf8.ValidString(k) || seen[k] || len(keys) >= 64 {
				continue
			}
			seen[k] = true
			keys = append(keys, k)
			mustPut(t, s, "b", k)
		}
		metadatatest.CheckPagination(t, s, "b", keys, metadata.ListOptions{
			Prefix: prefix, Delimiter: delim, MaxKeys: int(maxKeys%10) + 1, StartAfter: startAfter,
		})
	})
}

func TestCancelledContext(t *testing.T) {
	s := newStore(t)
	c, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.CreateBucket(c, "b"); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := s.ListBuckets(c); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListBuckets: %v", err)
	}
}
