package boltstore

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
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

func mustCreate(t testing.TB, s *Store, name string) {
	t.Helper()
	if err := s.CreateBucket(ctx, name); err != nil {
		t.Fatalf("CreateBucket(%s): %v", name, err)
	}
}

func mustPut(t testing.TB, s *Store, bucket, key string) metadata.ObjectMeta {
	t.Helper()
	obj := metadata.ObjectMeta{
		Bucket: bucket, Key: key, VersionID: "v1", Size: int64(len(key)), ETag: `"e"`,
		LastModified: time.Unix(1700000000, 0).UTC(),
		Chunks: []metadata.Chunk{{Size: int64(len(key)), Scheme: metadata.Single, Shards: []metadata.ShardRef{
			{ID: storage.ComputeShardID([]byte(key)), Size: int64(len(key)), Node: "local"},
		}}},
	}
	if err := s.PutObject(ctx, obj); err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
	return obj
}

func TestBucketLifecycle(t *testing.T) {
	s := newStore(t)
	if _, err := s.HeadBucket(ctx, "b"); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("HeadBucket missing: %v", err)
	}
	mustCreate(t, s, "b2")
	mustCreate(t, s, "b1")
	if err := s.CreateBucket(ctx, "b1"); !errors.Is(err, metadata.ErrBucketAlreadyExists) {
		t.Fatalf("duplicate CreateBucket: %v", err)
	}
	if err := s.CreateBucket(ctx, ""); !errors.Is(err, metadata.ErrInvalidArgument) {
		t.Fatalf("empty CreateBucket: %v", err)
	}
	info, err := s.HeadBucket(ctx, "b1")
	if err != nil || info.Name != "b1" || info.CreatedAt.IsZero() {
		t.Fatalf("HeadBucket = %+v, %v", info, err)
	}
	list, err := s.ListBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Name != "b1" || list[1].Name != "b2" {
		t.Fatalf("ListBuckets = %+v, want b1, b2 sorted", list)
	}

	mustPut(t, s, "b1", "k")
	if err := s.DeleteBucket(ctx, "b1"); !errors.Is(err, metadata.ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket non-empty: %v", err)
	}
	if err := s.DeleteObject(ctx, "b1", "k"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteBucket(ctx, "b1"); err != nil {
		t.Fatalf("DeleteBucket empty: %v", err)
	}
	if err := s.DeleteBucket(ctx, "b1"); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("DeleteBucket twice: %v", err)
	}
	// A recreated bucket must start empty.
	mustCreate(t, s, "b1")
	if res, _ := s.ListObjects(ctx, "b1", metadata.ListOptions{}); len(res.Objects) != 0 {
		t.Fatalf("recreated bucket has %d objects", len(res.Objects))
	}
}

func TestObjectLifecycle(t *testing.T) {
	s := newStore(t)
	if err := s.PutObject(ctx, metadata.ObjectMeta{Bucket: "nope", Key: "k"}); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("PutObject into missing bucket: %v", err)
	}
	mustCreate(t, s, "b")
	for _, k := range []string{"", "bad\xffutf8"} {
		if err := s.PutObject(ctx, metadata.ObjectMeta{Bucket: "b", Key: k}); !errors.Is(err, metadata.ErrInvalidArgument) {
			t.Fatalf("PutObject key %q: %v", k, err)
		}
	}
	if _, err := s.GetObject(ctx, "b", "k"); !errors.Is(err, metadata.ErrNoSuchKey) {
		t.Fatalf("GetObject missing: %v", err)
	}
	if _, err := s.GetObject(ctx, "nope", "k"); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("GetObject missing bucket: %v", err)
	}

	want := mustPut(t, s, "b", "dir/k")
	want.UserMeta = map[string]string{"color": "blue"}
	want.ContentType = "text/plain"
	if err := s.PutObject(ctx, want); err != nil { // overwrite
		t.Fatal(err)
	}
	got, err := s.GetObject(ctx, "b", "dir/k")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetObject round trip:\n got  %+v\n want %+v", got, want)
	}

	if err := s.DeleteObject(ctx, "b", "dir/k"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteObject(ctx, "b", "dir/k"); err != nil {
		t.Fatalf("DeleteObject twice should succeed: %v", err)
	}
	if err := s.DeleteObject(ctx, "nope", "k"); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("DeleteObject missing bucket: %v", err)
	}
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

func TestListObjectsDelimiter(t *testing.T) {
	s := newStore(t)
	mustCreate(t, s, "b")
	for _, k := range []string{"a.txt", "photos/2024/a.jpg", "photos/2024/b.jpg", "photos/2025/c.jpg", "photos/x.jpg", "z"} {
		mustPut(t, s, "b", k)
	}
	cases := []struct {
		name     string
		opts     metadata.ListOptions
		keys     []string
		prefixes []string
	}{
		{"all", metadata.ListOptions{}, []string{"a.txt", "photos/2024/a.jpg", "photos/2024/b.jpg", "photos/2025/c.jpg", "photos/x.jpg", "z"}, nil},
		{"root delimiter", metadata.ListOptions{Delimiter: "/"}, []string{"a.txt", "z"}, []string{"photos/"}},
		{"prefix delimiter", metadata.ListOptions{Prefix: "photos/", Delimiter: "/"}, []string{"photos/x.jpg"}, []string{"photos/2024/", "photos/2025/"}},
		{"prefix only", metadata.ListOptions{Prefix: "photos/2024/"}, []string{"photos/2024/a.jpg", "photos/2024/b.jpg"}, nil},
		{"no match", metadata.ListOptions{Prefix: "q"}, nil, nil},
		{"start after", metadata.ListOptions{StartAfter: "photos/2025/c.jpg"}, []string{"photos/x.jpg", "z"}, nil},
		{"multi-char delimiter", metadata.ListOptions{Delimiter: "20"}, []string{"a.txt", "photos/x.jpg", "z"}, []string{"photos/20"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := s.ListObjects(ctx, "b", c.opts)
			if err != nil {
				t.Fatal(err)
			}
			var keys []string
			for _, o := range res.Objects {
				keys = append(keys, o.Key)
			}
			if !slices.Equal(keys, c.keys) || !slices.Equal(res.CommonPrefixes, c.prefixes) || res.IsTruncated {
				t.Fatalf("got keys=%v prefixes=%v truncated=%v\nwant keys=%v prefixes=%v",
					keys, res.CommonPrefixes, res.IsTruncated, c.keys, c.prefixes)
			}
		})
	}
}

func TestListObjectsErrors(t *testing.T) {
	s := newStore(t)
	if _, err := s.ListObjects(ctx, "nope", metadata.ListOptions{}); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("missing bucket: %v", err)
	}
	mustCreate(t, s, "b")
	for _, tok := range []string{"!!!", "eDp4", "a3g"} { // bad base64, "x:x", "k" without colon
		if _, err := s.ListObjects(ctx, "b", metadata.ListOptions{ContinuationToken: tok}); !errors.Is(err, metadata.ErrInvalidArgument) {
			t.Fatalf("token %q: err = %v, want ErrInvalidArgument", tok, err)
		}
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

// entry is one listing element: a key or a common prefix.
type entry struct {
	prefix bool
	value  string
}

// referenceList computes the full (unpaginated) listing by brute force.
func referenceList(keys []string, opts metadata.ListOptions) []entry {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	var out []entry
	for _, k := range sorted {
		if !strings.HasPrefix(k, opts.Prefix) || k <= opts.StartAfter {
			continue
		}
		if opts.Delimiter != "" {
			if i := strings.Index(k[len(opts.Prefix):], opts.Delimiter); i >= 0 {
				cp := k[:len(opts.Prefix)+i+len(opts.Delimiter)]
				if len(out) == 0 || out[len(out)-1] != (entry{true, cp}) {
					out = append(out, entry{true, cp})
				}
				continue
			}
		}
		out = append(out, entry{false, k})
	}
	return out
}

// paginate lists everything through the store page by page.
func paginate(t testing.TB, s *Store, bucket string, opts metadata.ListOptions) []entry {
	t.Helper()
	var out []entry
	for page := 0; ; page++ {
		if page > 100000 {
			t.Fatal("pagination did not terminate")
		}
		res, err := s.ListObjects(ctx, bucket, opts)
		if err != nil {
			t.Fatal(err)
		}
		// Within a page, S3 interleaves keys and prefixes in sorted order.
		var pageEntries []entry
		for _, o := range res.Objects {
			pageEntries = append(pageEntries, entry{false, o.Key})
		}
		for _, p := range res.CommonPrefixes {
			pageEntries = append(pageEntries, entry{true, p})
		}
		slices.SortFunc(pageEntries, func(a, b entry) int { return strings.Compare(a.value, b.value) })
		if n := len(pageEntries); n > opts.MaxKeys && opts.MaxKeys > 0 {
			t.Fatalf("page has %d entries, max-keys %d", n, opts.MaxKeys)
		}
		out = append(out, pageEntries...)
		if !res.IsTruncated {
			return out
		}
		if res.NextContinuationToken == "" {
			t.Fatal("truncated page without continuation token")
		}
		opts.ContinuationToken = res.NextContinuationToken
	}
}

func checkPagination(t testing.TB, s *Store, bucket string, keys []string, opts metadata.ListOptions) {
	t.Helper()
	want := referenceList(keys, opts)
	got := paginate(t, s, bucket, opts)
	if !slices.Equal(got, want) {
		i := 0
		for i < len(got) && i < len(want) && got[i] == want[i] {
			i++
		}
		t.Fatalf("opts %+v: len(got)=%d len(want)=%d, first difference at %d:\n got  %v\n want %v",
			opts, len(got), len(want), i, got[i:min(i+3, len(got))], want[i:min(i+3, len(want))])
	}
}

func TestListObjectsPaginationMatchesReference(t *testing.T) {
	s := newStore(t)
	mustCreate(t, s, "b")
	rng := rand.New(rand.NewPCG(1, 2))
	alphabet := []string{"a", "b", "/", "-", "\u00ff", "\U0010FFFF"}
	seen := map[string]bool{}
	var keys []string
	for len(keys) < 400 {
		var sb strings.Builder
		for n := 1 + rng.IntN(6); n > 0; n-- {
			sb.WriteString(alphabet[rng.IntN(len(alphabet))])
		}
		k := sb.String()
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
			mustPut(t, s, "b", k)
		}
	}
	for _, prefix := range []string{"", "a", "a/", "b-", "\u00ff", "\U0010FFFF"} {
		for _, delim := range []string{"", "/", "-", "a/"} {
			for _, maxKeys := range []int{1, 2, 7, 1000} {
				for _, startAfter := range []string{"", "a", "b/"} {
					checkPagination(t, s, "b", keys, metadata.ListOptions{
						Prefix: prefix, Delimiter: delim, MaxKeys: maxKeys, StartAfter: startAfter,
					})
				}
			}
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
		checkPagination(t, s, "b", keys, metadata.ListOptions{
			Prefix: prefix, Delimiter: delim, MaxKeys: int(maxKeys%10) + 1, StartAfter: startAfter,
		})
	})
}

func TestWalkChunks(t *testing.T) {
	s := newStore(t)
	mustCreate(t, s, "b1")
	mustCreate(t, s, "b2")
	for i := 0; i < 5; i++ {
		mustPut(t, s, "b1", fmt.Sprint("k", i))
		mustPut(t, s, "b2", fmt.Sprint("k", i))
	}
	n := 0
	err := s.WalkChunks(ctx, func(bucket, key, version string, idx int, c metadata.Chunk) error {
		if c.Shards[0].ID != storage.ComputeShardID([]byte(key)) {
			t.Errorf("chunk for %s/%s has wrong shard", bucket, key)
		}
		n++
		return nil
	})
	if err != nil || n != 10 {
		t.Fatalf("WalkChunks visited %d chunks, err %v; want 10", n, err)
	}
}

func TestNotImplemented(t *testing.T) {
	s := newStore(t)
	if _, err := s.CreateMultipart(ctx, "b", "k", metadata.UploadMeta{}); !errors.Is(err, metadata.ErrNotImplemented) {
		t.Fatal(err)
	}
	if err := s.UpdateShardLocation(ctx, metadata.ShardLocationUpdate{}); !errors.Is(err, metadata.ErrNotImplemented) {
		t.Fatal(err)
	}
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
