// Package metadatatest is a conformance suite for metadata.Metadata. The
// single-node BoltDB store and the Raft cluster client both run it, so the
// gateway sees identical semantics from either backend.
package metadatatest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

var ctx = context.Background()

// Run executes the suite. newMeta must return an empty store.
func Run(t *testing.T, newMeta func(t *testing.T) metadata.Metadata) {
	t.Run("BucketLifecycle", func(t *testing.T) { testBucketLifecycle(t, newMeta(t)) })
	t.Run("ObjectLifecycle", func(t *testing.T) { testObjectLifecycle(t, newMeta(t)) })
	t.Run("ListDelimiter", func(t *testing.T) { testListDelimiter(t, newMeta(t)) })
	t.Run("ListPaginationMatchesReference", func(t *testing.T) { testPaginationReference(t, newMeta(t)) })
	t.Run("ListBadToken", func(t *testing.T) { testBadToken(t, newMeta(t)) })
	t.Run("WalkChunks", func(t *testing.T) { testWalkChunks(t, newMeta(t)) })
	t.Run("NotImplemented", func(t *testing.T) { testNotImplemented(t, newMeta(t)) })
}

// Object returns a minimal object for bucket/key with one single-copy chunk.
func Object(bucket, key string) metadata.ObjectMeta {
	return metadata.ObjectMeta{
		Bucket: bucket, Key: key, VersionID: "v-" + key, Size: int64(len(key)), ETag: `"e"`,
		ContentType:  "binary/octet-stream",
		LastModified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Chunks: []metadata.Chunk{{Size: int64(len(key)), Scheme: metadata.Single, Shards: []metadata.ShardRef{
			{ID: storage.ComputeShardID([]byte(key)), Size: int64(len(key)), Node: "local"},
		}}},
	}
}

// MustCreateBucket creates a bucket or fails the test.
func MustCreateBucket(t testing.TB, m metadata.Metadata, name string) {
	t.Helper()
	if err := m.CreateBucket(ctx, name); err != nil {
		t.Fatalf("CreateBucket(%s): %v", name, err)
	}
}

// MustPut commits Object(bucket, key) or fails the test.
func MustPut(t testing.TB, m metadata.Metadata, bucket, key string) metadata.ObjectMeta {
	t.Helper()
	obj := Object(bucket, key)
	if err := m.PutObject(ctx, obj); err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
	return obj
}

func testBucketLifecycle(t *testing.T, m metadata.Metadata) {
	if _, err := m.HeadBucket(ctx, "b"); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("HeadBucket missing: %v", err)
	}
	MustCreateBucket(t, m, "b2")
	MustCreateBucket(t, m, "b1")
	if err := m.CreateBucket(ctx, "b1"); !errors.Is(err, metadata.ErrBucketAlreadyExists) {
		t.Fatalf("duplicate CreateBucket: %v", err)
	}
	if err := m.CreateBucket(ctx, ""); !errors.Is(err, metadata.ErrInvalidArgument) {
		t.Fatalf("empty CreateBucket: %v", err)
	}
	info, err := m.HeadBucket(ctx, "b1")
	if err != nil || info.Name != "b1" || info.CreatedAt.IsZero() {
		t.Fatalf("HeadBucket = %+v, %v", info, err)
	}
	list, err := m.ListBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Name != "b1" || list[1].Name != "b2" {
		t.Fatalf("ListBuckets = %+v, want b1, b2 sorted", list)
	}

	MustPut(t, m, "b1", "k")
	if err := m.DeleteBucket(ctx, "b1"); !errors.Is(err, metadata.ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket non-empty: %v", err)
	}
	if err := m.DeleteObject(ctx, "b1", "k"); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteBucket(ctx, "b1"); err != nil {
		t.Fatalf("DeleteBucket empty: %v", err)
	}
	if err := m.DeleteBucket(ctx, "b1"); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("DeleteBucket twice: %v", err)
	}
	MustCreateBucket(t, m, "b1")
	if res, _ := m.ListObjects(ctx, "b1", metadata.ListOptions{}); len(res.Objects) != 0 {
		t.Fatalf("recreated bucket has %d objects", len(res.Objects))
	}
}

func testObjectLifecycle(t *testing.T, m metadata.Metadata) {
	if err := m.PutObject(ctx, Object("nope", "k")); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("PutObject into missing bucket: %v", err)
	}
	MustCreateBucket(t, m, "b")
	for _, k := range []string{"", "bad\xffutf8"} {
		if err := m.PutObject(ctx, Object("b", k)); !errors.Is(err, metadata.ErrInvalidArgument) {
			t.Fatalf("PutObject key %q: %v", k, err)
		}
	}
	if _, err := m.GetObject(ctx, "b", "k"); !errors.Is(err, metadata.ErrNoSuchKey) {
		t.Fatalf("GetObject missing: %v", err)
	}
	if _, err := m.GetObject(ctx, "nope", "k"); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("GetObject missing bucket: %v", err)
	}

	want := MustPut(t, m, "b", "dir/k")
	want.UserMeta = map[string]string{"color": "blue"}
	want.Headers = map[string]string{"Cache-Control": "max-age=1"}
	want.ContentType = "text/plain"
	want.VersionID = "v2"
	if err := m.PutObject(ctx, want); err != nil { // overwrite
		t.Fatal(err)
	}
	got, err := m.GetObject(ctx, "b", "dir/k")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetObject round trip:\n got  %+v\n want %+v", got, want)
	}

	if err := m.DeleteObject(ctx, "b", "dir/k"); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteObject(ctx, "b", "dir/k"); err != nil {
		t.Fatalf("DeleteObject twice should succeed: %v", err)
	}
	if err := m.DeleteObject(ctx, "nope", "k"); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("DeleteObject missing bucket: %v", err)
	}
}

func testListDelimiter(t *testing.T, m metadata.Metadata) {
	MustCreateBucket(t, m, "b")
	for _, k := range []string{"a.txt", "photos/2024/a.jpg", "photos/2024/b.jpg", "photos/2025/c.jpg", "photos/x.jpg", "z"} {
		MustPut(t, m, "b", k)
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
			res, err := m.ListObjects(ctx, "b", c.opts)
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

func testBadToken(t *testing.T, m metadata.Metadata) {
	if _, err := m.ListObjects(ctx, "nope", metadata.ListOptions{}); !errors.Is(err, metadata.ErrNoSuchBucket) {
		t.Fatalf("missing bucket: %v", err)
	}
	MustCreateBucket(t, m, "b")
	for _, tok := range []string{"!!!", "eDp4", "a3g"} { // bad base64, "x:x", "k" without colon
		if _, err := m.ListObjects(ctx, "b", metadata.ListOptions{ContinuationToken: tok}); !errors.Is(err, metadata.ErrInvalidArgument) {
			t.Fatalf("token %q: err = %v, want ErrInvalidArgument", tok, err)
		}
	}
}

func testPaginationReference(t *testing.T, m metadata.Metadata) {
	MustCreateBucket(t, m, "b")
	rng := rand.New(rand.NewPCG(1, 2)) // #nosec G404 -- fixed seed on purpose: reproducible test keys, no security role
	alphabet := []string{"a", "b", "/", "-", "\u00ff", "\U0010FFFF"}
	seen := map[string]bool{}
	var keys []string
	for len(keys) < 150 {
		var sb strings.Builder
		for n := 1 + rng.IntN(5); n > 0; n-- {
			sb.WriteString(alphabet[rng.IntN(len(alphabet))])
		}
		if k := sb.String(); !seen[k] {
			seen[k] = true
			keys = append(keys, k)
			MustPut(t, m, "b", k)
		}
	}
	for _, prefix := range []string{"", "a", "a/", "\u00ff"} {
		for _, delim := range []string{"", "/", "a/"} {
			for _, maxKeys := range []int{1, 7, 1000} {
				for _, startAfter := range []string{"", "b/"} {
					CheckPagination(t, m, "b", keys, metadata.ListOptions{
						Prefix: prefix, Delimiter: delim, MaxKeys: maxKeys, StartAfter: startAfter,
					})
				}
			}
		}
	}
}

func testWalkChunks(t *testing.T, m metadata.Metadata) {
	MustCreateBucket(t, m, "b1")
	MustCreateBucket(t, m, "b2")
	want := map[string]bool{}
	for i := 0; i < 5; i++ {
		for _, b := range []string{"b1", "b2"} {
			k := fmt.Sprint("k", i)
			MustPut(t, m, b, k)
			want[b+"/"+k] = true
		}
	}
	got := map[string]bool{}
	err := m.WalkChunks(ctx, func(bucket, key, version string, idx int, c metadata.Chunk) error {
		if c.Shards[0].ID != storage.ComputeShardID([]byte(key)) || version != "v-"+key || idx != 0 {
			t.Errorf("chunk for %s/%s: version %s index %d", bucket, key, version, idx)
		}
		got[bucket+"/"+key] = true
		return nil
	})
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("WalkChunks visited %v (err %v), want %v", got, err, want)
	}
	stop := errors.New("stop")
	if err := m.WalkChunks(ctx, func(string, string, string, int, metadata.Chunk) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("WalkChunks early stop: %v", err)
	}
}

func testNotImplemented(t *testing.T, m metadata.Metadata) {
	if _, err := m.CreateMultipart(ctx, "b", "k", metadata.UploadMeta{}); !errors.Is(err, metadata.ErrNotImplemented) {
		t.Fatal(err)
	}
	if err := m.UpdateShardLocation(ctx, metadata.ShardLocationUpdate{}); !errors.Is(err, metadata.ErrNotImplemented) {
		t.Fatal(err)
	}
}

// Entry is one listing element: a key or a common prefix.
type Entry struct {
	Prefix bool
	Value  string
}

// ReferenceList computes the full (unpaginated) ListObjectsV2 result by
// brute force.
func ReferenceList(keys []string, opts metadata.ListOptions) []Entry {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	var out []Entry
	for _, k := range sorted {
		if !strings.HasPrefix(k, opts.Prefix) || k <= opts.StartAfter {
			continue
		}
		if opts.Delimiter != "" {
			if i := strings.Index(k[len(opts.Prefix):], opts.Delimiter); i >= 0 {
				cp := k[:len(opts.Prefix)+i+len(opts.Delimiter)]
				if len(out) == 0 || out[len(out)-1] != (Entry{true, cp}) {
					out = append(out, Entry{true, cp})
				}
				continue
			}
		}
		out = append(out, Entry{false, k})
	}
	return out
}

// Paginate lists everything through m page by page.
func Paginate(t testing.TB, m metadata.Metadata, bucket string, opts metadata.ListOptions) []Entry {
	t.Helper()
	var out []Entry
	for page := 0; ; page++ {
		if page > 100000 {
			t.Fatal("pagination did not terminate")
		}
		res, err := m.ListObjects(ctx, bucket, opts)
		if err != nil {
			t.Fatal(err)
		}
		var pageEntries []Entry
		for _, o := range res.Objects {
			pageEntries = append(pageEntries, Entry{false, o.Key})
		}
		for _, p := range res.CommonPrefixes {
			pageEntries = append(pageEntries, Entry{true, p})
		}
		slices.SortFunc(pageEntries, func(a, b Entry) int { return strings.Compare(a.Value, b.Value) })
		if n := len(pageEntries); opts.MaxKeys > 0 && n > opts.MaxKeys {
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

// CheckPagination asserts that paginating yields exactly the reference
// listing: every key or common prefix exactly once, in order.
func CheckPagination(t testing.TB, m metadata.Metadata, bucket string, keys []string, opts metadata.ListOptions) {
	t.Helper()
	want := ReferenceList(keys, opts)
	got := Paginate(t, m, bucket, opts)
	if !slices.Equal(got, want) {
		i := 0
		for i < len(got) && i < len(want) && got[i] == want[i] {
			i++
		}
		t.Fatalf("opts %+v: len(got)=%d len(want)=%d, first difference at %d:\n got  %v\n want %v",
			opts, len(got), len(want), i, got[i:min(i+3, len(got))], want[i:min(i+3, len(want))])
	}
}
