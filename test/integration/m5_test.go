package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/gateway"
	"github.com/Sounabha07/S3-Object-Store/internal/gc"
	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/metadatatest"
	"github.com/Sounabha07/S3-Object-Store/internal/repair"
	"github.com/Sounabha07/S3-Object-Store/internal/s3api"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

func adminPost(t *testing.T, base, path string, out any) {
	t.Helper()
	resp, err := http.Post(base+path, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: HTTP %d", path, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

// wipe destroys a storage node's data and brings it back empty.
func (s *storageNode) wipe(t *testing.T) {
	t.Helper()
	s.kill()
	if err := s.disk.Close(); err != nil {
		t.Fatal(err)
	}
	s.disk = nil
	if err := os.RemoveAll(s.dir); err != nil {
		t.Fatal(err)
	}
	s.revive()
}

// TestRepairRestoresFullRedundancyAfterNodeWiped: a storage node loses all
// its data. One repair pass rebuilds everything, after which the data
// survives two further node failures (full redundancy is back).
func TestRepairRestoresFullRedundancyAfterNodeWiped(t *testing.T) {
	cg := startClusterGatewayCfg(t, 6, gateway.Config{MinPartSize: 64 << 10})
	c := cg.client
	b := newBucket(t, c, "repair")
	objects := map[string][]byte{}
	for i := 0; i < 8; i++ {
		objects[fmt.Sprint("ec-", i)] = randomBytes(t, 1<<20+i*1000) // erasure coded
		objects[fmt.Sprint("rep-", i)] = randomBytes(t, 50_000)      // replicated
	}
	for k, v := range objects {
		put(t, c, b, k, v)
	}

	victim := cg.sc.nodes[3]
	victim.wipe(t)
	waitNodeUp(t, cg.g, victim)

	var st repair.Stats
	adminPost(t, cg.srv.URL, "/_admin/repair", &st)
	t.Logf("repair after wiping %s: %+v", victim.id, st)
	if st.ShardsRebuilt == 0 || st.ChunksRepaired != st.ChunksDegraded || st.Unrecoverable != 0 || st.Errors != 0 {
		t.Fatalf("repair stats %+v", st)
	}

	for _, pair := range [][2]int{{0, 1}, {2, 4}, {4, 5}} {
		a, z := cg.sc.nodes[pair[0]], cg.sc.nodes[pair[1]]
		a.kill()
		z.kill()
		for k, want := range objects {
			if got := get(t, c, b, k); !bytes.Equal(got, want) {
				t.Fatalf("%s and %s down after repair: %s mismatch", a.id, z.id, k)
			}
		}
		a.revive()
		z.revive()
		waitNodeUp(t, cg.g, a)
		waitNodeUp(t, cg.g, z)
	}
	adminPost(t, cg.srv.URL, "/_admin/repair", &st)
	if st.ChunksDegraded != 0 {
		t.Fatalf("second pass found degraded chunks: %+v", st)
	}
}

// TestGarbageCollectionReclaimsSpace: overwritten objects, deleted objects
// and aborted uploads leave unreferenced shards; GC removes exactly those.
func TestGarbageCollectionReclaimsSpace(t *testing.T) {
	cg := startClusterGatewayCfg(t, 6, gateway.Config{MinPartSize: 64 << 10, GCGrace: time.Millisecond})
	c := cg.client
	b := newBucket(t, c, "gc-test")
	keep := map[string][]byte{}
	for i := 0; i < 5; i++ {
		k := fmt.Sprint("obj-", i)
		put(t, c, b, k, randomBytes(t, 1<<20))
		keep[k] = randomBytes(t, 1<<20)
		put(t, c, b, k, keep[k]) // overwrite
		put(t, c, b, fmt.Sprint("del-", i), randomBytes(t, 30_000))
		if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String(fmt.Sprint("del-", i))}); err != nil {
			t.Fatal(err)
		}
	}
	up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("aborted")})
	if err != nil {
		t.Fatal(err)
	}
	uploadPart(t, c, b, "aborted", aws.ToString(up.UploadId), 1, randomBytes(t, 100_000))
	if _, err := c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("aborted"), UploadId: up.UploadId}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond) // older than the grace period

	shardFiles := func() int {
		n := 0
		for _, sn := range cg.sc.nodes {
			_ = sn.disk.Walk(ctx, func(storage.ShardInfo) error { n++; return nil })
		}
		return n
	}
	referenced := func() int {
		n := 0
		_ = cg.g.Meta.WalkChunks(ctx, func(_, _, _ string, _ int, ch metadata.Chunk) error {
			for _, s := range ch.Shards {
				if s.Node != "" {
					n++
				}
			}
			return nil
		})
		return n
	}
	before := shardFiles()
	var st gc.Stats
	adminPost(t, cg.srv.URL, "/_admin/gc", &st)
	after := shardFiles()
	t.Logf("gc: %d shard files before, %d after, %d referenced; stats %+v", before, after, referenced(), st)
	if st.Deleted == 0 || after != referenced() || st.Errors != 0 {
		t.Fatalf("after GC: %d files, %d referenced, stats %+v", after, referenced(), st)
	}
	for k, want := range keep {
		if got := get(t, c, b, k); !bytes.Equal(got, want) {
			t.Fatalf("%s damaged by GC", k)
		}
	}
}

// TestListObjectsV2TenThousandKeys lists a 10,000-key bucket through the
// aws SDK paginator with prefixes, delimiters and page sizes, and checks
// each listing against a brute-force reference: every key and common
// prefix exactly once, in order, with no page over max-keys.
func TestListObjectsV2TenThousandKeys(t *testing.T) {
	meta, err := boltstore.OpenWithOptions(filepath.Join(t.TempDir(), "meta.db"), boltstore.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()
	if err := meta.CreateBucket(ctx, "big"); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for d := 0; d < 20; d++ {
		for s := 0; s < 10; s++ {
			for f := 0; f < 50; f++ {
				keys = append(keys, fmt.Sprintf("logs/d%02d/s%d/f%03d.txt", d, s, f))
			}
		}
	}
	for _, k := range keys {
		if err := meta.PutObject(ctx, metadatatest.Object("big", k)); err != nil {
			t.Fatal(err)
		}
	}
	if len(keys) != 10_000 {
		t.Fatalf("%d keys", len(keys))
	}
	srv := httptest.NewServer(httpserv.Root(s3api.New(s3api.Config{
		Meta: meta, Data: dataplane.New(nil, "unused", 0),
	}), nil))
	defer srv.Close()
	c := newClient(srv.URL)

	cases := []metadata.ListOptions{
		{MaxKeys: 1000},
		{MaxKeys: 333},
		{Delimiter: "/"},
		{Prefix: "logs/", Delimiter: "/", MaxKeys: 7},
		{Prefix: "logs/d07/", Delimiter: "/", MaxKeys: 3},
		{Prefix: "logs/d13/s4/", MaxKeys: 11},
		{Prefix: "logs/", Delimiter: "/s", MaxKeys: 50},
		{StartAfter: "logs/d18/s9/f040.txt", MaxKeys: 4},
		{Prefix: "logs/d1", Delimiter: "/", MaxKeys: 2},
	}
	for _, opts := range cases {
		in := &s3.ListObjectsV2Input{Bucket: aws.String("big")}
		if opts.MaxKeys > 0 {
			in.MaxKeys = aws.Int32(int32(opts.MaxKeys))
		}
		if opts.Prefix != "" {
			in.Prefix = aws.String(opts.Prefix)
		}
		if opts.Delimiter != "" {
			in.Delimiter = aws.String(opts.Delimiter)
		}
		if opts.StartAfter != "" {
			in.StartAfter = aws.String(opts.StartAfter)
		}
		var got []metadatatest.Entry
		pages := 0
		p := s3.NewListObjectsV2Paginator(c, in)
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			pages++
			var entries []metadatatest.Entry
			for _, o := range page.Contents {
				entries = append(entries, metadatatest.Entry{Value: aws.ToString(o.Key)})
			}
			for _, cp := range page.CommonPrefixes {
				entries = append(entries, metadatatest.Entry{Prefix: true, Value: aws.ToString(cp.Prefix)})
			}
			if opts.MaxKeys > 0 && len(entries) > opts.MaxKeys {
				t.Fatalf("%+v: page of %d entries", opts, len(entries))
			}
			if int(aws.ToInt32(page.KeyCount)) != len(entries) {
				t.Fatalf("%+v: KeyCount %d for %d entries", opts, aws.ToInt32(page.KeyCount), len(entries))
			}
			slices.SortFunc(entries, func(a, b metadatatest.Entry) int { return strings.Compare(a.Value, b.Value) })
			got = append(got, entries...)
		}
		want := metadatatest.ReferenceList(keys, opts)
		if !slices.Equal(got, want) {
			t.Fatalf("%+v: %d entries over %d pages, want %d", opts, len(got), pages, len(want))
		}
		t.Logf("%+v: %d entries, %d pages", opts, len(got), pages)
	}
}
