package integration

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
)

func TestBucketLifecycle(t *testing.T) {
	c := startNode(t, t.TempDir()).client

	newBucket(t, c, "alpha")
	newBucket(t, c, "beta")

	_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("alpha")})
	wantCode(t, err, "BucketAlreadyOwnedByYou")
	_, err = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("Bad_Name")})
	wantCode(t, err, "InvalidBucketName")

	// A LocationConstraint body is accepted and ignored.
	_, err = c.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                    aws.String("gamma"),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraintEuWest1},
	})
	if err != nil {
		t.Fatalf("CreateBucket with configuration: %v", err)
	}

	list, err := c.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, b := range list.Buckets {
		names = append(names, aws.ToString(b.Name))
		if b.CreationDate == nil || b.CreationDate.IsZero() {
			t.Errorf("bucket %s has no creation date", aws.ToString(b.Name))
		}
	}
	if !slices.Equal(names, []string{"alpha", "beta", "gamma"}) {
		t.Fatalf("ListBuckets = %v", names)
	}

	if _, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("alpha")}); err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}
	_, err = c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("missing")})
	wantCode(t, err, "NotFound")

	loc, err := c.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String("alpha")})
	if err != nil || loc.LocationConstraint != "" {
		t.Fatalf("GetBucketLocation = %q, %v", loc.LocationConstraint, err)
	}

	put(t, c, "alpha", "k", []byte("x"))
	_, err = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String("alpha")})
	wantCode(t, err, "BucketNotEmpty")

	if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("alpha"), Key: aws.String("k")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String("alpha")}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	_, err = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String("alpha")})
	wantCode(t, err, "NoSuchBucket")
	_, err = c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("alpha")})
	wantCode(t, err, "NotFound")
}

func TestPutGetSizes(t *testing.T) {
	c := startNode(t, t.TempDir()).client
	b := newBucket(t, c, "sizes")
	const chunk = dataplane.DefaultChunkSize
	for _, size := range []int{0, 1, 1000, 1 << 20, chunk - 1, chunk, chunk + 1, 2*chunk + 12345} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := randomBytes(t, size)
			key := fmt.Sprintf("obj-%d", size)
			out := put(t, c, b, key, data)
			sum := md5.Sum(data)
			wantETag := `"` + hex.EncodeToString(sum[:]) + `"`
			if aws.ToString(out.ETag) != wantETag {
				t.Fatalf("PutObject ETag = %s, want %s", aws.ToString(out.ETag), wantETag)
			}

			g, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(g.Body)
			g.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("GetObject returned %d bytes, want %d (content mismatch)", len(got), len(data))
			}
			if aws.ToInt64(g.ContentLength) != int64(size) || aws.ToString(g.ETag) != wantETag {
				t.Fatalf("GetObject ContentLength=%d ETag=%s", aws.ToInt64(g.ContentLength), aws.ToString(g.ETag))
			}
		})
	}
}

// TestPutUnseekableBody uploads bodies the SDK cannot rewind. Without an
// explicit checksum algorithm the SDK sends them as UNSIGNED-PAYLOAD with a
// plain body; it only does so over TLS.
func TestPutUnseekableBody(t *testing.T) {
	n := startTLSNode(t, t.TempDir())
	c := n.client
	defer func() {
		if !n.sawPayloadMode("UNSIGNED-PAYLOAD||") {
			t.Errorf("expected an UNSIGNED-PAYLOAD upload; modes seen: %v", n.payloadModes)
		}
	}()
	b := newBucket(t, c, "stream")
	for _, size := range []int{0, 100, 5<<20 + 3} {
		data := randomBytes(t, size)
		key := fmt.Sprint("stream-", size)
		_, err := c.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(b),
			Key:           aws.String(key),
			Body:          iotest.HalfReader(bytes.NewReader(data)), // hides Seek
			ContentLength: aws.Int64(int64(size)),
		})
		if err != nil {
			t.Fatalf("size %d: PutObject: %v", size, err)
		}
		if got := get(t, c, b, key); !bytes.Equal(got, data) {
			t.Fatalf("size %d: content mismatch", size)
		}
	}
}

func TestLargeObjectStreaming(t *testing.T) {
	if testing.Short() {
		t.Skip("64 MiB object")
	}
	c := startNode(t, t.TempDir()).client
	b := newBucket(t, c, "large")
	data := randomBytes(t, 64<<20)
	put(t, c, b, "big", data)
	got := get(t, c, b, "big")
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatal("64 MiB object content mismatch")
	}
}

func TestRangeGets(t *testing.T) {
	c := startNode(t, t.TempDir()).client
	b := newBucket(t, c, "ranges")
	const chunk = dataplane.DefaultChunkSize
	size := 2*chunk + 1000
	data := randomBytes(t, size)
	put(t, c, b, "obj", data)

	cases := []struct {
		rng        string
		start, end int // inclusive
	}{
		{"bytes=0-0", 0, 0},
		{"bytes=0-99", 0, 99},
		{fmt.Sprintf("bytes=%d-%d", chunk-10, chunk+9), chunk - 10, chunk + 9}, // spans a chunk boundary
		{fmt.Sprintf("bytes=%d-", size-5), size - 5, size - 1},
		{"bytes=-7", size - 7, size - 1},
		{fmt.Sprintf("bytes=%d-%d", chunk, 2*chunk-1), chunk, 2*chunk - 1}, // exactly one chunk
		{fmt.Sprintf("bytes=100-%d", size*2), 100, size - 1},               // end past EOF
	}
	for _, tc := range cases {
		t.Run(tc.rng, func(t *testing.T) {
			out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("obj"), Range: aws.String(tc.rng)})
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(out.Body)
			out.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data[tc.start:tc.end+1]) {
				t.Fatalf("got %d bytes, want %d", len(got), tc.end-tc.start+1)
			}
			wantCR := fmt.Sprintf("bytes %d-%d/%d", tc.start, tc.end, size)
			if aws.ToString(out.ContentRange) != wantCR {
				t.Fatalf("Content-Range = %q, want %q", aws.ToString(out.ContentRange), wantCR)
			}
		})
	}

	_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("obj"), Range: aws.String(fmt.Sprintf("bytes=%d-", size))})
	wantCode(t, err, "InvalidRange")

	// A malformed range is ignored and the whole object returned.
	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("obj"), Range: aws.String("bytes=9-3")})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(out.Body)
	out.Body.Close()
	if len(got) != size || out.ContentRange != nil {
		t.Fatalf("malformed range: got %d bytes, Content-Range %v", len(got), out.ContentRange)
	}
}

func TestHeadObjectMetadata(t *testing.T) {
	n := startNode(t, t.TempDir())
	c := n.client
	b := newBucket(t, c, "meta")
	_, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:             aws.String(b),
		Key:                aws.String("doc.txt"),
		Body:               strings.NewReader("hello"),
		ContentType:        aws.String("text/plain; charset=utf-8"),
		CacheControl:       aws.String("max-age=60"),
		ContentDisposition: aws.String(`attachment; filename="doc.txt"`),
		Metadata:           map[string]string{"color": "blue", "Owner": "team-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("doc.txt")})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToInt64(h.ContentLength) != 5 || aws.ToString(h.ContentType) != "text/plain; charset=utf-8" ||
		aws.ToString(h.CacheControl) != "max-age=60" || aws.ToString(h.ContentDisposition) != `attachment; filename="doc.txt"` {
		t.Fatalf("HeadObject headers: len=%d type=%q cache=%q disp=%q", aws.ToInt64(h.ContentLength),
			aws.ToString(h.ContentType), aws.ToString(h.CacheControl), aws.ToString(h.ContentDisposition))
	}
	if h.Metadata["color"] != "blue" || h.Metadata["owner"] != "team-a" {
		t.Fatalf("user metadata = %v", h.Metadata)
	}
	if h.LastModified == nil || aws.ToString(h.AcceptRanges) != "bytes" {
		t.Fatalf("LastModified=%v AcceptRanges=%q", h.LastModified, aws.ToString(h.AcceptRanges))
	}

	// Default content type, and response-* overrides on GET.
	put(t, c, b, "raw", []byte("x"))
	g, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("raw"), ResponseContentType: aws.String("image/png")})
	if err != nil {
		t.Fatal(err)
	}
	g.Body.Close()
	if aws.ToString(g.ContentType) != "image/png" {
		t.Fatalf("response-content-type override = %q", aws.ToString(g.ContentType))
	}
	// The SDK always sends a Content-Type, so check S3's default with raw HTTP.
	req, _ := http.NewRequest(http.MethodPut, n.srv.URL+"/"+b+"/untyped", strings.NewReader("x"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	h, err = c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("untyped")})
	if err != nil || aws.ToString(h.ContentType) != "binary/octet-stream" {
		t.Fatalf("default content type = %q, %v", aws.ToString(h.ContentType), err)
	}
}

func TestMissingResources(t *testing.T) {
	c := startNode(t, t.TempDir()).client
	b := newBucket(t, c, "present")

	_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("nope")})
	wantCode(t, err, "NoSuchKey")
	_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("absent"), Key: aws.String("k")})
	wantCode(t, err, "NoSuchBucket")
	_, err = c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("nope")})
	wantCode(t, err, "NotFound")
	_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("absent"), Key: aws.String("k"), Body: strings.NewReader("x")})
	wantCode(t, err, "NoSuchBucket")
	_, err = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("absent"), Key: aws.String("k")})
	wantCode(t, err, "NoSuchBucket")
	_, err = c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("absent")})
	wantCode(t, err, "NoSuchBucket")

	// Deleting a missing key succeeds, as in S3.
	if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("nope")}); err != nil {
		t.Fatalf("DeleteObject missing key: %v", err)
	}
}

func TestOverwriteAndDelete(t *testing.T) {
	c := startNode(t, t.TempDir()).client
	b := newBucket(t, c, "overwrite")
	put(t, c, b, "k", []byte("first version"))
	put(t, c, b, "k", []byte("second"))
	if got := get(t, c, b, "k"); string(got) != "second" {
		t.Fatalf("after overwrite got %q", got)
	}
	if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("k")}); err != nil {
		t.Fatal(err)
	}
	_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("k")})
	wantCode(t, err, "NoSuchKey")
}

func TestSpecialKeys(t *testing.T) {
	c := startNode(t, t.TempDir()).client
	b := newBucket(t, c, "keys")
	keys := []string{
		"with space", "plus+sign", "percent%20literal", "question?mark", "hash#tag", "amp&and=eq",
		"a//double/slash", "dot/./segment", "dotdot/../segment", "trailing/", "日本語/ファイル.txt",
		"emoji-🎉", "tab\tchar", "semi;colon", "quote\"s", "<angle>", strings.Repeat("k", 1024),
	}
	for _, k := range keys {
		data := []byte("value of " + k)
		put(t, c, b, k, data)
		if got := get(t, c, b, k); !bytes.Equal(got, data) {
			t.Fatalf("key %q: got %q", k, got)
		}
	}
	var listed []string
	p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Contents {
			listed = append(listed, aws.ToString(o.Key))
		}
	}
	want := slices.Clone(keys)
	slices.Sort(want)
	if !slices.Equal(listed, want) {
		t.Fatalf("listed keys:\n got  %q\n want %q", listed, want)
	}

	_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(strings.Repeat("k", 1025)), Body: strings.NewReader("x")})
	wantCode(t, err, "KeyTooLongError")
}

func TestListObjectsV2(t *testing.T) {
	c := startNode(t, t.TempDir()).client
	b := newBucket(t, c, "listing")
	var all []string
	for _, dir := range []string{"logs/2025/", "logs/2026/", "photos/", ""} {
		for i := 0; i < 30; i++ {
			k := fmt.Sprintf("%sfile-%02d", dir, i)
			all = append(all, k)
			put(t, c, b, k, []byte(k))
		}
	}
	slices.Sort(all)

	collect := func(in *s3.ListObjectsV2Input) (keys, prefixes []string, pages int) {
		p := s3.NewListObjectsV2Paginator(c, in)
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			pages++
			if in.MaxKeys != nil && int(aws.ToInt32(page.KeyCount)) > int(*in.MaxKeys) {
				t.Fatalf("page KeyCount %d exceeds MaxKeys %d", aws.ToInt32(page.KeyCount), *in.MaxKeys)
			}
			for _, o := range page.Contents {
				keys = append(keys, aws.ToString(o.Key))
				if aws.ToInt64(o.Size) != int64(len(aws.ToString(o.Key))) || aws.ToString(o.ETag) == "" {
					t.Fatalf("bad entry %+v", o)
				}
			}
			for _, cp := range page.CommonPrefixes {
				prefixes = append(prefixes, aws.ToString(cp.Prefix))
			}
		}
		return
	}

	keys, _, pages := collect(&s3.ListObjectsV2Input{Bucket: aws.String(b), MaxKeys: aws.Int32(7)})
	if !slices.Equal(keys, all) {
		t.Fatalf("paginated listing returned %d keys, want %d", len(keys), len(all))
	}
	if want := (len(all) + 6) / 7; pages != want {
		t.Fatalf("pages = %d, want %d", pages, want)
	}

	keys, prefixes, _ := collect(&s3.ListObjectsV2Input{Bucket: aws.String(b), Delimiter: aws.String("/"), MaxKeys: aws.Int32(4)})
	if len(keys) != 30 || !slices.Equal(prefixes, []string{"logs/", "photos/"}) {
		t.Fatalf("delimiter listing: %d keys, prefixes %v", len(keys), prefixes)
	}

	keys, prefixes, _ = collect(&s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("logs/"), Delimiter: aws.String("/")})
	if len(keys) != 0 || !slices.Equal(prefixes, []string{"logs/2025/", "logs/2026/"}) {
		t.Fatalf("prefix+delimiter: keys %v prefixes %v", keys, prefixes)
	}

	keys, _, _ = collect(&s3.ListObjectsV2Input{Bucket: aws.String(b), StartAfter: aws.String("photos/file-27")})
	if !slices.Equal(keys, []string{"photos/file-28", "photos/file-29"}) {
		t.Fatalf("start-after: %v", keys)
	}

	out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(b), MaxKeys: aws.Int32(0)})
	if err != nil || len(out.Contents) != 0 || aws.ToBool(out.IsTruncated) {
		t.Fatalf("max-keys=0: %d entries truncated=%v err=%v", len(out.Contents), aws.ToBool(out.IsTruncated), err)
	}

	_, err = c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(b), ContinuationToken: aws.String("not-a-token")})
	wantCode(t, err, "InvalidArgument")
}

func TestListEncodingTypeURL(t *testing.T) {
	c := startNode(t, t.TempDir()).client
	b := newBucket(t, c, "enc")
	key := "dir with space/a+b&c.txt"
	put(t, c, b, key, []byte("x"))
	out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(b), EncodingType: types.EncodingTypeUrl})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Contents) != 1 {
		t.Fatalf("got %d objects", len(out.Contents))
	}
	decoded, err := url.QueryUnescape(aws.ToString(out.Contents[0].Key))
	if err != nil || decoded != key {
		t.Fatalf("encoded key %q decodes to %q (%v)", aws.ToString(out.Contents[0].Key), decoded, err)
	}
}

func TestContentMD5(t *testing.T) {
	c := startNode(t, t.TempDir()).client
	b := newBucket(t, c, "md5")
	data := []byte("integrity matters")
	sum := md5.Sum(data)
	good := base64.StdEncoding.EncodeToString(sum[:])
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String("ok"), Body: bytes.NewReader(data), ContentMD5: aws.String(good)}); err != nil {
		t.Fatalf("correct Content-MD5: %v", err)
	}
	bad := base64.StdEncoding.EncodeToString(make([]byte, 16))
	_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String("bad"), Body: bytes.NewReader(data), ContentMD5: aws.String(bad)})
	wantCode(t, err, "BadDigest")
	_, err = c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("bad")})
	wantCode(t, err, "NotFound") // nothing committed
}

func TestFlexibleChecksums(t *testing.T) {
	n := startTLSNode(t, t.TempDir()) // TLS so unseekable bodies use trailers
	c := n.client
	defer func() {
		for _, algo := range []string{"crc32", "crc32c", "crc64nvme", "sha1", "sha256"} {
			mode := "STREAMING-UNSIGNED-PAYLOAD-TRAILER|aws-chunked|x-amz-checksum-" + algo
			if !n.sawPayloadMode(mode) {
				t.Errorf("SDK never sent %s via aws-chunked trailer; modes seen: %v", algo, n.payloadModes)
			}
		}
	}()
	b := newBucket(t, c, "checksums")
	data := randomBytes(t, 300_000)
	for _, algo := range []types.ChecksumAlgorithm{
		types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmCrc64nvme,
		types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256,
	} {
		key := "obj-" + string(algo)
		for name, body := range map[string]io.Reader{"seekable": bytes.NewReader(data), "unseekable": iotest.HalfReader(bytes.NewReader(data))} {
			_, err := c.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(b), Key: aws.String(key), Body: body,
				ContentLength: aws.Int64(int64(len(data))), ChecksumAlgorithm: algo,
			})
			if err != nil {
				t.Fatalf("%s %s: %v", algo, name, err)
			}
			if got := get(t, c, b, key); !bytes.Equal(got, data) {
				t.Fatalf("%s %s: content mismatch", algo, name)
			}
		}
	}
	_, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b), Key: aws.String("wrong"), Body: bytes.NewReader(data),
		ChecksumCRC32: aws.String("AAAAAA=="),
	})
	wantCode(t, err, "BadDigest")
}

func TestConditionalGet(t *testing.T) {
	c := startNode(t, t.TempDir()).client
	b := newBucket(t, c, "cond")
	etag := aws.ToString(put(t, c, b, "k", []byte("data")).ETag)

	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("k"), IfMatch: aws.String(etag)})
	if err != nil {
		t.Fatalf("If-Match current etag: %v", err)
	}
	out.Body.Close()
	_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("k"), IfMatch: aws.String(`"other"`)})
	wantCode(t, err, "PreconditionFailed")
	_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("k"), IfNoneMatch: aws.String(etag)})
	wantCode(t, err, "NotModified")
}

func TestConcurrentClients(t *testing.T) {
	n := startNode(t, t.TempDir())
	b := newBucket(t, n.client, "concurrent")
	const workers, perWorker = 16, 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := newClient(n.srv.URL)
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("w%d/o%d", w, i)
				data := []byte(strings.Repeat(key, 10_000))
				if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader(data)}); err != nil {
					errs <- err
					return
				}
				out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
				if err != nil {
					errs <- err
					return
				}
				got, err := io.ReadAll(out.Body)
				out.Body.Close()
				if err != nil || !bytes.Equal(got, data) {
					errs <- fmt.Errorf("%s: mismatch (%v)", key, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	out, err := n.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
	if err != nil || int(aws.ToInt32(out.KeyCount)) != workers*perWorker {
		t.Fatalf("listed %d objects, want %d (%v)", aws.ToInt32(out.KeyCount), workers*perWorker, err)
	}
}

func TestDataSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	n := startNode(t, dir)
	b := newBucket(t, n.client, "durable")
	data := randomBytes(t, 6<<20)
	put(t, n.client, b, "k", data)
	n.stop()

	n2 := startNode(t, dir)
	if got := get(t, n2.client, b, "k"); !bytes.Equal(got, data) {
		t.Fatal("content mismatch after restart")
	}
}

func TestCorruptShardIsNotServed(t *testing.T) {
	n := startNode(t, t.TempDir())
	b := newBucket(t, n.client, "corrupt")
	data := randomBytes(t, 10_000)
	put(t, n.client, b, "k", data)

	// Flip one byte in every shard file on disk (every replica, on every
	// node, in whichever storage mode the suite runs).
	corruptAllShards(t, n.dir)
	// With every copy corrupt there is nothing to read or reconstruct from,
	// so the gateway must refuse rather than serve bad bytes.
	_, err := n.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("k")})
	wantCode(t, err, "InternalError")
}

func TestUnsupportedOperations(t *testing.T) {
	n := startNode(t, t.TempDir())
	c := n.client
	b := newBucket(t, c, "unsupported")
	put(t, c, b, "src", []byte("x"))

	_, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String(b), Key: aws.String("dst"), CopySource: aws.String(b + "/src")})
	wantCode(t, err, "NotImplemented")
	_, err = c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("mp")})
	wantCode(t, err, "NotImplemented")
	_, err = c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(b)})
	wantCode(t, err, "NotImplemented")
	_, err = c.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String(b)})
	wantCode(t, err, "NotImplemented")

	// Raw HTTP: PutObject without a length is rejected, and POST on an object is not allowed.
	req, _ := http.NewRequest(http.MethodPut, n.srv.URL+"/"+b+"/nolen", io.NopCloser(strings.NewReader("abc")))
	req.ContentLength = -1
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusLengthRequired || !strings.Contains(string(body), "<Code>MissingContentLength</Code>") {
		t.Fatalf("PUT without length: %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "<RequestId>") || resp.Header.Get("x-amz-request-id") == "" {
		t.Fatalf("error response missing request id: %s", body)
	}

	resp, err = http.Post(n.srv.URL+"/"+b+"/src", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST object: %d", resp.StatusCode)
	}
}

// rawChunkedPut sends an aws-chunked PUT by hand, so tests can control the
// framing, signatures and trailers exactly.
func rawChunkedPut(t *testing.T, n *node, path string, data []byte, mode string, trailer, trailerValue string) (int, string) {
	t.Helper()
	sig := ""
	if strings.HasPrefix(mode, "STREAMING-AWS4-HMAC-SHA256") {
		sig = ";chunk-signature=" + strings.Repeat("a", 64)
	}
	var body bytes.Buffer
	for off := 0; off < len(data); off += 1000 {
		end := min(off+1000, len(data))
		fmt.Fprintf(&body, "%x%s\r\n%s\r\n", end-off, sig, data[off:end])
	}
	fmt.Fprintf(&body, "0%s\r\n", sig)
	if trailer != "" {
		fmt.Fprintf(&body, "%s:%s\r\n", trailer, trailerValue)
	}
	body.WriteString("\r\n")

	req, _ := http.NewRequest(http.MethodPut, n.srv.URL+path, &body)
	req.Header.Set("x-amz-content-sha256", mode)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-decoded-content-length", fmt.Sprint(len(data)))
	if trailer != "" {
		req.Header.Set("x-amz-trailer", trailer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestRawAwsChunkedUploads(t *testing.T) {
	n := startNode(t, t.TempDir())
	b := newBucket(t, n.client, "chunked")
	data := randomBytes(t, 5555)
	crc := crc32Base64(data)

	cases := []struct {
		name, mode, trailer, value string
		wantStatus                 int
		wantCode                   string
	}{
		{"signed no trailer", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "", "", 200, ""},
		{"signed with trailer", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", "x-amz-checksum-crc32", crc, 200, ""},
		{"unsigned trailer", "STREAMING-UNSIGNED-PAYLOAD-TRAILER", "x-amz-checksum-crc32", crc, 200, ""},
		{"wrong trailer", "STREAMING-UNSIGNED-PAYLOAD-TRAILER", "x-amz-checksum-crc32", "AAAAAA==", 400, "BadDigest"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := strings.ReplaceAll(tc.name, " ", "-")
			status, body := rawChunkedPut(t, n, "/"+b+"/"+key, data, tc.mode, tc.trailer, tc.value)
			if status != tc.wantStatus || (tc.wantCode != "" && !strings.Contains(body, "<Code>"+tc.wantCode+"</Code>")) {
				t.Fatalf("status %d body %s; want %d %s", status, body, tc.wantStatus, tc.wantCode)
			}
			if tc.wantStatus == 200 {
				if got := get(t, n.client, b, key); !bytes.Equal(got, data) {
					t.Fatal("content mismatch")
				}
			} else {
				_, err := n.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
				wantCode(t, err, "NotFound")
			}
		})
	}
}

func crc32Base64(b []byte) string {
	h := crc32.NewIEEE()
	h.Write(b)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// corruptAllShards flips one byte in every shard file under dir.
func corruptAllShards(t *testing.T, dir string) {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || len(d.Name()) != 64 || filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(p)))) != "shards" {
			return err
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		raw[len(raw)/2] ^= 0x01
		n++
		return os.WriteFile(p, raw, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("found no shard files to corrupt")
	}
}
