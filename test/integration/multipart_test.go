package integration

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/Sounabha07/S3-Object-Store/internal/gateway"
)

type uploadedPart struct {
	n    int32
	data []byte
	etag string
}

func uploadPart(t *testing.T, c *s3.Client, bucket, key, id string, n int32, data []byte) uploadedPart {
	t.Helper()
	out, err := c.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(id),
		PartNumber: aws.Int32(n), Body: bytes.NewReader(data),
	})
	if err != nil {
		t.Errorf("UploadPart %d: %v", n, err)
		return uploadedPart{}
	}
	return uploadedPart{n: n, data: data, etag: aws.ToString(out.ETag)}
}

func completedParts(parts []uploadedPart) *types.CompletedMultipartUpload {
	out := &types.CompletedMultipartUpload{}
	for _, p := range parts {
		out.Parts = append(out.Parts, types.CompletedPart{PartNumber: aws.Int32(p.n), ETag: aws.String(p.etag)})
	}
	return out
}

// expectedETag computes S3's multipart ETag independently of the server.
func expectedETag(parts []uploadedPart) string {
	var cat []byte
	for _, p := range parts {
		sum := md5.Sum(p.data)
		cat = append(cat, sum[:]...)
	}
	return fmt.Sprintf(`"%x-%d"`, md5.Sum(cat), len(parts))
}

// TestMultipart101PartsOutOfOrder uploads 101 parts concurrently in random
// order across 6 storage nodes, then completes and reads back.
func TestMultipart101PartsOutOfOrder(t *testing.T) {
	cg := startClusterGatewayCfg(t, 6, gateway.Config{MinPartSize: 64 << 10})
	c := cg.client
	b := newBucket(t, c, "mpu")
	created, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b), Key: aws.String("video.bin"), ContentType: aws.String("video/mp4"),
		Metadata: map[string]string{"camera": "a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(created.UploadId)

	const n = 101
	parts := make([]uploadedPart, n)
	work := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				size := 64<<10 + i*1000
				if i == n-1 {
					size = 123 // the last part may be small
				}
				parts[i] = uploadPart(t, c, b, "video.bin", id, int32(i+1), randomBytes(t, size))
			}
		}()
	}
	for _, i := range rand.Perm(n) {
		work <- i
	}
	close(work)
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	var listed []int32
	pager := s3.NewListPartsPaginator(c, &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String("video.bin"), UploadId: aws.String(id), MaxParts: aws.Int32(40)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range page.Parts {
			listed = append(listed, aws.ToInt32(p.PartNumber))
		}
	}
	if len(listed) != n || listed[0] != 1 || listed[n-1] != n {
		t.Fatalf("ListParts returned %d parts", len(listed))
	}

	out, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(b), Key: aws.String("video.bin"), UploadId: aws.String(id), MultipartUpload: completedParts(parts),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := aws.ToString(out.ETag), expectedETag(parts); got != want {
		t.Fatalf("ETag %s, want %s", got, want)
	}

	var whole []byte
	for _, p := range parts {
		whole = append(whole, p.data...)
	}
	if got := get(t, c, b, "video.bin"); !bytes.Equal(got, whole) {
		t.Fatalf("assembled object: %d bytes, want %d (content mismatch)", len(got), len(whole))
	}
	h, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("video.bin")})
	if err != nil || aws.ToInt64(h.ContentLength) != int64(len(whole)) || aws.ToString(h.ContentType) != "video/mp4" || h.Metadata["camera"] != "a" {
		t.Fatalf("HeadObject: %+v, %v", h, err)
	}
	// A range across the boundary between parts 1 and 2.
	r, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("video.bin"), Range: aws.String(fmt.Sprintf("bytes=%d-%d", 64<<10-5, 64<<10+4))})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if !bytes.Equal(got, whole[64<<10-5:64<<10+5]) {
		t.Fatal("range across a part boundary mismatch")
	}
	_, err = c.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(b), Key: aws.String("video.bin"), UploadId: aws.String(id)})
	wantCode(t, err, "NoSuchUpload")
}

// TestMultipartWithS3PartSizes uses the real 5 MiB minimum.
func TestMultipartWithS3PartSizes(t *testing.T) {
	n := startNode(t, t.TempDir())
	c := n.client
	b := newBucket(t, c, "mpu-real")
	create := func(key string) string {
		out, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		return aws.ToString(out.UploadId)
	}
	id := create("ok")
	parts := []uploadedPart{
		uploadPart(t, c, b, "ok", id, 1, randomBytes(t, 5<<20)),
		uploadPart(t, c, b, "ok", id, 2, randomBytes(t, 5<<20)),
		uploadPart(t, c, b, "ok", id, 3, randomBytes(t, 1)),
	}
	out, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("ok"), UploadId: aws.String(id), MultipartUpload: completedParts(parts)})
	if err != nil || aws.ToString(out.ETag) != expectedETag(parts) {
		t.Fatalf("Complete: %v (ETag %s)", err, aws.ToString(out.ETag))
	}
	want := append(append(bytes.Clone(parts[0].data), parts[1].data...), parts[2].data...)
	if got := get(t, c, b, "ok"); !bytes.Equal(got, want) {
		t.Fatal("content mismatch")
	}

	id = create("small")
	small := []uploadedPart{
		uploadPart(t, c, b, "small", id, 1, randomBytes(t, 1<<20)),
		uploadPart(t, c, b, "small", id, 2, randomBytes(t, 10)),
	}
	_, err = c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("small"), UploadId: aws.String(id), MultipartUpload: completedParts(small)})
	wantCode(t, err, "EntityTooSmall")
}

func TestMultipartErrors(t *testing.T) {
	n := startNode(t, t.TempDir())
	c := n.client
	b := newBucket(t, c, "mpu-errors")
	out, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("k")})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(out.UploadId)
	p1 := uploadPart(t, c, b, "k", id, 1, randomBytes(t, 100))
	p2 := uploadPart(t, c, b, "k", id, 2, randomBytes(t, 100))
	complete := func(parts ...types.CompletedPart) error {
		_, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: aws.String(b), Key: aws.String("k"), UploadId: aws.String(id),
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		})
		return err
	}
	part := func(n int32, etag string) types.CompletedPart {
		return types.CompletedPart{PartNumber: aws.Int32(n), ETag: aws.String(etag)}
	}
	wantCode(t, complete(part(2, p2.etag), part(1, p1.etag)), "InvalidPartOrder")
	wantCode(t, complete(part(1, p2.etag)), "InvalidPart")
	wantCode(t, complete(part(9, p1.etag)), "InvalidPart")

	_, err = c.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String("other-key"), UploadId: aws.String(id), PartNumber: aws.Int32(1), Body: strings.NewReader("x")})
	wantCode(t, err, "NoSuchUpload")
	_, err = c.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String("k"), UploadId: aws.String(id), PartNumber: aws.Int32(10001), Body: strings.NewReader("x")})
	wantCode(t, err, "InvalidArgument")

	// Malformed completion body (raw HTTP).
	resp, err := http.Post(n.srv.URL+"/"+b+"/k?uploadId="+id, "application/xml", strings.NewReader("<not-xml"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 400 || !strings.Contains(string(body), "MalformedXML") {
		t.Fatalf("malformed XML: %d %s", resp.StatusCode, body)
	}

	// Replacing a part: the later upload wins.
	p1b := uploadPart(t, c, b, "k", id, 1, randomBytes(t, 200))
	// (Only part 1 is completed: a 200-byte non-final part would be below
	// the real 5 MiB minimum.)
	wantCode(t, complete(part(1, p1.etag)), "InvalidPart") // the old ETag no longer matches
	if err := complete(part(1, p1b.etag)); err != nil {
		t.Fatalf("complete with replaced part: %v", err)
	}
	if got := get(t, c, b, "k"); !bytes.Equal(got, p1b.data) {
		t.Fatal("replaced part not used")
	}

	// Abort.
	out, _ = c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("gone")})
	id = aws.ToString(out.UploadId)
	uploadPart(t, c, b, "gone", id, 1, randomBytes(t, 10))
	if _, err := c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("gone"), UploadId: aws.String(id)}); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	_, err = c.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String("gone"), UploadId: aws.String(id), PartNumber: aws.Int32(2), Body: strings.NewReader("x")})
	wantCode(t, err, "NoSuchUpload")
	_, err = c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("gone")})
	wantCode(t, err, "NotFound")
	_, err = c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String("no-such-bucket"), Key: aws.String("k")})
	wantCode(t, err, "NoSuchBucket")
}
