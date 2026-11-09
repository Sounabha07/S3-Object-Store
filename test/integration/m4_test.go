package integration

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
)

func pairs(n int) [][2]int {
	var out [][2]int
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			out = append(out, [2]int{i, j})
		}
	}
	return out
}

// TestErasureCodedObjectsSurviveAnyTwoNodeFailures stores large (erasure
// coded) and small (replicated) objects through S3, then for each of the
// 15 pairs of the 6 storage nodes kills both and reads everything back.
// With 3 nodes down, reads of chunks that lost 3 shards fail with a clean
// 503 instead of wrong data.
func TestErasureCodedObjectsSurviveAnyTwoNodeFailures(t *testing.T) {
	cg := startClusterGateway(t, 6) // 256 KiB chunks, 1 MiB small-object threshold
	b := newBucket(t, cg.client, "ec-pairs")
	objects := map[string][]byte{
		"large-a": randomBytes(t, 2<<20+12345), // EC: 9 chunks
		"large-b": randomBytes(t, 1<<20),       // EC: exactly at the threshold
		"small":   randomBytes(t, 300_000),     // replicated
	}
	for k, v := range objects {
		put(t, cg.client, b, k, v)
	}
	obj, err := cg.g.Meta.GetObject(ctx, b, "large-a")
	if err != nil {
		t.Fatal(err)
	}
	if obj.Chunks[0].Scheme != metadata.EC42 {
		t.Fatalf("large object stored with %+v", obj.Chunks[0].Scheme)
	}

	for _, p := range pairs(6) {
		a, c := cg.sc.nodes[p[0]], cg.sc.nodes[p[1]]
		a.kill()
		c.kill()
		for k, want := range objects {
			if got := get(t, cg.client, b, k); !bytes.Equal(got, want) {
				t.Fatalf("%s and %s down: %s mismatch", a.id, c.id, k)
			}
		}
		a.revive()
		c.revive()
		waitNodeUp(t, cg.g, a)
		waitNodeUp(t, cg.g, c)
	}

	for _, n := range cg.sc.nodes[:3] {
		n.kill()
	}
	_, err = cg.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("large-a")})
	wantCode(t, err, "ServiceUnavailable")
	for _, n := range cg.sc.nodes[:3] {
		n.revive()
	}
}

// TestErasureCodedRangeReads checks ranged GETs that start and end inside
// erasure-coded chunks, with one node down so some reads reconstruct.
func TestErasureCodedRangeReads(t *testing.T) {
	cg := startClusterGateway(t, 6)
	b := newBucket(t, cg.client, "ec-ranges")
	data := randomBytes(t, 3<<20)
	put(t, cg.client, b, "obj", data)
	cg.sc.nodes[2].kill()
	const chunk = 256 << 10
	for _, r := range [][2]int{{0, 0}, {chunk - 1, chunk}, {chunk*3 + 17, chunk*5 - 1}, {len(data) - 9, len(data) - 1}} {
		out, err := cg.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(b), Key: aws.String("obj"), Range: aws.String(fmt.Sprintf("bytes=%d-%d", r[0], r[1])),
		})
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(out.Body)
		out.Body.Close()
		if !bytes.Equal(buf.Bytes(), data[r[0]:r[1]+1]) {
			t.Fatalf("range %v mismatch", r)
		}
	}
}
