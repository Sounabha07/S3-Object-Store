package integration

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/raftmeta"
	"github.com/Sounabha07/S3-Object-Store/internal/s3api"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
)

// TestS3SurvivesMetadataLeaderKill drives S3 PUTs through the gateway while
// the Raft metadata leader is killed, then checks every PUT that returned
// 200 is readable with the right content. It always uses Raft, regardless
// of S3OS_IT_META.
func TestS3SurvivesMetadataLeaderKill(t *testing.T) {
	dir := t.TempDir()
	mc := startMetaCluster(t, filepath.Join(dir, "meta"))
	t.Cleanup(func() { _ = mc.stop() })
	store, err := disk.Open(filepath.Join(dir, "shards"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gw := httptest.NewServer(httpserv.Root(s3api.New(s3api.Config{
		Meta: raftmeta.NewClient(mc.urls, nil),
		Data: dataplane.New(store, "local", dataplane.DefaultChunkSize),
	}), store.Health))
	t.Cleanup(gw.Close)
	c := newClient(gw.URL)

	// Wait for an elected leader via a successful bucket creation.
	b := newBucket(t, c, "failover")

	var (
		mu     sync.Mutex
		acked  = map[string][]byte{}
		failed atomic.Int64
		stop   atomic.Bool
		wg     sync.WaitGroup
	)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				key := fmt.Sprintf("w%d/%05d", w, i)
				data := []byte(fmt.Sprintf("payload %s %d", key, time.Now().UnixNano()))
				_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader(data)})
				if err != nil {
					failed.Add(1)
					continue
				}
				mu.Lock()
				acked[key] = data
				mu.Unlock()
			}
		}(w)
	}

	time.Sleep(400 * time.Millisecond)
	leader := -1
	for i, n := range mc.nodes {
		if n.IsLeader() {
			leader = i
		}
	}
	if leader < 0 {
		t.Fatal("no metadata leader found")
	}
	_ = mc.srvs[leader].Close()
	_ = mc.nodes[leader].Shutdown()
	mu.Lock()
	atKill := len(acked)
	mu.Unlock()

	time.Sleep(1500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(acked) <= atKill {
		t.Fatalf("no PUT succeeded after the metadata leader was killed (%d before, %d total)", atKill, len(acked))
	}
	for key, want := range acked {
		if got := get(t, c, b, key); !bytes.Equal(got, want) {
			t.Fatalf("acknowledged object %s has wrong content after failover", key)
		}
	}
	t.Logf("acknowledged PUTs: %d (%d before leader kill), failed PUTs: %d", len(acked), atKill, failed.Load())
}
