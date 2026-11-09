package storagenode

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/storagetest"
)

var bg = context.Background()

func newNode(t *testing.T) (*Client, *disk.Disk, string) {
	t.Helper()
	dir := t.TempDir()
	d, err := disk.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	srv := httptest.NewServer(httpserv.Root(Handler(d, nil), d.Health))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL, nil), d, dir
}

func shardFile(dir string, id storage.ShardID) string {
	h := id.String()
	return filepath.Join(dir, "shards", h[0:2], h[2:4], h)
}

func TestConformanceOverHTTP(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage { c, _, _ := newNode(t); return c })
}

func TestCorruptShardDetectedByClient(t *testing.T) {
	c, _, dir := newNode(t)
	data := storagetest.RandomBytes(t, 100_000)
	info := storagetest.MustPut(t, c, data)
	p := shardFile(dir, info.ID)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[500] ^= 1
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := storagetest.ReadAll(c, info.ID); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

// TestClientVerifiesIndependently: a node that serves wrong bytes with a
// 200 (for example a buggy or malicious node) is still caught by the client.
func TestClientVerifiesIndependently(t *testing.T) {
	liar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "not the shard you asked for")
	}))
	defer liar.Close()
	c := NewClient(liar.URL, nil)
	if _, err := storagetest.ReadAll(c, storage.ComputeShardID([]byte("real"))); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestPutTooLargeRejected(t *testing.T) {
	c, _, _ := newNode(t)
	_, err := c.Put(bg, io.LimitReader(zeros{}, MaxShardSize+1), storage.ZeroShardID)
	if err == nil || !strings.Contains(err.Error(), "413") {
		t.Fatalf("err = %v, want HTTP 413", err)
	}
}

func TestInvalidIDs(t *testing.T) {
	_, d, _ := newNode(t)
	srv := httptest.NewServer(Handler(d, nil))
	defer srv.Close()
	for _, path := range []string{"/v1/shards/zz", "/v1/shards/..%2F..%2Fetc", "/v1/shards/" + strings.Repeat("a", 63)} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: status %d", path, resp.StatusCode)
		}
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/shards", strings.NewReader("x"))
	req.Header.Set(headerExpected, "nothex")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad expected hash: status %d", resp.StatusCode)
	}
}

func TestUnreachableNode(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	c := NewClient(url, nil)
	if _, err := c.Put(bg, bytes.NewReader([]byte("x")), storage.ZeroShardID); !errors.Is(err, storage.ErrUnavailable) {
		t.Fatalf("Put: %v", err)
	}
	if _, err := c.Get(bg, storage.ComputeShardID(nil)); !errors.Is(err, storage.ErrUnavailable) {
		t.Fatalf("Get: %v", err)
	}
	if err := c.Health(bg); !errors.Is(err, storage.ErrUnavailable) {
		t.Fatalf("Health: %v", err)
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
