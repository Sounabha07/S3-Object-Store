// Package integration runs real aws-sdk-go-v2 clients against an in-process
// gateway backed by the real disk store and BoltDB metadata.
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
	"github.com/Sounabha07/S3-Object-Store/internal/s3api"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
)

var ctx = context.Background()

// node is one running single-node gateway.
type node struct {
	t      *testing.T
	dir    string
	srv    *httptest.Server
	meta   *boltstore.Store
	client *s3.Client

	mu           sync.Mutex
	payloadModes map[string]int // x-amz-content-sha256 values seen on PUTs
}

// sawPayloadMode reports whether any PUT declared the given payload mode.
func (n *node) sawPayloadMode(mode string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.payloadModes[mode] > 0
}

// startNode runs a plain-HTTP gateway on dir (created if empty).
func startNode(t *testing.T, dir string) *node { return start(t, dir, false) }

// startTLSNode runs the gateway over HTTPS. The SDK only streams unseekable
// bodies (aws-chunked with a trailing checksum) over TLS.
func startTLSNode(t *testing.T, dir string) *node { return start(t, dir, true) }

func start(t *testing.T, dir string, useTLS bool) *node {
	t.Helper()
	store, err := disk.Open(filepath.Join(dir, "shards"))
	if err != nil {
		t.Fatal(err)
	}
	meta, err := boltstore.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	handler := s3api.New(s3api.Config{
		Meta: meta,
		Data: dataplane.New(store, "local", dataplane.DefaultChunkSize),
	})
	n := &node{t: t, dir: dir, meta: meta, payloadModes: map[string]int{}}
	root := httpserv.Root(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			n.mu.Lock()
			n.payloadModes[r.Header.Get("x-amz-content-sha256")+"|"+r.Header.Get("Content-Encoding")+"|"+r.Header.Get("x-amz-trailer")]++
			n.mu.Unlock()
		}
		handler.ServeHTTP(w, r)
	}), store.Health)
	var srv *httptest.Server
	var client *s3.Client
	if useTLS {
		srv = httptest.NewTLSServer(root)
		client = newClient(srv.URL, func(o *s3.Options) { o.HTTPClient = srv.Client() })
	} else {
		srv = httptest.NewServer(root)
		client = newClient(srv.URL)
	}
	n.srv, n.client = srv, client
	t.Cleanup(n.stop)
	return n
}

func (n *node) stop() {
	if n.srv != nil {
		n.srv.Close()
		_ = n.meta.Close()
		n.srv = nil
	}
}

func newClient(endpoint string, optFns ...func(*s3.Options)) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint:     aws.String(endpoint),
		Region:           "us-east-1",
		UsePathStyle:     true,
		Credentials:      credentials.NewStaticCredentialsProvider("test-access-key", "test-secret-key", ""),
		RetryMaxAttempts: 1,
	}, optFns...)
}

func newBucket(t *testing.T, c *s3.Client, name string) string {
	t.Helper()
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("CreateBucket(%s): %v", name, err)
	}
	return name
}

func randomBytes(t testing.TB, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func put(t *testing.T, c *s3.Client, bucket, key string, data []byte) *s3.PutObjectOutput {
	t.Helper()
	out, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
	return out
}

func get(t *testing.T, c *s3.Client, bucket, key string) []byte {
	t.Helper()
	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject(%s/%s): %v", bucket, key, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read body %s/%s: %v", bucket, key, err)
	}
	return b
}

// errorCode returns the S3 error code of err, or "" if it is not an API error.
func errorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got success", code)
	}
	if got := errorCode(err); got != code {
		t.Fatalf("error code = %q (%v), want %q", got, err, code)
	}
}
