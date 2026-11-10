package integration

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Sounabha07/S3-Object-Store/internal/gateway"
	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/sigv4"
)

const (
	authKey    = "test-access-key"
	authSecret = "test-secret-key"
	adminToken = "test-admin-token"
)

// authGateway is a single-node gateway that requires SigV4.
type authGateway struct {
	g   *gateway.Gateway
	srv *httptest.Server
}

func startAuthGateway(t *testing.T, useTLS bool) *authGateway {
	t.Helper()
	g, err := gateway.New(gateway.Config{
		DataDir:     t.TempDir(),
		Credentials: map[string]string{authKey: authSecret, "second-key": "second-secret"},
		AdminToken:  adminToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	root := httpserv.RootWithMetrics(g.Handler, g.Health, g.MetricsHandler())
	var srv *httptest.Server
	if useTLS {
		srv = httptest.NewTLSServer(root)
	} else {
		srv = httptest.NewServer(root)
	}
	t.Cleanup(srv.Close)
	return &authGateway{g: g, srv: srv}
}

func (a *authGateway) client(key, secret string) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(a.srv.URL), Region: "us-east-1", UsePathStyle: true, RetryMaxAttempts: 1,
		Credentials: credentials.NewStaticCredentialsProvider(key, secret, ""),
		HTTPClient:  a.srv.Client(),
	})
}

// TestAuthenticatedSDKOperations runs a full object lifecycle with valid
// credentials, including multipart and a second configured key.
func TestAuthenticatedSDKOperations(t *testing.T) {
	a := startAuthGateway(t, false)
	c := a.client(authKey, authSecret)
	b := newBucket(t, c, "authed")
	data := randomBytes(t, 300_000)
	put(t, c, b, "dir/with space+plus.bin", data)
	if got := get(t, c, b, "dir/with space+plus.bin"); !bytes.Equal(got, data) {
		t.Fatal("content mismatch")
	}
	out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(b), Prefix: aws.String("dir/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(5)})
	if err != nil || len(out.Contents) != 1 {
		t.Fatalf("list: %v", err)
	}
	r, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("dir/with space+plus.bin"), Range: aws.String("bytes=10-19")})
	if err != nil {
		t.Fatalf("ranged GET: %v", err)
	}
	r.Body.Close()

	up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String("mpu")})
	if err != nil {
		t.Fatal(err)
	}
	p := uploadPart(t, c, b, "mpu", aws.ToString(up.UploadId), 1, randomBytes(t, 1000))
	if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(b), Key: aws.String("mpu"), UploadId: up.UploadId, MultipartUpload: completedParts([]uploadedPart{p}),
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	other := a.client("second-key", "second-secret")
	if _, err := other.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("mpu")}); err != nil {
		t.Fatalf("second credential: %v", err)
	}
}

// TestAuthenticatedStreamingUploads: unseekable bodies over TLS use
// aws-chunked with an unsigned trailer (STREAMING-UNSIGNED-PAYLOAD-TRAILER),
// which S3 accepts on a signed request.
func TestAuthenticatedStreamingUploads(t *testing.T) {
	a := startAuthGateway(t, true)
	c := a.client(authKey, authSecret)
	b := newBucket(t, c, "authed-stream")
	data := randomBytes(t, 2<<20)
	_, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b), Key: aws.String("s"), Body: iotest.HalfReader(bytes.NewReader(data)),
		ContentLength: aws.Int64(int64(len(data))), ChecksumAlgorithm: "CRC32",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := get(t, c, b, "s"); !bytes.Equal(got, data) {
		t.Fatal("content mismatch")
	}
}

func TestAuthRejections(t *testing.T) {
	a := startAuthGateway(t, false)
	good := a.client(authKey, authSecret)
	b := newBucket(t, good, "rejections")
	put(t, good, b, "k", []byte("secret data"))

	_, err := a.client(authKey, "wrong-secret").GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("k")})
	wantCode(t, err, "SignatureDoesNotMatch")
	_, err = a.client("unknown-key", authSecret).GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("k")})
	wantCode(t, err, "InvalidAccessKeyId")

	for _, path := range []string{"/", "/" + b, "/" + b + "/k"} {
		resp, err := http.Get(a.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(body), "<Code>AccessDenied</Code>") || strings.Contains(string(body), "secret data") {
			t.Fatalf("anonymous GET %s: %d %s", path, resp.StatusCode, body)
		}
	}
	// Health and metrics stay reachable without credentials.
	for _, path := range []string{httpserv.HealthPath, httpserv.MetricsPath} {
		resp, err := http.Get(a.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
	}
}

func TestPresignedURLs(t *testing.T) {
	a := startAuthGateway(t, false)
	c := a.client(authKey, authSecret)
	b := newBucket(t, c, "presign")
	pc := s3.NewPresignClient(c)

	data := []byte("uploaded through a presigned URL")
	pput, err := pc.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String("p")}, s3.WithPresignExpires(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(pput.Method, pput.URL, bytes.NewReader(data))
	for k, v := range pput.SignedHeader {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("presigned PUT: %d", resp.StatusCode)
	}

	pget, err := pc.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("p")})
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.Get(pget.URL)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("presigned GET: %d %q", resp.StatusCode, got)
	}

	// Pointing a presigned URL at another key breaks the signature.
	resp, err = http.Get(strings.Replace(pget.URL, "/p?", "/other?", 1))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("tampered presigned URL: %d", resp.StatusCode)
	}
}

// chunkSig computes the next signature of a signed aws-chunked body.
func chunkSig(key []byte, date, scope, prev string, data []byte) string {
	emptyHash := sha256.Sum256(nil)
	dataHash := sha256.Sum256(data)
	sts := "AWS4-HMAC-SHA256-PAYLOAD\n" + date + "\n" + scope + "\n" + prev + "\n" + hex.EncodeToString(emptyHash[:]) + "\n" + hex.EncodeToString(dataHash[:])
	m := hmac.New(sha256.New, key)
	m.Write([]byte(sts))
	return hex.EncodeToString(m.Sum(nil))
}

// signedStreamingPut sends a STREAMING-AWS4-HMAC-SHA256-PAYLOAD upload: the
// headers are signed by the aws SDK's own v4 signer and each chunk carries
// a chained signature. If tamper >= 0, that chunk's data is altered after
// signing.
func signedStreamingPut(t *testing.T, url string, data []byte, chunk, tamper int) int {
	t.Helper()
	now := time.Now().UTC()
	date := now.Format("20060102T150405Z")
	scope := now.Format("20060102") + "/us-east-1/s3/aws4_request"

	var chunks [][]byte
	for off := 0; off < len(data); off += chunk {
		chunks = append(chunks, bytes.Clone(data[off:min(off+chunk, len(data))]))
	}
	chunks = append(chunks, nil) // final empty chunk

	// Body length depends only on chunk sizes and the fixed signature length.
	bodyLen := 0
	for _, c := range chunks {
		bodyLen += len(fmt.Sprintf("%x;chunk-signature=%s\r\n", len(c), strings.Repeat("0", 64))) + len(c) + 2
	}
	req, _ := http.NewRequest(http.MethodPut, url, nil)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Content-Sha256", sigv4.StreamingPayload)
	req.Header.Set("X-Amz-Decoded-Content-Length", fmt.Sprint(len(data)))
	req.ContentLength = int64(bodyLen)
	err := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }).SignHTTP(ctx,
		aws.Credentials{AccessKeyID: authKey, SecretAccessKey: authSecret}, req, sigv4.StreamingPayload, "s3", "us-east-1", now)
	if err != nil {
		t.Fatal(err)
	}
	auth := req.Header.Get("Authorization")
	seed := auth[strings.LastIndex(auth, "Signature=")+len("Signature="):]

	key := sigv4.SigningKey(authSecret, scope)
	var body bytes.Buffer
	prev := seed
	for i, c := range chunks {
		sig := chunkSig(key, date, scope, prev, c)
		prev = sig
		if i == tamper && len(c) > 0 {
			c[0] ^= 0xff
		}
		fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(c), sig)
		body.Write(c)
		body.WriteString("\r\n")
	}
	if body.Len() != bodyLen {
		t.Fatalf("body length %d, declared %d", body.Len(), bodyLen)
	}
	req.Body = io.NopCloser(&body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Logf("signed streaming PUT: %d %s", resp.StatusCode, msg)
	}
	return resp.StatusCode
}

func TestSignedStreamingUpload(t *testing.T) {
	a := startAuthGateway(t, false)
	c := a.client(authKey, authSecret)
	b := newBucket(t, c, "signed-stream")
	data := randomBytes(t, 150_000)

	if code := signedStreamingPut(t, a.srv.URL+"/"+b+"/ok", data, 64<<10, -1); code != http.StatusOK {
		t.Fatalf("valid signed streaming upload: HTTP %d", code)
	}
	if got := get(t, c, b, "ok"); !bytes.Equal(got, data) {
		t.Fatal("content mismatch")
	}
	if code := signedStreamingPut(t, a.srv.URL+"/"+b+"/tampered", data, 64<<10, 1); code != http.StatusForbidden {
		t.Fatalf("tampered chunk: HTTP %d, want 403", code)
	}
	_, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("tampered")})
	wantCode(t, err, "NotFound")
}

func TestAdminAPIRequiresToken(t *testing.T) {
	a := startAuthGateway(t, false)
	for _, tc := range []struct {
		header string
		want   int
	}{
		{"", http.StatusForbidden},
		{"Bearer wrong", http.StatusForbidden},
		{"Bearer " + adminToken, http.StatusOK},
	} {
		req, _ := http.NewRequest(http.MethodPost, a.srv.URL+"/_admin/gc", nil)
		if tc.header != "" {
			req.Header.Set("Authorization", tc.header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("admin with %q: %d, want %d", tc.header, resp.StatusCode, tc.want)
		}
	}
}
