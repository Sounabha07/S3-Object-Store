package sigv4

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The examples below are from the Amazon S3 documentation for Signature
// Version 4 ("Examples: Signature Calculations"), which publishes the
// expected signatures for these exact requests.
const (
	exampleKey    = "AKIAIOSFODNN7EXAMPLE"
	exampleSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

var exampleTime = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

func exampleVerifier() *Verifier {
	return &Verifier{Credentials: map[string]string{exampleKey: exampleSecret}, Now: func() time.Time { return exampleTime }}
}

func authHeader(signed, sig string) string {
	return "AWS4-HMAC-SHA256 Credential=" + exampleKey + "/20130524/us-east-1/s3/aws4_request,SignedHeaders=" + signed + ",Signature=" + sig
}

func TestAWSExampleGetObject(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	r.Header.Set("Range", "bytes=0-9")
	r.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	r.Header.Set("X-Amz-Date", "20130524T000000Z")
	r.Header.Set("Authorization", authHeader("host;range;x-amz-content-sha256;x-amz-date",
		"f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))
	res, err := exampleVerifier().Verify(r)
	if err != nil || res.AccessKey != exampleKey {
		t.Fatalf("Verify: %+v, %v", res, err)
	}
}

func TestAWSExamplePutObject(t *testing.T) {
	r := httptest.NewRequest(http.MethodPut, "http://examplebucket.s3.amazonaws.com/test$file.text", strings.NewReader("Welcome to Amazon S3."))
	r.Header.Set("Date", "Fri, 24 May 2013 00:00:00 GMT")
	r.Header.Set("X-Amz-Date", "20130524T000000Z")
	r.Header.Set("X-Amz-Storage-Class", "REDUCED_REDUNDANCY")
	r.Header.Set("X-Amz-Content-Sha256", "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072")
	r.Header.Set("Authorization", authHeader("date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
		"98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"))
	if _, err := exampleVerifier().Verify(r); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestAWSExampleGetBucketLifecycle(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/?lifecycle", nil)
	r.Header.Set("X-Amz-Date", "20130524T000000Z")
	r.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	r.Header.Set("Authorization", authHeader("host;x-amz-content-sha256;x-amz-date",
		"fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"))
	if _, err := exampleVerifier().Verify(r); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestAWSExampleListObjects(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J", nil)
	r.Header.Set("X-Amz-Date", "20130524T000000Z")
	r.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	r.Header.Set("Authorization", authHeader("host;x-amz-content-sha256;x-amz-date",
		"34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"))
	if _, err := exampleVerifier().Verify(r); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestAWSExamplePresignedURL(t *testing.T) {
	u := "http://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
		"&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host" +
		"&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	r := httptest.NewRequest(http.MethodGet, u, nil)
	if _, err := exampleVerifier().Verify(r); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	// One second past expiry.
	v := exampleVerifier()
	v.Now = func() time.Time { return exampleTime.Add(86401 * time.Second) }
	if _, err := v.Verify(r); !hasCode(err, "AccessDenied") {
		t.Fatalf("expired URL: %v", err)
	}
}

// TestAWSExampleChunkedUpload is the "Signature Calculations for the
// Authorization Header: Transferring Payload in Multiple Chunks" example:
// 66,560 bytes of 'a' sent as a 65,536-byte chunk, a 1,024-byte chunk and
// a final empty chunk.
func TestAWSExampleChunkedUpload(t *testing.T) {
	r := httptest.NewRequest(http.MethodPut, "http://s3.amazonaws.com/examplebucket/chunkObject.txt", nil)
	r.Header.Set("X-Amz-Date", "20130524T000000Z")
	r.Header.Set("X-Amz-Storage-Class", "REDUCED_REDUNDANCY")
	r.Header.Set("X-Amz-Content-Sha256", StreamingPayload)
	r.Header.Set("Content-Encoding", "aws-chunked")
	r.Header.Set("X-Amz-Decoded-Content-Length", "66560")
	r.Header.Set("Content-Length", "66824")
	r.Header.Set("Authorization", authHeader(
		"content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class",
		"4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"))
	res, err := exampleVerifier().Verify(r)
	if err != nil {
		t.Fatalf("seed signature: %v", err)
	}
	if res.Stream == nil {
		t.Fatal("no stream verifier for a streaming upload")
	}
	chunks := []struct {
		size int
		sig  string
	}{
		{65536, "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"},
		{1024, "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"},
		{0, "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"},
	}
	for i, c := range chunks {
		sum := sha256.Sum256(bytes.Repeat([]byte("a"), c.size))
		if err := res.Stream.VerifyChunk(c.sig, sum[:]); err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
	}
}

func TestChunkChainRejectsTampering(t *testing.T) {
	sign := func() *Stream {
		return &Stream{key: SigningKey(exampleSecret, "20130524/us-east-1/s3/aws4_request"),
			scope: "20130524/us-east-1/s3/aws4_request", date: "20130524T000000Z", prev: "seed"}
	}
	a, b := sha256.Sum256([]byte("chunk-a")), sha256.Sum256([]byte("chunk-b"))
	signer := sign()
	sigA := hexHMAC(signer, a[:])
	signer.prev = sigA
	sigB := hexHMAC(signer, b[:])

	v := sign()
	if err := v.VerifyChunk(sigA, a[:]); err != nil {
		t.Fatal(err)
	}
	if err := v.VerifyChunk(sigB, b[:]); err != nil {
		t.Fatal(err)
	}
	// Swapped order fails.
	v = sign()
	if err := v.VerifyChunk(sigB, b[:]); err == nil {
		t.Fatal("reordered chunk accepted")
	}
	// Altered data fails.
	v = sign()
	if err := v.VerifyChunk(sigA, b[:]); err == nil {
		t.Fatal("altered chunk accepted")
	}
}

func hexHMAC(s *Stream, sum []byte) string {
	sts := "AWS4-HMAC-SHA256-PAYLOAD\n" + s.date + "\n" + s.scope + "\n" + s.prev + "\n" + emptySHA256 + "\n" + hexString(sum)
	return hexString(hmacSHA256(s.key, []byte(sts)))
}

func hexString(b []byte) string {
	const d = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[2*i], out[2*i+1] = d[c>>4], d[c&15]
	}
	return string(out)
}

func hasCode(err error, code string) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == code
}

func TestRejections(t *testing.T) {
	base := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt", nil)
		r.Header.Set("Range", "bytes=0-9")
		r.Header.Set("X-Amz-Content-Sha256", emptySHA256)
		r.Header.Set("X-Amz-Date", "20130524T000000Z")
		r.Header.Set("Authorization", authHeader("host;range;x-amz-content-sha256;x-amz-date",
			"f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))
		return r
	}
	cases := []struct {
		name   string
		mutate func(*http.Request, *Verifier)
		code   string
	}{
		{"anonymous", func(r *http.Request, _ *Verifier) { r.Header.Del("Authorization") }, "AccessDenied"},
		{"sigv2", func(r *http.Request, _ *Verifier) { r.Header.Set("Authorization", "AWS AKID:sig") }, "AccessDenied"},
		{"unknown key", func(_ *http.Request, v *Verifier) { v.Credentials = map[string]string{"OTHER": "x"} }, "InvalidAccessKeyId"},
		{"wrong secret", func(_ *http.Request, v *Verifier) { v.Credentials[exampleKey] = "nope" }, "SignatureDoesNotMatch"},
		{"tampered header", func(r *http.Request, _ *Verifier) { r.Header.Set("Range", "bytes=0-99") }, "SignatureDoesNotMatch"},
		{"tampered path", func(r *http.Request, _ *Verifier) { r.URL.Path = "/other.txt" }, "SignatureDoesNotMatch"},
		{"tampered method", func(r *http.Request, _ *Verifier) { r.Method = http.MethodDelete }, "SignatureDoesNotMatch"},
		{"clock skew", func(_ *http.Request, v *Verifier) { v.Now = func() time.Time { return exampleTime.Add(time.Hour) } }, "RequestTimeTooSkewed"},
		{"no date", func(r *http.Request, _ *Verifier) { r.Header.Del("X-Amz-Date") }, "AccessDenied"},
		{"host not signed", func(r *http.Request, _ *Verifier) {
			r.Header.Set("Authorization", authHeader("range;x-amz-date", "abc"))
		}, "AuthorizationHeaderMalformed"},
		{"bad credential", func(r *http.Request, _ *Verifier) {
			r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AK/2013/x,SignedHeaders=host,Signature=a")
		}, "AuthorizationHeaderMalformed"},
		{"scope date mismatch", func(r *http.Request, _ *Verifier) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "20130524/", "20130525/", 1))
		}, "AuthorizationHeaderMalformed"},
		{"missing payload hash", func(r *http.Request, _ *Verifier) { r.Header.Del("X-Amz-Content-Sha256") }, "InvalidRequest"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, v := base(), exampleVerifier()
			c.mutate(r, v)
			if _, err := v.Verify(r); !hasCode(err, c.code) {
				t.Fatalf("err = %v, want %s", err, c.code)
			}
		})
	}
}

func TestUriEncode(t *testing.T) {
	cases := map[string]string{
		"/a b/c+d":  "/a%20b/c%2Bd",
		"/test$f":   "/test%24f",
		"/日本":       "/%E6%97%A5%E6%9C%AC",
		"/~-_.ok":   "/~-_.ok",
		"/a//b/../": "/a//b/../",
	}
	for in, want := range cases {
		if got := uriEncode(in, false); got != want {
			t.Errorf("uriEncode(%q) = %q, want %q", in, got, want)
		}
	}
	if got := uriEncode("a/b", true); got != "a%2Fb" {
		t.Errorf("query encoding kept a slash: %q", got)
	}
}

func TestCanonicalQuery(t *testing.T) {
	got := canonicalQuery("prefix=J&max-keys=2&list-type=2&empty&a=%20b+c", false)
	want := "a=%20b%20c&empty=&list-type=2&max-keys=2&prefix=J"
	if got != want {
		t.Fatalf("canonicalQuery = %q, want %q", got, want)
	}
}
