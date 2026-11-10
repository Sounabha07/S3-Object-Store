package sigv4

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestSignReproducesAWSExample: signing the documented GET Object request
// yields the documented signature.
func TestSignReproducesAWSExample(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	r.Header.Set("Range", "bytes=0-9")
	Sign(r, exampleKey, exampleSecret, "us-east-1", emptySHA256, exampleTime)
	auth := r.Header.Get("Authorization")
	if !strings.HasSuffix(auth, "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41") {
		t.Fatalf("Authorization = %s", auth)
	}
	if !strings.Contains(auth, "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date") {
		t.Fatalf("signed headers: %s", auth)
	}
}

// TestSignVerifyRoundTrip: requests signed by Sign pass the Verifier,
// including paths and queries that need escaping, and fail when altered.
func TestSignVerifyRoundTrip(t *testing.T) {
	v := &Verifier{Credentials: map[string]string{"ak": "sk"}}
	for _, target := range []string{
		"http://gw:9000/bucket/plain.txt",
		"http://gw:9000/bucket/dir/with%20space%2Bplus%24.bin",
		"http://gw:9000/bucket?list-type=2&prefix=a%2Fb&delimiter=%2F&max-keys=7",
		"http://gw:9000/",
	} {
		r := httptest.NewRequest(http.MethodPut, target, nil)
		r.Header.Set("Content-Type", "text/plain")
		Sign(r, "ak", "sk", "us-east-1", UnsignedPayload, time.Now())
		if _, err := v.Verify(r); err != nil {
			t.Fatalf("%s: %v", target, err)
		}
		r.Header.Set("Content-Type", "text/html")
		if _, err := v.Verify(r); !hasCode(err, "SignatureDoesNotMatch") {
			t.Fatalf("%s altered: %v", target, err)
		}
	}
}
