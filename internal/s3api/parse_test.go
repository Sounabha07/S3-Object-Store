package s3api

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
)

func TestParseRange(t *testing.T) {
	const size = 100
	cases := []struct {
		header  string
		start   int64
		length  int64
		partial bool
		invalid bool
	}{
		{"", 0, 100, false, false},
		{"bytes=0-0", 0, 1, true, false},
		{"bytes=0-99", 0, 100, true, false},
		{"bytes=10-19", 10, 10, true, false},
		{"bytes=90-", 90, 10, true, false},
		{"bytes=90-1000", 90, 10, true, false}, // end clamped
		{"bytes=-10", 90, 10, true, false},
		{"bytes=-1000", 0, 100, true, false}, // suffix longer than object
		{"bytes=99-99", 99, 1, true, false},
		{" bytes=1-2 ", 1, 2, true, false},
		{"bytes=100-", 0, 0, false, true}, // starts past the end
		{"bytes=100-200", 0, 0, false, true},
		{"bytes=-0", 0, 0, false, true},
		// Malformed or unsupported: ignored, whole object.
		{"bytes=5-1", 0, 100, false, false},
		{"bytes=0-1,5-6", 0, 100, false, false},
		{"bytes=a-b", 0, 100, false, false},
		{"bytes=+1-2", 0, 100, false, false},
		{"bytes=1--2", 0, 100, false, false},
		{"items=0-1", 0, 100, false, false},
		{"bytes=", 0, 100, false, false},
		{"bytes=-", 0, 100, false, false},
		{"bytes=99999999999999999999-", 0, 100, false, false},
	}
	for _, c := range cases {
		r, err := parseRange(c.header, size)
		if c.invalid {
			if err != ErrInvalidRange {
				t.Errorf("%q: err = %v, want InvalidRange", c.header, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: unexpected error %v", c.header, err)
			continue
		}
		if r.start != c.start || r.length != c.length || r.partial != c.partial {
			t.Errorf("%q: got %+v, want start=%d length=%d partial=%v", c.header, r, c.start, c.length, c.partial)
		}
	}
}

func TestParseRangeEmptyObject(t *testing.T) {
	if r, err := parseRange("", 0); err != nil || r.length != 0 || r.partial {
		t.Fatalf("no range on empty object: %+v %v", r, err)
	}
	for _, h := range []string{"bytes=0-", "bytes=0-0", "bytes=-1"} {
		if _, err := parseRange(h, 0); err != ErrInvalidRange {
			t.Fatalf("%q on empty object: err = %v, want InvalidRange", h, err)
		}
	}
}

// FuzzParseRange checks that any header yields either an error or a range
// inside the object.
func FuzzParseRange(f *testing.F) {
	for _, s := range []string{"bytes=0-1", "bytes=-5", "bytes=10-", "bytes=1,2", "garbage"} {
		f.Add(s, int64(50))
	}
	f.Fuzz(func(t *testing.T, h string, size int64) {
		if size < 0 {
			size = -size
		}
		if size < 0 { // MinInt64
			return
		}
		r, err := parseRange(h, size)
		if err != nil {
			if err != ErrInvalidRange {
				t.Fatalf("unexpected error %v", err)
			}
			return
		}
		if r.start < 0 || r.length < 0 || r.start+r.length > size {
			t.Fatalf("range %+v outside object of size %d", r, size)
		}
		if !r.partial && (r.start != 0 || r.length != size) {
			t.Fatalf("non-partial range %+v does not cover object of size %d", r, size)
		}
		if r.partial && r.length == 0 {
			t.Fatalf("empty partial range %+v", r)
		}
	})
}

// encodeChunked produces an aws-chunked body with the given chunk sizes
// (cycled) and trailers.
func encodeChunked(data []byte, sizes []int, trailers map[string]string, signed bool) []byte {
	var b bytes.Buffer
	sig := ""
	if signed {
		sig = ";chunk-signature=" + strings.Repeat("0", 64)
	}
	for i := 0; len(data) > 0; i++ {
		n := min(sizes[i%len(sizes)], len(data))
		fmt.Fprintf(&b, "%x%s\r\n", n, sig)
		b.Write(data[:n])
		b.WriteString("\r\n")
		data = data[n:]
	}
	fmt.Fprintf(&b, "0%s\r\n", sig)
	for k, v := range trailers {
		fmt.Fprintf(&b, "%s:%s\r\n", k, v)
	}
	b.WriteString("\r\n")
	return b.Bytes()
}

func TestChunkedReaderRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for _, size := range []int{0, 1, 100, 8192, 65536, 200000} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		for _, signed := range []bool{false, true} {
			for _, chunkSizes := range [][]int{{1}, {7, 3}, {8192}, {65536}} {
				if size > 20000 && chunkSizes[0] < 100 {
					continue
				}
				trailers := map[string]string{"x-amz-checksum-crc32": "abc="}
				enc := encodeChunked(data, chunkSizes, trailers, signed)
				for _, wrap := range []func(io.Reader) io.Reader{
					func(r io.Reader) io.Reader { return r },
					iotest.OneByteReader,
					iotest.HalfReader,
				} {
					cr := newChunkedReader(wrap(bytes.NewReader(enc)))
					got, err := io.ReadAll(cr)
					if err != nil {
						t.Fatalf("size %d sizes %v signed %v: %v", size, chunkSizes, signed, err)
					}
					if !bytes.Equal(got, data) {
						t.Fatalf("size %d sizes %v: data mismatch", size, chunkSizes)
					}
					if cr.Trailers()["x-amz-checksum-crc32"] != "abc=" {
						t.Fatalf("trailers = %v", cr.Trailers())
					}
				}
			}
		}
	}
}

func TestChunkedReaderMalformed(t *testing.T) {
	cases := map[string]string{
		"bad size":          "zz\r\nabc\r\n0\r\n\r\n",
		"negative size":     "-1\r\n",
		"missing data CRLF": "3\r\nabcX\r\n0\r\n\r\n",
		"truncated data":    "a\r\nabc",
		"no CRLF on line":   "3\nabc\r\n0\r\n\r\n",
		"bad trailer":       "3\r\nabc\r\n0\r\nnocolon\r\n\r\n",
		"empty body":        "",
		"huge line":         strings.Repeat("1", 5000) + "\r\n",
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := io.ReadAll(newChunkedReader(strings.NewReader(in)))
			if err == nil {
				t.Fatal("decoded malformed input without error")
			}
		})
	}
}

func TestChunkedReaderToleratesMissingFinalCRLF(t *testing.T) {
	got, err := io.ReadAll(newChunkedReader(strings.NewReader("3\r\nabc\r\n0\r\n")))
	if err != nil || string(got) != "abc" {
		t.Fatalf("got %q, %v", got, err)
	}
}

// FuzzChunkedReader checks that arbitrary input never panics or yields more
// bytes than it contains, and that valid encodings round-trip.
func FuzzChunkedReader(f *testing.F) {
	f.Add([]byte("3\r\nabc\r\n0\r\n\r\n"), []byte("abc"), uint16(2))
	f.Add([]byte("0;chunk-signature=00\r\nx-amz-checksum-crc32:AAA=\r\n\r\n"), []byte(""), uint16(1))
	f.Fuzz(func(t *testing.T, raw, data []byte, chunk uint16) {
		got, _ := io.ReadAll(newChunkedReader(bytes.NewReader(raw)))
		if len(got) > len(raw) {
			t.Fatalf("decoded %d bytes from %d bytes of input", len(got), len(raw))
		}
		enc := encodeChunked(data, []int{int(chunk%4096) + 1}, nil, chunk%2 == 0)
		got, err := io.ReadAll(newChunkedReader(bytes.NewReader(enc)))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("round trip failed: %v", err)
		}
	})
}

func TestChecksumAlgorithms(t *testing.T) {
	input := []byte("123456789") // the standard CRC check input
	cases := map[string]string{
		"x-amz-checksum-crc32":     "cbf43926",
		"x-amz-checksum-crc32c":    "e3069283",
		"x-amz-checksum-crc64nvme": "ae8b14860a799888",
		"x-amz-checksum-sha1":      "f7c3bc1d808e04732adf679965ccc34ca7ae3441",
		"x-amz-checksum-sha256":    "15e2b0d3c33891ebb0f1ef609ec419420c20e320ce94c65fbc8c3312448eb225",
	}
	for header, wantHex := range cases {
		c := newChecksumCheck(header)
		if c == nil {
			t.Fatalf("%s: not recognised", header)
		}
		c.h.Write(input)
		if got := fmt.Sprintf("%x", c.h.Sum(nil)); got != wantHex {
			t.Errorf("%s = %s, want %s", header, got, wantHex)
		}
	}
	for _, h := range []string{"x-amz-checksum-type", "x-amz-checksum-md5", "content-md5"} {
		if newChecksumCheck(h) != nil {
			t.Errorf("%s should not be a checksum", h)
		}
	}
}

func TestIntegrityVerify(t *testing.T) {
	data := []byte("hello")
	crc := crc32.ChecksumIEEE(data)
	crcB64 := base64.StdEncoding.EncodeToString([]byte{byte(crc >> 24), byte(crc >> 16), byte(crc >> 8), byte(crc)})

	mk := func(h http.Header) *integrity {
		r, _ := http.NewRequest(http.MethodPut, "/b/k", nil)
		r.Header = h
		in, apiErr := parseIntegrity(r)
		if apiErr != nil {
			t.Fatal(apiErr)
		}
		for _, w := range in.writers() {
			w.Write(data)
		}
		return in
	}
	md5 := []byte{0x5d, 0x41, 0x40, 0x2a, 0xbc, 0x4b, 0x2a, 0x76, 0xb9, 0x71, 0x9d, 0x91, 0x10, 0x17, 0xc5, 0x92}

	if err := mk(http.Header{"X-Amz-Checksum-Crc32": {crcB64}}).verify(md5, nil); err != nil {
		t.Errorf("correct crc32 header: %v", err)
	}
	if err := mk(http.Header{"X-Amz-Checksum-Crc32": {"AAAAAA=="}}).verify(md5, nil); err == nil || err.Code != "BadDigest" {
		t.Errorf("wrong crc32 header: %v", err)
	}
	trailer := http.Header{"X-Amz-Trailer": {"x-amz-checksum-crc32"}}
	if err := mk(trailer).verify(md5, map[string]string{"x-amz-checksum-crc32": crcB64}); err != nil {
		t.Errorf("correct trailer: %v", err)
	}
	if err := mk(trailer).verify(md5, nil); err == nil || err.Code != "InvalidRequest" {
		t.Errorf("missing trailer: %v", err)
	}
	if err := mk(http.Header{}).verify(make([]byte, 16), nil); err != nil {
		t.Errorf("no checks: %v", err)
	}
	contentMD5 := http.Header{"Content-Md5": {base64.StdEncoding.EncodeToString(md5)}}
	if err := mk(contentMD5).verify(md5, nil); err != nil {
		t.Errorf("correct Content-MD5: %v", err)
	}
	if err := mk(contentMD5).verify(make([]byte, 16), nil); err != ErrBadDigest {
		t.Errorf("wrong Content-MD5: %v", err)
	}
	sha := http.Header{"X-Amz-Content-Sha256": {strings.Repeat("0", 64)}}
	if err := mk(sha).verify(md5, nil); err != ErrSHA256Mismatch {
		t.Errorf("wrong payload sha256: %v", err)
	}

	r, _ := http.NewRequest(http.MethodPut, "/b/k", nil)
	r.Header.Set("Content-MD5", "not-base64!")
	if _, err := parseIntegrity(r); err != ErrInvalidDigest {
		t.Errorf("malformed Content-MD5: %v", err)
	}
}

func TestValidBucketName(t *testing.T) {
	valid := []string{"abc", "my-bucket", "my.bucket", "a1b2c3", strings.Repeat("a", 63), "123"}
	invalid := []string{
		"", "ab", strings.Repeat("a", 64), "MyBucket", "my_bucket", "-abc", "abc-", ".abc", "abc.",
		"a..b", "a.-b", "a-.b", "192.168.1.1", "xn--abc", "abc-s3alias", "my bucket", "_health",
	}
	for _, n := range valid {
		if !validBucketName(n) {
			t.Errorf("%q should be valid", n)
		}
	}
	for _, n := range invalid {
		if validBucketName(n) {
			t.Errorf("%q should be invalid", n)
		}
	}
}

func TestCheckKey(t *testing.T) {
	if checkKey(strings.Repeat("k", 1024)) != nil {
		t.Error("1024-byte key rejected")
	}
	if checkKey(strings.Repeat("k", 1025)) != ErrKeyTooLong {
		t.Error("1025-byte key accepted")
	}
	if checkKey("bad\xff") == nil {
		t.Error("invalid UTF-8 key accepted")
	}
}

func TestEvaluatePreconditions(t *testing.T) {
	mod := time.Date(2026, 1, 2, 3, 4, 5, 600, time.UTC)
	obj := metadata.ObjectMeta{ETag: `"abc"`, LastModified: mod}
	before := mod.Add(-time.Hour).Format(http.TimeFormat)
	same := mod.Format(http.TimeFormat) // sub-second part is truncated
	after := mod.Add(time.Hour).Format(http.TimeFormat)
	cases := []struct {
		headers map[string]string
		want    int
	}{
		{nil, 0},
		{map[string]string{"If-Match": `"abc"`}, 0},
		{map[string]string{"If-Match": `"x", "abc"`}, 0},
		{map[string]string{"If-Match": "*"}, 0},
		{map[string]string{"If-Match": `"nope"`}, 412},
		{map[string]string{"If-None-Match": `"abc"`}, 304},
		{map[string]string{"If-None-Match": `W/"abc"`}, 304},
		{map[string]string{"If-None-Match": `"nope"`}, 0},
		{map[string]string{"If-Modified-Since": before}, 0},
		{map[string]string{"If-Modified-Since": same}, 304},
		{map[string]string{"If-Modified-Since": after}, 304},
		{map[string]string{"If-Unmodified-Since": before}, 412},
		{map[string]string{"If-Unmodified-Since": same}, 0},
		{map[string]string{"If-Match": `"abc"`, "If-Unmodified-Since": before}, 0}, // If-Match wins
		{map[string]string{"If-None-Match": `"nope"`, "If-Modified-Since": after}, 0},
		{map[string]string{"If-Modified-Since": "garbage"}, 0},
	}
	for _, c := range cases {
		r, _ := http.NewRequest(http.MethodGet, "/b/k", nil)
		for k, v := range c.headers {
			r.Header.Set(k, v)
		}
		if got := evaluatePreconditions(r, obj); got != c.want {
			t.Errorf("%v: got %d, want %d", c.headers, got, c.want)
		}
	}
}

func TestToAPIError(t *testing.T) {
	cases := map[error]*APIError{
		metadata.ErrNoSuchBucket:                      ErrNoSuchBucket,
		fmt.Errorf("wrap: %w", metadata.ErrNoSuchKey): ErrNoSuchKey,
		metadata.ErrBucketAlreadyExists:               ErrBucketAlreadyOwnedByYou,
		metadata.ErrBucketNotEmpty:                    ErrBucketNotEmpty,
		metadata.ErrNotImplemented:                    ErrNotImplemented,
		errors.New("disk on fire"):                    ErrInternal,
		ErrInvalidRange:                               ErrInvalidRange,
	}
	for in, want := range cases {
		if got := toAPIError(in); got.Code != want.Code {
			t.Errorf("toAPIError(%v) = %s, want %s", in, got.Code, want.Code)
		}
	}
}

func TestStripToken(t *testing.T) {
	cases := map[string]string{
		"aws-chunked":       "",
		"aws-chunked,gzip":  "gzip",
		"gzip, aws-chunked": "gzip",
		"gzip":              "gzip",
		"":                  "",
	}
	for in, want := range cases {
		if got := stripToken(in, "aws-chunked"); got != want {
			t.Errorf("stripToken(%q) = %q, want %q", in, got, want)
		}
	}
}
