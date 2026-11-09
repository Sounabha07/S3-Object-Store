package storage

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestComputeShardIDKnownVector(t *testing.T) {
	// SHA-256("abc"), FIPS 180-2 test vector.
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got := ComputeShardID([]byte("abc")).String(); got != want {
		t.Fatalf("ComputeShardID(abc) = %s, want %s", got, want)
	}
}

func TestParseShardIDRoundTrip(t *testing.T) {
	id := ComputeShardID([]byte("hello"))
	for _, s := range []string{id.String(), strings.ToUpper(id.String())} {
		got, err := ParseShardID(s)
		if err != nil {
			t.Fatalf("ParseShardID(%q): %v", s, err)
		}
		if got != id {
			t.Fatalf("ParseShardID(%q) = %s, want %s", s, got, id)
		}
	}
}

func TestParseShardIDRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"empty":     "",
		"short":     "abcd",
		"long":      strings.Repeat("a", 65),
		"non-hex":   strings.Repeat("z", 64),
		"path-like": "../" + strings.Repeat("a", 61),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseShardID(in)
			if !errors.Is(err, ErrInvalidShardID) {
				t.Fatalf("ParseShardID(%q) err = %v, want ErrInvalidShardID", in, err)
			}
		})
	}
}

func TestShardIDJSONRoundTrip(t *testing.T) {
	in := ShardInfo{ID: ComputeShardID([]byte("x")), Size: 1}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), in.ID.String()) {
		t.Fatalf("JSON %s does not contain hex id", b)
	}
	var out ShardInfo
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != in.ID || out.Size != in.Size {
		t.Fatalf("round trip mismatch: %+v vs %+v", out, in)
	}
}

func TestIsZero(t *testing.T) {
	if !ZeroShardID.IsZero() {
		t.Fatal("ZeroShardID.IsZero() = false")
	}
	if ComputeShardID(nil).IsZero() {
		t.Fatal("hash of empty input reported as zero")
	}
}
