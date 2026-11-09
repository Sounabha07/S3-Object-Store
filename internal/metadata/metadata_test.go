package metadata

import (
	"crypto/md5"
	"errors"
	"fmt"
	"testing"
)

func TestSchemes(t *testing.T) {
	cases := []struct {
		name        string
		s           Scheme
		total       int
		replication bool
	}{
		{"replication3", Replication3, 3, true},
		{"ec42", EC42, 6, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.s.Total(); got != c.total {
				t.Errorf("Total() = %d, want %d", got, c.total)
			}
			if got := c.s.IsReplication(); got != c.replication {
				t.Errorf("IsReplication() = %v, want %v", got, c.replication)
			}
		})
	}
}

func TestMultipartETag(t *testing.T) {
	a, b := md5.Sum([]byte("a")), md5.Sum([]byte("b"))
	parts := []CompletedPart{{1, fmt.Sprintf(`"%x"`, a)}, {2, fmt.Sprintf("%x", b)}}
	want := md5.Sum(append(a[:], b[:]...))
	got, err := MultipartETag(parts)
	if err != nil || got != fmt.Sprintf(`"%x-2"`, want) {
		t.Fatalf("MultipartETag = %s, %v", got, err)
	}
	if _, err := MultipartETag([]CompletedPart{{1, `"zz"`}}); !errors.Is(err, ErrInvalidPart) {
		t.Fatalf("bad ETag: %v", err)
	}
}
