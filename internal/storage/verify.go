package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
)

// ErrUnavailable means a store could not be reached.
var ErrUnavailable = errors.New("storage: unavailable")

// NewVerifyingReader wraps rc so that reading it to the end checks the
// bytes against id. On mismatch the final Read returns ErrCorrupt instead of
// io.EOF, and every later Read returns the same error.
func NewVerifyingReader(rc io.ReadCloser, id ShardID) io.ReadCloser {
	return &verifyingReader{rc: rc, h: sha256.New(), want: id}
}

type verifyingReader struct {
	rc   io.ReadCloser
	h    hash.Hash
	want ShardID
	err  error
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	if v.err != nil {
		return 0, v.err
	}
	n, err := v.rc.Read(p)
	v.h.Write(p[:n])
	if errors.Is(err, io.EOF) {
		var got ShardID
		v.h.Sum(got[:0])
		if got != v.want {
			v.err = fmt.Errorf("%w: %s", ErrCorrupt, v.want)
			return n, v.err
		}
		v.err = io.EOF
	} else if err != nil {
		v.err = err
	}
	return n, err
}

func (v *verifyingReader) Close() error { return v.rc.Close() }
