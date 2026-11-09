package s3api

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// aws-chunked is the body encoding AWS SDKs use for streaming uploads
// (x-amz-content-sha256: STREAMING-*). The wire format is:
//
//	<hex-size>[;chunk-signature=<sig>]\r\n
//	<data>\r\n
//	... repeated ...
//	0[;chunk-signature=<sig>]\r\n
//	[<trailer-name>:<value>\r\n]*
//	\r\n
//
// Chunk signatures are not verified here; SigV4 arrives in M6.

const (
	maxChunkLine    = 4096     // longest accepted size line or trailer line
	maxTrailerBytes = 16 << 10 // total trailer section limit
)

var errMalformedChunk = errors.New("malformed aws-chunked body")

type chunkedReader struct {
	r        *bufio.Reader
	left     int64 // bytes remaining in the current chunk
	done     bool
	err      error
	trailers map[string]string
}

func newChunkedReader(r io.Reader) *chunkedReader {
	return &chunkedReader{r: bufio.NewReaderSize(r, 64<<10), trailers: map[string]string{}}
}

// Trailers returns the trailing headers (lowercased names). Valid only after
// Read has returned io.EOF.
func (c *chunkedReader) Trailers() map[string]string { return c.trailers }

func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	for c.left == 0 {
		if c.done {
			c.err = io.EOF
			return 0, io.EOF
		}
		if err := c.nextChunk(); err != nil {
			c.err = err
			return 0, err
		}
	}
	if int64(len(p)) > c.left {
		p = p[:c.left]
	}
	n, err := c.r.Read(p)
	c.left -= int64(n)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		c.err = err
		return n, err
	}
	if c.left == 0 {
		if err := c.expectCRLF(); err != nil {
			c.err = err
			return n, err
		}
	}
	return n, nil
}

// nextChunk reads a chunk header. For the final zero-size chunk it also
// consumes the trailer section.
func (c *chunkedReader) nextChunk() error {
	line, err := c.readLine()
	if err != nil {
		return err
	}
	sizeStr, _, _ := strings.Cut(line, ";")
	size, err := strconv.ParseInt(strings.TrimSpace(sizeStr), 16, 64)
	if err != nil || size < 0 {
		return fmt.Errorf("%w: bad chunk size %q", errMalformedChunk, sizeStr)
	}
	if size > 0 {
		c.left = size
		return nil
	}
	c.done = true
	return c.readTrailers()
}

func (c *chunkedReader) readTrailers() error {
	total := 0
	for {
		line, err := c.readLine()
		if errors.Is(err, io.ErrUnexpectedEOF) && total == 0 {
			return nil // tolerate a body that ends right after "0\r\n"
		}
		if err != nil {
			return err
		}
		if line == "" {
			return nil
		}
		total += len(line)
		if total > maxTrailerBytes {
			return fmt.Errorf("%w: trailers too large", errMalformedChunk)
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return fmt.Errorf("%w: bad trailer %q", errMalformedChunk, line)
		}
		c.trailers[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(value)
	}
}

// readLine reads one CRLF-terminated line without the terminator.
func (c *chunkedReader) readLine() (string, error) {
	var buf []byte
	for {
		frag, err := c.r.ReadSlice('\n')
		buf = append(buf, frag...)
		if len(buf) > maxChunkLine {
			return "", fmt.Errorf("%w: line too long", errMalformedChunk)
		}
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			return "", io.ErrUnexpectedEOF
		}
		return "", err
	}
	line, ok := bytes.CutSuffix(buf, []byte("\r\n"))
	if !ok {
		return "", fmt.Errorf("%w: line not CRLF-terminated", errMalformedChunk)
	}
	return string(line), nil
}

func (c *chunkedReader) expectCRLF() error {
	var crlf [2]byte
	if _, err := io.ReadFull(c.r, crlf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if crlf != [2]byte{'\r', '\n'} {
		return fmt.Errorf("%w: missing CRLF after chunk data", errMalformedChunk)
	}
	return nil
}
