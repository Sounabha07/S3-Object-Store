package s3api

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"hash"
	"net/http"

	"errors"
	"fmt"
	"github.com/Sounabha07/S3-Object-Store/internal/sigv4"
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

	// With stream set (authenticated signed streaming upload), every chunk
	// signature and the trailer signature are verified; a mismatch fails
	// the read with a *sigv4.Error.
	stream       *sigv4.Stream
	chunkSig     string
	chunkHash    hash.Hash
	trailerBytes []byte // "name:value\n" lines, for the trailer signature
	trailerSig   string
}

func newChunkedReader(r io.Reader) *chunkedReader {
	return &chunkedReader{r: bufio.NewReaderSize(r, 64<<10), trailers: map[string]string{}}
}

// newSignedChunkedReader decodes and verifies a signed streaming body.
func newSignedChunkedReader(r io.Reader, stream *sigv4.Stream) *chunkedReader {
	c := newChunkedReader(r)
	c.stream = stream
	c.chunkHash = sha256.New()
	return c
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
	if c.stream != nil {
		c.chunkHash.Write(p[:n])
	}
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
		if err := c.verifyChunk(); err != nil {
			c.err = err
			return n, err
		}
	}
	return n, nil
}

// verifyChunk checks the signature of the chunk just completed.
func (c *chunkedReader) verifyChunk() error {
	if c.stream == nil {
		return nil
	}
	if c.chunkSig == "" {
		return &sigv4.Error{Code: "SignatureDoesNotMatch", Message: "chunk without chunk-signature in a signed streaming upload", Status: http.StatusForbidden}
	}
	err := c.stream.VerifyChunk(c.chunkSig, c.chunkHash.Sum(nil))
	c.chunkHash.Reset()
	return err
}

// nextChunk reads a chunk header. For the final zero-size chunk it also
// consumes the trailer section.
func (c *chunkedReader) nextChunk() error {
	line, err := c.readLine()
	if err != nil {
		return err
	}
	sizeStr, ext, _ := strings.Cut(line, ";")
	size, err := strconv.ParseInt(strings.TrimSpace(sizeStr), 16, 64)
	if err != nil || size < 0 {
		return fmt.Errorf("%w: bad chunk size %q", errMalformedChunk, sizeStr)
	}
	c.chunkSig = ""
	if sig, ok := strings.CutPrefix(strings.TrimSpace(ext), "chunk-signature="); ok {
		c.chunkSig = sig
	}
	if size > 0 {
		c.left = size
		return nil
	}
	c.done = true
	if err := c.verifyChunk(); err != nil { // the final, empty chunk is signed too
		return err
	}
	if err := c.readTrailers(); err != nil {
		return err
	}
	if c.stream != nil && c.trailerSig != "" {
		return c.stream.VerifyTrailer(c.trailerSig, c.trailerBytes)
	}
	return nil
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
		name, value = strings.ToLower(strings.TrimSpace(name)), strings.TrimSpace(value)
		if name == "x-amz-trailer-signature" {
			c.trailerSig = value
			continue
		}
		c.trailers[name] = value
		c.trailerBytes = append(c.trailerBytes, name+":"+value+"\n"...)
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
