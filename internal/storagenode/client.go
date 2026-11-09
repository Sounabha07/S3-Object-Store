package storagenode

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// Client implements storage.Storage against a storage node. Every Get is
// verified against the shard ID on the client side too, so corruption on the
// node, in transit, or in the node's own verification path is caught.
type Client struct {
	base string
	hc   *http.Client
}

var _ storage.Storage = (*Client)(nil)

// NewClient returns a client for the node at base (e.g.
// "http://storage-1:8000"). hc may be nil.
func NewClient(base string, hc *http.Client) *Client {
	if hc == nil {
		hc = DefaultHTTPClient()
	}
	return &Client{base: base, hc: hc}
}

// DefaultHTTPClient has short connect and header timeouts: a dead node must
// be abandoned quickly so a replica elsewhere can be used instead.
func DefaultHTTPClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.ResponseHeaderTimeout = 10 * time.Second
	t.MaxIdleConnsPerHost = 64
	t.DialContext = (&netDialer{timeout: 2 * time.Second}).DialContext
	return &http.Client{Transport: t}
}

// Base returns the node's base URL.
func (c *Client) Base() string { return c.base }

func (c *Client) do(ctx context.Context, method, path string, body io.Reader, hdr http.Header) (*http.Response, error) {
	// #nosec G704 -- base comes from operator config (-storage); path is built from a validated hex ShardID
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := c.hc.Do(req) // #nosec G704 -- request URL as above: operator-configured node, not client input
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %s: %v", storage.ErrUnavailable, c.base, err)
	}
	return resp, nil
}

// statusErr converts a non-2xx response into a storage error.
func statusErr(resp *http.Response) error {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	switch resp.Header.Get(headerCode) {
	case codeNotFound:
		return storage.ErrNotFound
	case codeCorrupt:
		return fmt.Errorf("%w: %s", storage.ErrCorrupt, msg)
	case codeModified:
		return storage.ErrModified
	}
	if resp.StatusCode == http.StatusNotFound {
		return storage.ErrNotFound
	}
	return fmt.Errorf("storagenode: %s: HTTP %d: %s", resp.Request.URL, resp.StatusCode, msg)
}

// Put implements storage.Storage.
func (c *Client) Put(ctx context.Context, r io.Reader, expected storage.ShardID) (storage.ShardInfo, error) {
	hdr := http.Header{"Content-Type": {"application/octet-stream"}}
	if !expected.IsZero() {
		hdr.Set(headerExpected, expected.String())
	}
	resp, err := c.do(ctx, http.MethodPost, "/v1/shards", r, hdr)
	if err != nil {
		return storage.ShardInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return storage.ShardInfo{}, statusErr(resp)
	}
	var info shardInfoJSON
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return storage.ShardInfo{}, fmt.Errorf("storagenode: decode put response: %w", err)
	}
	return storage.ShardInfo{ID: info.ID, Size: info.Size, ModTime: info.ModTime}, nil
}

// Get implements storage.Storage.
func (c *Client) Get(ctx context.Context, id storage.ShardID) (io.ReadCloser, error) {
	resp, err := c.do(ctx, http.MethodGet, "/v1/shards/"+id.String(), nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, statusErr(resp)
	}
	return storage.NewVerifyingReader(&truncationReader{rc: resp.Body}, id), nil
}

// truncationReader turns a body cut short by an aborted connection into
// io.EOF, so the verifying reader reports it as corruption (wrong hash)
// rather than a transport error the caller might retry on the same node.
type truncationReader struct{ rc io.ReadCloser }

func (t *truncationReader) Read(p []byte) (int, error) {
	n, err := t.rc.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, io.EOF
	}
	return n, err
}

func (t *truncationReader) Close() error { return t.rc.Close() }

// Stat implements storage.Storage.
func (c *Client) Stat(ctx context.Context, id storage.ShardID) (storage.ShardInfo, error) {
	resp, err := c.do(ctx, http.MethodHead, "/v1/shards/"+id.String(), nil, nil)
	if err != nil {
		return storage.ShardInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return storage.ShardInfo{}, statusErr(resp)
	}
	ns, _ := strconv.ParseInt(resp.Header.Get(headerModTime), 10, 64)
	return storage.ShardInfo{ID: id, Size: resp.ContentLength, ModTime: time.Unix(0, ns)}, nil
}

// Delete implements storage.Storage.
func (c *Client) Delete(ctx context.Context, id storage.ShardID) error {
	resp, err := c.do(ctx, http.MethodDelete, "/v1/shards/"+id.String(), nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return statusErr(resp)
	}
	return nil
}

// DeleteIfOlder implements storage.ConditionalDeleter; the node applies the
// condition atomically with respect to its own Puts.
func (c *Client) DeleteIfOlder(ctx context.Context, id storage.ShardID, cutoff time.Time) error {
	path := "/v1/shards/" + id.String() + "?older-than=" + strconv.FormatInt(cutoff.UnixNano(), 10)
	resp, err := c.do(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return statusErr(resp)
	}
	return nil
}

var _ storage.ConditionalDeleter = (*Client)(nil)

// Walk implements storage.Storage.
func (c *Client) Walk(ctx context.Context, fn func(storage.ShardInfo) error) error {
	resp, err := c.do(ctx, http.MethodGet, "/v1/shards", nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return statusErr(resp)
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		var rec walkRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return fmt.Errorf("storagenode: walk: %w", err)
		}
		switch {
		case rec.Done:
			return nil
		case rec.Error != "":
			return fmt.Errorf("storagenode: walk on %s: %s", c.base, rec.Error)
		case rec.Shard != nil:
			if err := fn(storage.ShardInfo{ID: rec.Shard.ID, Size: rec.Shard.Size, ModTime: rec.Shard.ModTime}); err != nil {
				return err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("%w: walk on %s interrupted: %v", storage.ErrUnavailable, c.base, err)
	}
	return fmt.Errorf("%w: walk on %s ended without completion marker", storage.ErrUnavailable, c.base)
}

// Health implements storage.Storage.
func (c *Client) Health(ctx context.Context) error {
	resp, err := c.do(ctx, http.MethodGet, "/_health", nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s health: HTTP %d", storage.ErrUnavailable, c.base, resp.StatusCode)
	}
	return nil
}
