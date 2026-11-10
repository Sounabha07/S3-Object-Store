package raftmeta

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
)

// Client implements metadata.Metadata against a raftmeta cluster.
//
// It sends every request to the node it believes is the leader, follows
// NotLeader redirects, and retries with backoff while the cluster has no
// leader, until the context deadline (or DefaultTimeout) expires.
//
// Retrying a write whose first attempt had an unknown outcome is safe for
// PutObject and DeleteObject (idempotent). For CreateBucket and DeleteBucket
// a retry can observe its own earlier success; the client then reports
// success rather than BucketAlreadyExists / NoSuchBucket.
type Client struct {
	endpoints []string
	hc        *http.Client
	leader    atomic.Pointer[string]
	next      atomic.Uint32
	now       func() time.Time

	// DefaultTimeout bounds a call whose context has no deadline.
	DefaultTimeout time.Duration
	// AttemptTimeout bounds one attempt, from dialing until response
	// headers, before the client moves on to another node.
	AttemptTimeout time.Duration
}

var _ metadata.Metadata = (*Client)(nil)

// NewClient returns a client for the given node base URLs. hc may be nil.
func NewClient(endpoints []string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{} // per-attempt and per-call deadlines come from contexts
	}
	c := &Client{
		hc: hc, now: func() time.Time { return time.Now().UTC() },
		DefaultTimeout: 15 * time.Second, AttemptTimeout: 3 * time.Second,
	}
	for _, e := range endpoints {
		if e != "" {
			c.endpoints = append(c.endpoints, e)
		}
	}
	return c
}

func (c *Client) pick() string {
	if l := c.leader.Load(); l != nil && *l != "" {
		return *l
	}
	i := c.next.Add(1)
	return c.endpoints[int(i)%len(c.endpoints)]
}

// setLeader caches endpoint as the leader. It stores a pointer to its own
// copy: storing a pointer to a caller's variable that is later reassigned
// is a data race with concurrent readers.
func (c *Client) setLeader(endpoint string) {
	c.leader.Store(&endpoint)
}

// after returns the endpoint following target, so a failed node is not
// retried immediately. (Choosing by attempt number could pick the same dead
// node twice in a row; benchmark scenario 6 measured that as a ~7.7 s write
// stall after a leader kill.)
func (c *Client) after(target string) string {
	for i, e := range c.endpoints {
		if e == target {
			return c.endpoints[(i+1)%len(c.endpoints)]
		}
	}
	return c.endpoints[0] // target came from a leader hint, not our list
}

func (c *Client) forget(endpoint string) {
	if l := c.leader.Load(); l != nil && *l == endpoint {
		c.leader.CompareAndSwap(l, nil)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// call performs one logical request. handle consumes a 200 response; its
// error is returned as-is and never retried. uncertain reports whether an
// earlier attempt of a write may have been applied.
func (c *Client) call(ctx context.Context, method, path string, q url.Values, body []byte, handle func(*http.Response) error) (uncertain bool, err error) {
	if len(c.endpoints) == 0 {
		return false, fmt.Errorf("%w: no metadata endpoints configured", metadata.ErrUnavailable)
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.DefaultTimeout)
		defer cancel()
	}
	isWrite := method == http.MethodPost
	target := c.pick()
	backoff := 20 * time.Millisecond
	wait := func() {
		sleep(ctx, backoff)
		backoff = min(backoff*2, 500*time.Millisecond)
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return uncertain, fmt.Errorf("%w: %v (last error: %v)", metadata.ErrUnavailable, ctx.Err(), lastErr)
		}
		u := target + path
		if len(q) > 0 {
			u += "?" + q.Encode()
		}
		var rd io.Reader
		if body != nil {
			rd = bytes.NewReader(body)
		}
		// Each attempt must get response headers within AttemptTimeout. A
		// crashed host often does not refuse connections (packets are
		// dropped), and without this one dial could use up the whole call
		// deadline instead of moving on to a live node. The timer stops
		// once headers arrive, so long response streams are not cut off.
		actx, cancelAttempt := context.WithCancel(ctx)
		req, err := http.NewRequestWithContext(actx, method, u, rd)
		if err != nil {
			cancelAttempt()
			return uncertain, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		timer := time.AfterFunc(c.AttemptTimeout, cancelAttempt)
		resp, err := c.hc.Do(req)
		if !timer.Stop() && err == nil {
			_ = resp.Body.Close() // headers arrived just as the timer fired
			err = errors.New("attempt timed out")
		}
		if err != nil {
			cancelAttempt()
			// The request may have reached the leader before failing.
			uncertain = uncertain || isWrite
			lastErr = fmt.Errorf("%s: %w", target, err)
			c.forget(target)
			target = c.after(target)
			wait()
			continue
		}
		if resp.StatusCode == http.StatusOK {
			c.setLeader(target) // only the leader answers 200
			err := handle(resp)
			_ = resp.Body.Close()
			cancelAttempt()
			return uncertain, err
		}

		var we wireError
		_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&we)
		_ = resp.Body.Close()
		cancelAttempt()
		lastErr = fmt.Errorf("%s: %s", we.Code, we.Message)
		switch we.Code {
		case codeNotLeader:
			c.forget(target)
			if we.Leader != "" && we.Leader != target {
				target = we.Leader
				c.setLeader(target)
				if attempt > 3 {
					wait() // nodes disagree about the leader; let an election settle
				}
				continue
			}
			target = c.after(target)
			wait()
		case codeOutcomeUnknown:
			uncertain = true
			c.forget(target)
			wait()
			target = c.pick()
		case "Unavailable", "":
			wait()
		default:
			if sentinel := codeError(we.Code); sentinel != nil {
				return uncertain, fmt.Errorf("%w: %s", sentinel, we.Message)
			}
			return uncertain, fmt.Errorf("raftmeta: %s", lastErr)
		}
	}
}

func decodeInto(v any) func(*http.Response) error {
	return func(resp *http.Response) error { return json.NewDecoder(resp.Body).Decode(v) }
}

func (c *Client) apply(ctx context.Context, cmd boltstore.Command) (uncertain bool, err error) {
	if err := cmd.Validate(); err != nil {
		return false, err
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		return false, err
	}
	return c.call(ctx, http.MethodPost, "/v1/apply", nil, body, func(*http.Response) error { return nil })
}

// CreateBucket implements metadata.Metadata.
func (c *Client) CreateBucket(ctx context.Context, name string) error {
	uncertain, err := c.apply(ctx, boltstore.Command{Op: boltstore.OpCreateBucket, Bucket: name, Time: c.now()})
	if uncertain && errors.Is(err, metadata.ErrBucketAlreadyExists) {
		return nil // our earlier attempt created it
	}
	return err
}

// DeleteBucket implements metadata.Metadata.
func (c *Client) DeleteBucket(ctx context.Context, name string) error {
	if name == "" {
		return metadata.ErrNoSuchBucket
	}
	uncertain, err := c.apply(ctx, boltstore.Command{Op: boltstore.OpDeleteBucket, Bucket: name})
	if uncertain && errors.Is(err, metadata.ErrNoSuchBucket) {
		return nil // our earlier attempt deleted it
	}
	return err
}

// PutObject implements metadata.Metadata.
func (c *Client) PutObject(ctx context.Context, obj metadata.ObjectMeta) error {
	_, err := c.apply(ctx, boltstore.Command{Op: boltstore.OpPutObject, Object: &obj})
	return err
}

// DeleteObject implements metadata.Metadata.
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	if bucket == "" {
		return metadata.ErrNoSuchBucket
	}
	_, err := c.apply(ctx, boltstore.Command{Op: boltstore.OpDeleteObject, Bucket: bucket, Key: key})
	return err
}

// HeadBucket implements metadata.Metadata.
func (c *Client) HeadBucket(ctx context.Context, name string) (metadata.BucketInfo, error) {
	var out metadata.BucketInfo
	_, err := c.call(ctx, http.MethodGet, "/v1/bucket", url.Values{"name": {name}}, nil, decodeInto(&out))
	return out, err
}

// ListBuckets implements metadata.Metadata.
func (c *Client) ListBuckets(ctx context.Context) ([]metadata.BucketInfo, error) {
	var out []metadata.BucketInfo
	_, err := c.call(ctx, http.MethodGet, "/v1/buckets", nil, nil, decodeInto(&out))
	return out, err
}

// GetObject implements metadata.Metadata.
func (c *Client) GetObject(ctx context.Context, bucket, key string) (metadata.ObjectMeta, error) {
	var out metadata.ObjectMeta
	_, err := c.call(ctx, http.MethodGet, "/v1/object", url.Values{"bucket": {bucket}, "key": {key}}, nil, decodeInto(&out))
	return out, err
}

// ListObjects implements metadata.Metadata.
func (c *Client) ListObjects(ctx context.Context, bucket string, opts metadata.ListOptions) (metadata.ListResult, error) {
	q := url.Values{"bucket": {bucket}}
	for k, v := range map[string]string{"prefix": opts.Prefix, "delimiter": opts.Delimiter,
		"start-after": opts.StartAfter, "token": opts.ContinuationToken} {
		if v != "" {
			q.Set(k, v)
		}
	}
	if opts.MaxKeys != 0 {
		q.Set("max-keys", strconv.Itoa(opts.MaxKeys))
	}
	var out metadata.ListResult
	_, err := c.call(ctx, http.MethodGet, "/v1/objects", q, nil, decodeInto(&out))
	return out, err
}

// WalkChunks implements metadata.Metadata. It streams; a stream cut short
// is reported as an error rather than silently ending the walk.
func (c *Client) WalkChunks(ctx context.Context, fn func(bucket, key, versionID string, chunkIndex int, c metadata.Chunk) error) error {
	return c.streamChunks(ctx, "/v1/chunks", func(r chunkRecord) error {
		return fn(r.Bucket, r.Key, r.VersionID, r.ChunkIndex, *r.Chunk)
	})
}

// WalkPartChunks implements metadata.Metadata.
func (c *Client) WalkPartChunks(ctx context.Context, fn func(uploadID string, partNumber int, c metadata.Chunk) error) error {
	return c.streamChunks(ctx, "/v1/partchunks", func(r chunkRecord) error {
		return fn(r.UploadID, r.PartNumber, *r.Chunk)
	})
}

func (c *Client) streamChunks(ctx context.Context, path string, fn func(chunkRecord) error) error {
	_, err := c.call(ctx, http.MethodGet, path, nil, nil, func(resp *http.Response) error {
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64<<10), maxCommandBytes)
		for sc.Scan() {
			var rec chunkRecord
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				return fmt.Errorf("raftmeta: chunk stream: %w", err)
			}
			switch {
			case rec.Done:
				return nil
			case rec.Error != nil:
				if sentinel := codeError(rec.Error.Code); sentinel != nil {
					return fmt.Errorf("%w: %s", sentinel, rec.Error.Message)
				}
				return fmt.Errorf("raftmeta: chunk stream: %s", rec.Error.Message)
			case rec.Chunk != nil:
				if err := fn(rec); err != nil {
					return err
				}
			}
		}
		if err := sc.Err(); err != nil {
			return fmt.Errorf("%w: chunk stream interrupted: %v", metadata.ErrUnavailable, err)
		}
		return fmt.Errorf("%w: chunk stream ended without completion marker", metadata.ErrUnavailable)
	})
	return err
}

// CreateMultipart implements metadata.Metadata. The upload ID is chosen
// here, so a retry after an unknown outcome can recognise its own upload.
func (c *Client) CreateMultipart(ctx context.Context, bucket, key string, m metadata.UploadMeta) (string, error) {
	id := boltstore.NewUploadID()
	uncertain, err := c.apply(ctx, boltstore.Command{Op: boltstore.OpCreateMultipart, Bucket: bucket, Key: key, UploadID: id, Time: c.now(), Upload: &m})
	if err != nil && uncertain && errors.Is(err, metadata.ErrInvalidArgument) {
		if up, gerr := c.GetUpload(ctx, id); gerr == nil && up.Bucket == bucket && up.Key == key {
			return id, nil // our earlier attempt created it
		}
	}
	if err != nil {
		return "", err
	}
	return id, nil
}

// GetUpload implements metadata.Metadata.
func (c *Client) GetUpload(ctx context.Context, uploadID string) (metadata.UploadInfo, error) {
	var out metadata.UploadInfo
	_, err := c.call(ctx, http.MethodGet, "/v1/upload", url.Values{"id": {uploadID}}, nil, decodeInto(&out))
	return out, err
}

// PutPart implements metadata.Metadata. Recording the same part twice is
// harmless, so retries need no special handling.
func (c *Client) PutPart(ctx context.Context, uploadID string, p metadata.PartMeta) error {
	_, err := c.apply(ctx, boltstore.Command{Op: boltstore.OpPutPart, UploadID: uploadID, Part: &p})
	return err
}

// ListParts implements metadata.Metadata.
func (c *Client) ListParts(ctx context.Context, uploadID string, opts metadata.ListPartsOptions) (metadata.ListPartsResult, error) {
	q := url.Values{"upload": {uploadID}}
	if opts.MaxParts > 0 {
		q.Set("max", strconv.Itoa(opts.MaxParts))
	}
	if opts.PartNumberMarker > 0 {
		q.Set("marker", strconv.Itoa(opts.PartNumberMarker))
	}
	var out metadata.ListPartsResult
	_, err := c.call(ctx, http.MethodGet, "/v1/parts", q, nil, decodeInto(&out))
	return out, err
}

// CompleteMultipart implements metadata.Metadata. If a retry finds the
// upload gone, it checks whether the object now has the requested version,
// i.e. whether its own earlier attempt completed it.
func (c *Client) CompleteMultipart(ctx context.Context, uploadID string, req metadata.CompleteRequest) (metadata.ObjectMeta, error) {
	up, err := c.GetUpload(ctx, uploadID)
	if err != nil {
		return metadata.ObjectMeta{}, err
	}
	uncertain, err := c.apply(ctx, boltstore.Command{Op: boltstore.OpCompleteMultipart, UploadID: uploadID, Time: c.now(), Complete: &req})
	if err != nil && uncertain && errors.Is(err, metadata.ErrNoSuchUpload) {
		if obj, gerr := c.GetObject(ctx, up.Bucket, up.Key); gerr == nil && obj.VersionID == req.VersionID {
			err = nil
		}
	}
	if err != nil {
		return metadata.ObjectMeta{}, err
	}
	etag, err := metadata.MultipartETag(req.Parts)
	if err != nil {
		return metadata.ObjectMeta{}, err
	}
	return metadata.ObjectMeta{Bucket: up.Bucket, Key: up.Key, VersionID: req.VersionID, ETag: etag}, nil
}

// AbortMultipart implements metadata.Metadata.
func (c *Client) AbortMultipart(ctx context.Context, uploadID string) error {
	uncertain, err := c.apply(ctx, boltstore.Command{Op: boltstore.OpAbortMultipart, UploadID: uploadID})
	if uncertain && errors.Is(err, metadata.ErrNoSuchUpload) {
		return nil // our earlier attempt aborted it
	}
	return err
}

// UpdateShardLocation implements metadata.Metadata. It is idempotent: a
// retry after an unknown outcome that finds the shard already on u.To
// reports a conflict, which the caller treats as "someone moved it".
func (c *Client) UpdateShardLocation(ctx context.Context, u metadata.ShardLocationUpdate) error {
	_, err := c.apply(ctx, boltstore.Command{Op: boltstore.OpUpdateShardLocation, Update: &u})
	return err
}
