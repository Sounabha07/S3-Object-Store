package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/sigv4"
)

// Result is one run's measurements, written as JSON.
type Result struct {
	Label       string             `json:"label"`
	Op          string             `json:"op"`
	Size        int64              `json:"size_bytes"`
	Concurrency int                `json:"concurrency"`
	Duration    float64            `json:"duration_s"` // measured wall time
	Start       time.Time          `json:"start"`
	Total       Stats              `json:"total"`
	ByOp        map[string]Stats   `json:"by_op"`
	Extra       map[string]float64 `json:"extra,omitempty"`
}

// Stats summarises a set of operations.
type Stats struct {
	Ops      int     `json:"ops"`
	Errors   int     `json:"errors"`
	Bytes    int64   `json:"bytes"`
	MBps     float64 `json:"mb_per_s"` // 1 MB = 1,000,000 bytes
	OpsPerS  float64 `json:"ops_per_s"`
	P50ms    float64 `json:"p50_ms"`
	P95ms    float64 `json:"p95_ms"`
	P99ms    float64 `json:"p99_ms"`
	MaxMs    float64 `json:"max_ms"`
	FirstErr string  `json:"first_error,omitempty"`
}

type client struct {
	endpoint, access, secret string
	hc                       *http.Client
}

func (c *client) do(ctx context.Context, method, path string, body io.Reader, size int64) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.ContentLength = size
	}
	sigv4.Sign(req, c.access, c.secret, "us-east-1", sigv4.UnsignedPayload, time.Now())
	return c.hc.Do(req)
}

// randReader is an endless stream of pseudo-random bytes.
type randReader struct{ src *rand.ChaCha8 }

func (r randReader) Read(p []byte) (int, error) { return r.src.Read(p) }

func newRand() randReader {
	var seed [32]byte
	binary.LittleEndian.PutUint64(seed[:], rand.Uint64())
	binary.LittleEndian.PutUint64(seed[8:], uint64(time.Now().UnixNano()))
	return randReader{rand.NewChaCha8(seed)}
}

func (c *client) put(ctx context.Context, bucket, key string, size int64, r randReader) error {
	resp, err := c.do(ctx, http.MethodPut, "/"+bucket+"/"+key, io.LimitReader(r, size), size)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("PUT %s: HTTP %d %s", key, resp.StatusCode, msg)
	}
	return nil
}

func (c *client) get(ctx context.Context, bucket, key string, size int64) error {
	resp, err := c.do(ctx, http.MethodGet, "/"+bucket+"/"+key, nil, 0)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d", key, resp.StatusCode)
	}
	if n != size {
		return fmt.Errorf("GET %s: %d bytes, want %d", key, n, size)
	}
	return nil
}

func (c *client) del(ctx context.Context, bucket, key string) error {
	resp, err := c.do(ctx, http.MethodDelete, "/"+bucket+"/"+key, nil, 0)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("DELETE %s: HTTP %d", key, resp.StatusCode)
	}
	return nil
}

func (c *client) createBucket(ctx context.Context, bucket string) error {
	resp, err := c.do(ctx, http.MethodPut, "/"+bucket, nil, 0)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("create bucket: HTTP %d", resp.StatusCode)
	}
	return nil
}

// parseSize accepts plain bytes or a KiB/MiB/GiB suffix.
func parseSize(s string) (int64, error) {
	mult := int64(1)
	for suf, m := range map[string]int64{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30} {
		if strings.HasSuffix(s, suf) {
			s, mult = strings.TrimSuffix(s, suf), m
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	return n * mult, err
}

type sample struct {
	op    string
	lat   time.Duration
	end   int64 // unix nanos
	bytes int64
	err   error
}

func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	endpoint := fs.String("endpoint", "http://localhost:9000", "gateway URL")
	access := fs.String("access", envOr("AWS_ACCESS_KEY_ID", "test-access-key"), "access key")
	secret := fs.String("secret", envOr("AWS_SECRET_ACCESS_KEY", "test-secret-key"), "secret key")
	bucket := fs.String("bucket", "bench", "bucket (created if missing)")
	op := fs.String("op", "put", "put, get, or mixed (70% GET / 25% PUT / 5% DELETE)")
	sizeStr := fs.String("size", "1MiB", "object size")
	conc := fs.Int("concurrency", 8, "concurrent clients")
	duration := fs.Duration("duration", 15*time.Second, "measurement duration (ignored with -count)")
	count := fs.Int("count", 0, "stop after this many operations instead of after -duration")
	prefill := fs.Int("prefill", 0, "objects to upload before a get/mixed run (default 4x concurrency, at least 16)")
	label := fs.String("label", "", "label for summarize")
	out := fs.String("out", "", "write the JSON result here (default stdout)")
	events := fs.String("events", "", "write one CSV line per op: end_unix_ns,op,ok")
	keysOut := fs.String("keys-out", "", "write the keys of successful PUTs here (one per line)")
	keysIn := fs.String("keys-in", "", "read GET keys from this file instead of prefilling")
	_ = fs.Parse(args)

	size, err := parseSize(*sizeStr)
	if err != nil || size < 0 {
		return fmt.Errorf("bad -size %q", *sizeStr)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = *conc * 2
	tr.MaxConnsPerHost = 0
	tr.DisableCompression = true
	c := &client{endpoint: strings.TrimRight(*endpoint, "/"), access: *access, secret: *secret, hc: &http.Client{Transport: tr}}
	ctx := context.Background()
	if err := c.createBucket(ctx, *bucket); err != nil {
		return err
	}

	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	var keys []string
	if *keysIn != "" && (*op == "get" || *op == "mixed") {
		b, err := os.ReadFile(*keysIn)
		if err != nil {
			return err
		}
		keys = strings.Fields(string(b))
		if len(keys) == 0 {
			return fmt.Errorf("%s has no keys", *keysIn)
		}
	} else if *op == "get" || *op == "mixed" {
		n := *prefill
		if n == 0 {
			n = max(16, 4**conc)
		}
		keys = make([]string, n)
		var wg sync.WaitGroup
		var perr atomic.Value
		next := atomic.Int64{}
		for w := 0; w < min(*conc, 16); w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := newRand()
				for {
					i := int(next.Add(1)) - 1
					if i >= n {
						return
					}
					keys[i] = fmt.Sprintf("pre-%s/%06d", runID, i)
					if err := c.put(ctx, *bucket, keys[i], size, r); err != nil {
						perr.Store(err)
						return
					}
				}
			}()
		}
		wg.Wait()
		if e, ok := perr.Load().(error); ok {
			return fmt.Errorf("prefill: %w", e)
		}
	}

	var (
		mu      sync.Mutex
		samples []sample
		putKeys []string
		done    atomic.Bool
		opsLeft atomic.Int64
	)
	opsLeft.Store(int64(*count))
	start := time.Now()
	deadline := start.Add(*duration)
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := newRand()
			pick := rand.New(rand.NewPCG(uint64(w), uint64(start.UnixNano())))
			var mine []string
			local := make([]sample, 0, 1024)
			for i := 0; ; i++ {
				if *count > 0 {
					if opsLeft.Add(-1) < 0 {
						break
					}
				} else if done.Load() || time.Now().After(deadline) {
					break
				}
				kind := *op
				if kind == "mixed" {
					switch x := pick.IntN(100); {
					case x < 70:
						kind = "get"
					case x < 95:
						kind = "put"
					default:
						kind = "delete"
						if len(mine) == 0 {
							kind = "get"
						}
					}
				}
				t0 := time.Now()
				var err error
				var n int64
				switch kind {
				case "put":
					key := fmt.Sprintf("w-%s/%03d/%08d", runID, w, i)
					err = c.put(ctx, *bucket, key, size, r)
					if err == nil {
						mine = append(mine, key)
						n = size
					}
				case "get":
					err = c.get(ctx, *bucket, keys[pick.IntN(len(keys))], size)
					if err == nil {
						n = size
					}
				case "delete":
					j := pick.IntN(len(mine))
					err = c.del(ctx, *bucket, mine[j])
					mine = append(mine[:j], mine[j+1:]...)
				}
				end := time.Now()
				local = append(local, sample{op: kind, lat: end.Sub(t0), end: end.UnixNano(), bytes: n, err: err})
			}
			mu.Lock()
			samples = append(samples, local...)
			putKeys = append(putKeys, mine...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	res := Result{Label: *label, Op: *op, Size: size, Concurrency: *conc, Duration: elapsed.Seconds(), Start: start, ByOp: map[string]Stats{}}
	res.Total = summarise(samples, elapsed)
	byOp := map[string][]sample{}
	for _, s := range samples {
		byOp[s.op] = append(byOp[s.op], s)
	}
	for k, v := range byOp {
		res.ByOp[k] = summarise(v, elapsed)
	}
	if *keysOut != "" {
		if err := os.WriteFile(*keysOut, []byte(strings.Join(putKeys, "\n")+"\n"), 0o600); err != nil {
			return err
		}
	}
	if *events != "" {
		if err := writeEvents(*events, samples); err != nil {
			return err
		}
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	if *out == "" {
		_, err = os.Stdout.Write(append(b, '\n'))
		return err
	}
	return os.WriteFile(*out, append(b, '\n'), 0o600)
}

func writeEvents(path string, samples []sample) error {
	var sb strings.Builder
	sb.WriteString("end_unix_ns,op,ok\n")
	for _, s := range samples {
		fmt.Fprintf(&sb, "%d,%s,%t\n", s.end, s.op, s.err == nil)
	}
	return os.WriteFile(path, []byte(sb.String()), 0o600)
}

func summarise(samples []sample, elapsed time.Duration) Stats {
	var st Stats
	var lats []time.Duration
	for _, s := range samples {
		if s.err != nil {
			st.Errors++
			if st.FirstErr == "" {
				st.FirstErr = s.err.Error()
			}
			continue
		}
		st.Ops++
		st.Bytes += s.bytes
		lats = append(lats, s.lat)
	}
	secs := elapsed.Seconds()
	if secs > 0 {
		st.MBps = float64(st.Bytes) / 1e6 / secs
		st.OpsPerS = float64(st.Ops) / secs
	}
	st.P50ms = ms(percentile(lats, 50))
	st.P95ms = ms(percentile(lats, 95))
	st.P99ms = ms(percentile(lats, 99))
	st.MaxMs = ms(percentile(lats, 100))
	return st
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var errNoData = errors.New("no data")
