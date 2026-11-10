// Package chaos runs scripted fault-injection scenarios against the
// docker-compose cluster and checks that no acknowledged write is lost.
//
// It is skipped unless S3OS_CHAOS=1 (scripts/chaos.sh sets it up). Faults
// are injected with the docker CLI: killing containers, disconnecting a
// container from the cluster network, and corrupting shard files inside a
// storage node's volume.
package chaos

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	endpoint   = "http://localhost:9000"
	adminToken = "dev-admin-token"
	network    = "s3os_default"
)

var ctx = context.Background()

// metaPorts maps metadata services to their host-published HTTP ports.
var metaPorts = map[string]int{"meta-1": 7101, "meta-2": 7102, "meta-3": 7103}

func compose(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose", "-f", "../../deploy/docker-compose.yml"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func container(service string) string { return "s3os-" + service + "-1" }

// leader returns the metadata service that currently leads Raft.
func leader(t *testing.T) string {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		for svc, port := range metaPorts {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/status", port))
			if err != nil {
				continue
			}
			var st struct {
				State string `json:"state"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
			if st.State == "Leader" {
				return svc
			}
		}
	}
	t.Fatal("no Raft leader found")
	return ""
}

type writeRec struct {
	start, end time.Time
	ok         bool
}

type harness struct {
	t      *testing.T
	c      *s3.Client
	bucket string

	mu     sync.Mutex
	acked  map[string][32]byte
	writes []writeRec

	readOK, readErr, readMismatch atomic.Int64
	writeErr                      atomic.Int64
	stop                          atomic.Bool
}

func (h *harness) writer(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	sizes := []int{4 << 10, 64 << 10, 900 << 10, 2 << 20, 5<<20 + 123}
	for i := 0; !h.stop.Load(); i++ {
		data := make([]byte, sizes[mrand.IntN(len(sizes))])
		_, _ = rand.Read(data)
		key := fmt.Sprintf("w%d/%06d", id, i)
		start := time.Now()
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := h.c.PutObject(cctx, &s3.PutObjectInput{Bucket: aws.String(h.bucket), Key: aws.String(key), Body: bytes.NewReader(data)})
		cancel()
		end := time.Now()
		h.mu.Lock()
		h.writes = append(h.writes, writeRec{start, end, err == nil})
		if err == nil {
			h.acked[key] = sha256.Sum256(data)
		}
		h.mu.Unlock()
		if err != nil {
			h.writeErr.Add(1)
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// reader re-reads random acknowledged objects during the faults.
func (h *harness) reader(wg *sync.WaitGroup) {
	defer wg.Done()
	for !h.stop.Load() {
		h.mu.Lock()
		var key string
		var want [32]byte
		n := mrand.IntN(len(h.acked) + 1)
		for k, v := range h.acked {
			if n == 0 {
				key, want = k, v
				break
			}
			n--
		}
		h.mu.Unlock()
		if key == "" {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		got, err := h.get(key)
		switch {
		case err != nil:
			h.readErr.Add(1)
		case sha256.Sum256(got) != want:
			h.readMismatch.Add(1)
		default:
			h.readOK.Add(1)
		}
	}
}

func (h *harness) get(key string) ([]byte, error) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := h.c.GetObject(cctx, &s3.GetObjectInput{Bucket: aws.String(h.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// recovery is the time from the fault until the first write that started
// after the fault succeeded.
func (h *harness) recovery(fault time.Time, timeout time.Duration) (time.Duration, error) {
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		h.mu.Lock()
		best := time.Time{}
		for _, w := range h.writes {
			if w.ok && w.start.After(fault) && (best.IsZero() || w.end.Before(best)) {
				best = w.end
			}
		}
		h.mu.Unlock()
		if !best.IsZero() {
			return best.Sub(fault), nil
		}
	}
	return 0, errors.New("writes did not recover")
}

// Phase is one fault scenario's result.
type Phase struct {
	Name       string  `json:"name"`
	RecoveryMs float64 `json:"recovery_ms"` // fault -> first successful fresh write
	Note       string  `json:"note,omitempty"`
}

// Report is the chaos run's outcome.
type Report struct {
	Started          time.Time `json:"started"`
	Phases           []Phase   `json:"phases"`
	ObjectsWritten   int       `json:"objects_written_acknowledged"`
	ObjectsVerified  int       `json:"objects_verified"`
	Mismatches       int       `json:"mismatches"`
	Unreadable       int       `json:"unreadable"`
	WriteErrors      int64     `json:"write_errors_during_faults"`
	ReadsDuringFault int64     `json:"reads_ok_during_faults"`
	ReadErrors       int64     `json:"read_errors_during_faults"`
	ReadMismatches   int64     `json:"read_mismatches_during_faults"`
	CorruptReads     float64   `json:"corrupt_shard_reads_detected"`
}

func TestChaos(t *testing.T) {
	if os.Getenv("S3OS_CHAOS") != "1" {
		t.Skip("set S3OS_CHAOS=1 with the docker-compose cluster running (see scripts/chaos.sh)")
	}
	c := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint), Region: "us-east-1", UsePathStyle: true, RetryMaxAttempts: 1,
		Credentials: credentials.NewStaticCredentialsProvider("test-access-key", "test-secret-key", ""),
	})
	h := &harness{t: t, c: c, bucket: fmt.Sprintf("chaos-%d", time.Now().Unix()), acked: map[string][32]byte{}}
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(h.bucket)}); err != nil {
		t.Fatal(err)
	}
	report := Report{Started: time.Now().UTC()}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go h.writer(i, &wg)
	}
	wg.Add(1)
	go h.reader(&wg)
	time.Sleep(5 * time.Second) // baseline traffic

	phase := func(name string, inject, heal func() string) {
		t.Logf("phase: %s", name)
		note := inject()
		fault := time.Now()
		rec, err := h.recovery(fault, 60*time.Second)
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
		time.Sleep(8 * time.Second) // keep the fault in place under load
		if heal != nil {
			heal()
		}
		time.Sleep(5 * time.Second) // let the cluster settle
		report.Phases = append(report.Phases, Phase{Name: name, RecoveryMs: float64(rec.Microseconds()) / 1000, Note: note})
		t.Logf("  recovery %v", rec)
	}

	phase("kill one storage node (storage-2)",
		func() string { compose(t, "kill", "storage-2"); return "" },
		func() string { compose(t, "start", "storage-2"); return "" })
	phase("kill two storage nodes (storage-3, storage-5)",
		func() string { compose(t, "kill", "storage-3", "storage-5"); return "" },
		func() string { compose(t, "start", "storage-3", "storage-5"); return "" })
	var killed string
	phase("kill the Raft leader",
		func() string { killed = leader(t); compose(t, "kill", killed); return "killed " + killed },
		func() string { compose(t, "start", killed); return "" })
	phase("partition a storage node (storage-4)",
		func() string { docker(t, "network", "disconnect", network, container("storage-4")); return "" },
		func() string {
			docker(t, "network", "connect", "--alias", "storage-4", network, container("storage-4"))
			return ""
		})
	var parted string
	phase("partition the Raft leader",
		func() string {
			parted = leader(t)
			docker(t, "network", "disconnect", network, container(parted))
			return "partitioned " + parted
		},
		func() string {
			docker(t, "network", "connect", "--alias", parted, network, container(parted))
			return ""
		})
	phase("corrupt 40 shard files on storage-1",
		func() string {
			// Flip one byte in up to 40 shard files inside the volume, using
			// the golang image as a throwaway shell.
			out := docker(t, "run", "--rm", "-v", "s3os_storage-1:/data", "golang:1.27", "sh", "-c",
				`n=0; for f in $(find /data/shards -type f | shuf -n 40); do printf '\377' | dd of="$f" bs=1 seek=7 conv=notrunc 2>/dev/null; n=$((n+1)); done; echo $n`)
			return "corrupted " + strings.TrimSpace(out) + " files"
		}, nil)

	h.stop.Store(true)
	wg.Wait()

	// Final repair pass, then verify every acknowledged object.
	req, _ := http.NewRequest(http.MethodPost, endpoint+"/_admin/repair", nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
	report.ObjectsWritten = len(h.acked)
	for key, want := range h.acked {
		got, err := h.get(key)
		switch {
		case err != nil:
			report.Unreadable++
			t.Errorf("acknowledged object %s unreadable: %v", key, err)
		case sha256.Sum256(got) != want:
			report.Mismatches++
			t.Errorf("acknowledged object %s has wrong content", key)
		default:
			report.ObjectsVerified++
		}
	}
	report.WriteErrors = h.writeErr.Load()
	report.ReadsDuringFault = h.readOK.Load()
	report.ReadErrors = h.readErr.Load()
	report.ReadMismatches = h.readMismatch.Load()
	report.CorruptReads = scrapeCorrupt()
	if report.ReadMismatches > 0 {
		t.Errorf("%d reads during faults returned wrong data", report.ReadMismatches)
	}

	b, _ := json.MarshalIndent(report, "", "  ")
	t.Logf("report:\n%s", b)
	if out := os.Getenv("S3OS_CHAOS_OUT"); out != "" {
		if err := os.WriteFile(out, append(b, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// scrapeCorrupt sums storage_shard_read_failures_total{reason="corrupt"}.
func scrapeCorrupt() float64 {
	resp, err := http.Get(endpoint + "/_metrics")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	total := 0.0
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "storage_shard_read_failures_total{") && strings.Contains(line, `reason="corrupt"`) {
			var v float64
			if _, err := fmt.Sscanf(line[strings.LastIndex(line, " ")+1:], "%g", &v); err == nil {
				total += v
			}
		}
	}
	return total
}
