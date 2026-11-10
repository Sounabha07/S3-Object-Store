package integration

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Sounabha07/S3-Object-Store/internal/repair"
)

func scrape(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scrape: %d", resp.StatusCode)
	}
	return string(b)
}

// metricValue returns the value of the first sample whose line starts with
// prefix (metric name plus optional labels), or -1.
func metricValue(text, prefix string) float64 {
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(prefix) + `[^ ]* ([0-9.e+-]+)$`)
	m := re.FindStringSubmatch(text)
	if m == nil {
		return -1
	}
	v, _ := strconv.ParseFloat(m[1], 64)
	return v
}

// TestPrometheusMetrics drives traffic through a cluster gateway and checks
// the exported request, byte, latency, shard-failure and repair metrics.
func TestPrometheusMetrics(t *testing.T) {
	cg := startClusterGateway(t, 6)
	ms := httptest.NewServer(cg.g.MetricsHandler())
	defer ms.Close()
	c := cg.client
	b := newBucket(t, c, "metrics")
	data := randomBytes(t, 1<<20) // erasure coded
	put(t, c, b, "obj", data)
	get(t, c, b, "obj")
	_, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String("missing")})
	wantCode(t, err, "NotFound")

	// Kill a node holding a data shard so the next read records failures
	// for it and reconstructs.
	obj, err := cg.g.Meta.GetObject(ctx, b, "obj")
	if err != nil {
		t.Fatal(err)
	}
	victim := obj.Chunks[0].Shards[0].Node
	for _, n := range cg.sc.nodes {
		if n.id == victim {
			n.kill()
		}
	}
	get(t, c, b, "obj")

	text := scrape(t, ms.URL)
	checks := []struct {
		prefix string
		min    float64
	}{
		{`s3_requests_total{code="200",op="PutObject"}`, 1},
		{`s3_requests_total{code="200",op="GetObject"}`, 2},
		{`s3_requests_total{code="404",op="HeadObject"}`, 1},
		{`s3_request_duration_seconds_count{op="GetObject"}`, 2},
		{`s3_request_duration_seconds_bucket{op="PutObject",le="+Inf"}`, 1},
		{`s3_bytes_in_total`, float64(len(data))},
		{`s3_bytes_out_total`, float64(2 * len(data))},
		{`storage_shard_read_failures_total{node="` + string(victim) + `",reason="unavailable"}`, 1},
		{`repair_queue_depth`, 0},
		{`go_goroutines`, 1},
	}
	for _, ch := range checks {
		if v := metricValue(text, ch.prefix); v < ch.min {
			t.Errorf("%s = %v, want >= %v", ch.prefix, v, ch.min)
		}
	}

	var st repair.Stats
	adminPost(t, cg.srv.URL, "/_admin/repair", &st)
	text = scrape(t, ms.URL)
	if v := metricValue(text, "repair_passes_total"); v < 1 {
		t.Errorf("repair_passes_total = %v", v)
	}
	if !strings.Contains(text, "storage_nodes_down") {
		t.Error("storage_nodes_down gauge missing")
	}
	if v := metricValue(text, "repair_shards_rebuilt_total"); v != float64(st.ShardsRebuilt) {
		t.Errorf("repair_shards_rebuilt_total = %v, pass rebuilt %d", v, st.ShardsRebuilt)
	}
}
