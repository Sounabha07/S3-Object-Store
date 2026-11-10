package main

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	var d []time.Duration
	for i := 1; i <= 100; i++ {
		d = append(d, time.Duration(i)*time.Millisecond)
	}
	// Shuffle-independent: input order must not matter.
	d[0], d[99] = d[99], d[0]
	cases := map[float64]time.Duration{50: 50 * time.Millisecond, 95: 95 * time.Millisecond, 99: 99 * time.Millisecond, 100: 100 * time.Millisecond}
	for p, want := range cases {
		if got := percentile(d, p); got != want {
			t.Errorf("p%v = %v, want %v", p, got, want)
		}
	}
	if percentile(nil, 50) != 0 {
		t.Error("empty input")
	}
	if got := percentile([]time.Duration{7}, 99); got != 7 {
		t.Errorf("single sample p99 = %v", got)
	}
}

func TestMedian(t *testing.T) {
	if median([]float64{3, 1, 2}) != 2 || median([]float64{4, 1, 3, 2}) != 2.5 || !math.IsNaN(median(nil)) {
		t.Fatal("median")
	}
}

func TestParseSize(t *testing.T) {
	cases := map[string]int64{"4KiB": 4096, "1MiB": 1 << 20, "64MiB": 64 << 20, "123": 123, "2GiB": 2 << 30}
	for in, want := range cases {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v", in, got, err)
		}
	}
	if _, err := parseSize("lots"); err == nil {
		t.Error("garbage accepted")
	}
}

func TestSummariseCountsOnlySuccesses(t *testing.T) {
	s := []sample{
		{op: "put", lat: 10 * time.Millisecond, bytes: 1e6},
		{op: "put", lat: 30 * time.Millisecond, bytes: 1e6},
		{op: "put", lat: time.Second, err: os.ErrClosed},
	}
	st := summarise(s, 2*time.Second)
	if st.Ops != 2 || st.Errors != 1 || st.Bytes != 2e6 || st.MBps != 1 || st.OpsPerS != 1 || st.P99ms != 30 {
		t.Fatalf("stats %+v", st)
	}
}

func TestGapUsesLongestSilence(t *testing.T) {
	// Successes every 10 ms, a fault at t=1000 ms, then nothing until
	// 1700 ms; one request also "succeeds" at 1005 ms (in flight before the
	// fault). The window is 1005 -> 1700 = 695 ms, not 5 ms.
	var b strings.Builder
	b.WriteString("end_unix_ns,op,ok\n")
	ms := int64(time.Millisecond)
	for t := int64(0); t < 1000; t += 10 {
		b.WriteString(itoa(t*ms) + ",put,true\n")
	}
	b.WriteString(itoa(1005*ms) + ",put,true\n")
	b.WriteString(itoa(1500*ms) + ",put,false\n")
	for t := int64(1700); t < 2000; t += 10 {
		b.WriteString(itoa(t*ms) + ",put,true\n")
	}
	path := filepath.Join(t.TempDir(), "events.csv")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	out := captureStdout(t, func() error { return gapCmd([]string{"-events", path, "-after", itoa(1000 * ms)}) })
	if !strings.Contains(out, `"max_gap_between_successes_ms": 695.0`) || !strings.Contains(out, `"failed_ops_after_fault": 1`) {
		t.Fatalf("gap output: %s", out)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func captureStdout(t *testing.T, fn func() error) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	ferr := fn()
	w.Close()
	os.Stdout = old
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	if ferr != nil {
		t.Fatal(ferr)
	}
	return string(buf[:n])
}
