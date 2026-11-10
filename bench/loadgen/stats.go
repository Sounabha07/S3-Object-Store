package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// percentile returns the nearest-rank p-th percentile (p in (0,100]).
func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	rank := int(math.Ceil(p / 100 * float64(len(s))))
	return s[min(max(rank, 1), len(s))-1]
}

// median of the values (mean of the middle two for even counts).
func median(v []float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	s := slices.Clone(v)
	slices.Sort(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// summarizeCmd groups result files by label and prints the median of each
// metric as a Markdown table (one row per label, in first-seen order).
func summarizeCmd(args []string) error {
	fs := flag.NewFlagSet("summarize", flag.ExitOnError)
	op := fs.String("op", "", "report this operation's stats (put/get/delete) instead of the total")
	_ = fs.Parse(args)
	var files []string
	for _, pat := range fs.Args() {
		m, err := filepath.Glob(pat)
		if err != nil {
			return err
		}
		files = append(files, m...)
	}
	if len(files) == 0 {
		return errNoData
	}
	type group struct {
		label string
		runs  []Result
	}
	var order []string
	groups := map[string]*group{}
	for _, f := range files {
		b, err := os.ReadFile(filepath.Clean(f))
		if err != nil {
			return err
		}
		var r Result
		if err := json.Unmarshal(b, &r); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		g := groups[r.Label]
		if g == nil {
			g = &group{label: r.Label}
			groups[r.Label] = g
			order = append(order, r.Label)
		}
		g.runs = append(g.runs, r)
	}
	fmt.Println("| scenario | runs | MB/s | ops/s | p50 ms | p95 ms | p99 ms | errors |")
	fmt.Println("|---|---|---|---|---|---|---|---|")
	for _, l := range order {
		g := groups[l]
		var mbps, opsps, p50, p95, p99 []float64
		errs := 0
		for _, r := range g.runs {
			st := r.Total
			if *op != "" {
				st = r.ByOp[*op]
			}
			mbps = append(mbps, st.MBps)
			opsps = append(opsps, st.OpsPerS)
			p50 = append(p50, st.P50ms)
			p95 = append(p95, st.P95ms)
			p99 = append(p99, st.P99ms)
			errs += st.Errors
		}
		fmt.Printf("| %s | %d | %.1f | %.1f | %.1f | %.1f | %.1f | %d |\n",
			l, len(g.runs), median(mbps), median(opsps), median(p50), median(p95), median(p99), errs)
	}
	return nil
}

// gapCmd reads an -events CSV and reports, for ops ending after -after (unix
// ns), the time from -after to the first successful op of the given kind.
func gapCmd(args []string) error {
	fs := flag.NewFlagSet("gap", flag.ExitOnError)
	events := fs.String("events", "", "events CSV from loadgen run -events")
	after := fs.Int64("after", 0, "unix nanoseconds of the fault (e.g. leader kill)")
	op := fs.String("op", "put", "operation kind")
	_ = fs.Parse(args)
	f, err := os.Open(filepath.Clean(*events))
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil {
		return err
	}
	var ok []int64
	failures := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if !strings.EqualFold(rec[1], *op) {
			continue
		}
		end, _ := strconv.ParseInt(rec[0], 10, 64)
		if rec[2] != "true" {
			if end > *after {
				failures++
			}
			continue
		}
		ok = append(ok, end)
	}
	slices.Sort(ok)
	// The unavailability window is the longest interval between two
	// consecutive successful completions that ends after the fault. It
	// does not depend on requests that happened to finish just after the
	// fault without being affected by it.
	var worst int64 = -1
	for i := 1; i < len(ok); i++ {
		if ok[i] > *after && ok[i-1] < *after+int64(60*time.Second) {
			worst = max(worst, ok[i]-ok[i-1])
		}
	}
	if worst < 0 {
		return fmt.Errorf("no successful %s after the fault", *op)
	}
	var baseline []float64 // typical spacing before the fault, for context
	for i := 1; i < len(ok) && ok[i] < *after; i++ {
		baseline = append(baseline, float64(ok[i]-ok[i-1]))
	}
	fmt.Printf("{\"max_gap_between_successes_ms\": %.1f, \"median_gap_before_fault_ms\": %.2f, \"failed_ops_after_fault\": %d}\n",
		float64(worst)/1e6, median(baseline)/1e6, failures)
	return nil
}
