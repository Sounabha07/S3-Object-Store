package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// reportCmd prints every scenario's table (medians across runs) for a
// results directory written by bench/run.sh.
func reportCmd(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	dir := fs.String("dir", "", "results directory")
	_ = fs.Parse(args)
	if *dir == "" {
		return fmt.Errorf("-dir is required")
	}
	return report(os.Stdout, *dir)
}

func loadResults(pattern string) ([]Result, error) {
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	slices.Sort(files)
	var out []Result
	for _, f := range files {
		var r Result
		if err := readJSON(f, &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// statsTable prints one row per label: medians of each metric over runs.
func statsTable(w io.Writer, results []Result, op string) {
	fmt.Fprintln(w, "| scenario | runs | MB/s | ops/s | p50 ms | p95 ms | p99 ms | errors |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|")
	var order []string
	groups := map[string][]Stats{}
	for _, r := range results {
		st := r.Total
		if op != "" {
			st = r.ByOp[op]
		}
		if _, ok := groups[r.Label]; !ok {
			order = append(order, r.Label)
		}
		groups[r.Label] = append(groups[r.Label], st)
	}
	for _, l := range order {
		var mbps, opsps, p50, p95, p99 []float64
		errs := 0
		for _, st := range groups[l] {
			mbps = append(mbps, st.MBps)
			opsps = append(opsps, st.OpsPerS)
			p50 = append(p50, st.P50ms)
			p95 = append(p95, st.P95ms)
			p99 = append(p99, st.P99ms)
			errs += st.Errors
		}
		name := l
		if op != "" {
			name += " (" + strings.ToUpper(op) + " only)"
		}
		fmt.Fprintf(w, "| %s | %d | %.1f | %.1f | %.2f | %.2f | %.2f | %d |\n",
			name, len(groups[l]), median(mbps), median(opsps), median(p50), median(p95), median(p99), errs)
	}
	fmt.Fprintln(w)
}

func report(w io.Writer, dir string) error {
	section := func(title, pattern string, ops ...string) error {
		rs, err := loadResults(filepath.Join(dir, pattern))
		if err != nil || len(rs) == 0 {
			return err
		}
		fmt.Fprintf(w, "### %s\n\n", title)
		if len(ops) == 0 {
			ops = []string{""}
		}
		for _, op := range ops {
			statsTable(w, rs, op)
		}
		return nil
	}
	for _, s := range []struct{ title, pattern string }{
		{"1. PUT and GET by object size (8 clients)", "s1-*.json"},
		{"2. Concurrency sweep (1 MiB objects)", "s2-*.json"},
	} {
		if err := section(s.title, s.pattern); err != nil {
			return err
		}
	}
	if err := section("3. Mixed workload (70% GET / 25% PUT / 5% DELETE, 1 MiB, 32 clients)", "s3-mixed-*.json", "", "get", "put", "delete"); err != nil {
		return err
	}
	if err := section("4. GET with 2 of 6 storage nodes down (1 MiB, erasure coded, 8 clients)", "s4-[hd]*.json"); err != nil {
		return err
	}
	if err := repairReport(w, dir); err != nil {
		return err
	}
	if err := failoverReport(w, dir); err != nil {
		return err
	}
	return overheadReport(w, dir)
}

func repairReport(w io.Writer, dir string) error {
	timings, _ := filepath.Glob(filepath.Join(dir, "s5-timing-r*.json"))
	if len(timings) == 0 {
		return nil
	}
	slices.Sort(timings)
	fmt.Fprintln(w, "### 5. Repair after wiping one storage node")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| run | data on wiped node (MB) | shards rebuilt | bytes rebuilt (MB) | repair time (s) | MB/s rebuilt |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|")
	var mbps, secs []float64
	for _, tf := range timings {
		var tm struct {
			Wiped  int64 `json:"wiped_node_bytes"`
			WallNs int64 `json:"repair_wall_ns"`
		}
		if err := readJSON(tf, &tm); err != nil {
			return err
		}
		var st struct {
			ShardsRebuilt int
			BytesWritten  int64
			Errors        int
			Unrecoverable int
		}
		if err := readJSON(strings.Replace(tf, "s5-timing-", "s5-repair-", 1), &st); err != nil {
			return err
		}
		s := float64(tm.WallNs) / 1e9
		rate := float64(st.BytesWritten) / 1e6 / s
		mbps, secs = append(mbps, rate), append(secs, s)
		fmt.Fprintf(w, "| %s | %.0f | %d | %.0f | %.2f | %.1f |\n", strings.TrimSuffix(filepath.Base(tf)[len("s5-timing-"):], ".json"),
			float64(tm.Wiped)/1e6, st.ShardsRebuilt, float64(st.BytesWritten)/1e6, s, rate)
		if st.Errors > 0 || st.Unrecoverable > 0 {
			fmt.Fprintf(w, "| (run had %d errors, %d unrecoverable chunks) | | | | | |\n", st.Errors, st.Unrecoverable)
		}
	}
	fmt.Fprintf(w, "| **median** | | | | **%.2f** | **%.1f** |\n\n", median(secs), median(mbps))
	return nil
}

func failoverReport(w io.Writer, dir string) error {
	gaps, _ := filepath.Glob(filepath.Join(dir, "s6-gap-r*.json"))
	if len(gaps) == 0 {
		return nil
	}
	slices.Sort(gaps)
	fmt.Fprintln(w, "### 6. Raft leader failover under write load (4 clients, 4 KiB PUTs)")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| run | killed | longest gap between successful PUTs (ms) | typical gap before the kill (ms) | failed PUTs after kill |")
	fmt.Fprintln(w, "|---|---|---|---|---|")
	var g []float64
	for _, f := range gaps {
		var gap struct {
			Max      float64 `json:"max_gap_between_successes_ms"`
			Baseline float64 `json:"median_gap_before_fault_ms"`
			Failed   int     `json:"failed_ops_after_fault"`
		}
		if err := readJSON(f, &gap); err != nil {
			return err
		}
		var kill struct {
			Killed string `json:"killed"`
		}
		_ = readJSON(strings.Replace(f, "s6-gap-", "s6-kill-", 1), &kill)
		g = append(g, gap.Max)
		fmt.Fprintf(w, "| %s | %s | %.0f | %.2f | %d |\n", strings.TrimSuffix(filepath.Base(f)[len("s6-gap-"):], ".json"), kill.Killed, gap.Max, gap.Baseline, gap.Failed)
	}
	fmt.Fprintf(w, "| **median** | | **%.0f** | | |\n\n", median(g))
	return nil
}

func overheadReport(w io.Writer, dir string) error {
	var rows []string
	for _, mode := range []string{"ec", "replication"} {
		rs, err := loadResults(filepath.Join(dir, "s7-"+mode+"-r*[0-9].json"))
		if err != nil || len(rs) == 0 {
			continue
		}
		disks, _ := filepath.Glob(filepath.Join(dir, "s7-"+mode+"-disk-r*.json"))
		var over, mbps, p99 []float64
		for _, r := range rs {
			mbps = append(mbps, r.Total.MBps)
			p99 = append(p99, r.Total.P99ms)
		}
		for _, f := range disks {
			var d struct {
				Disk    int64 `json:"disk_bytes"`
				Logical int64 `json:"logical_bytes"`
			}
			if err := readJSON(f, &d); err != nil {
				return err
			}
			over = append(over, float64(d.Disk)/float64(d.Logical))
		}
		name := "RS(4,2) erasure coding"
		if mode == "replication" {
			name = "3x replication"
		}
		rows = append(rows, fmt.Sprintf("| %s | %d | %.3f | %.1f | %.2f |", name, len(rs), median(over), median(mbps), median(p99)))
	}
	if len(rows) == 0 {
		return nil
	}
	fmt.Fprintln(w, "### 7. Erasure coding vs 3x replication (1 GiB as 256 x 4 MiB PUTs, 8 clients)")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| scheme | runs | bytes on disk / logical bytes | PUT MB/s | PUT p99 ms |")
	fmt.Fprintln(w, "|---|---|---|---|---|")
	for _, r := range rows {
		fmt.Fprintln(w, r)
	}
	fmt.Fprintln(w)
	return nil
}
