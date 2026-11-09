package placement

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
)

func nodes(n int) []NodeID {
	out := make([]NodeID, n)
	for i := range out {
		out[i] = NodeID(fmt.Sprintf("storage-%d", i+1))
	}
	return out
}

func TestPickDistinctAndDeterministic(t *testing.T) {
	r := NewRing(nodes(6)...)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bucket/key-%d/v1/0", i)
		a, err := r.Pick(key, 6)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := r.Pick(key, 6)
		if !slices.Equal(a, b) {
			t.Fatalf("Pick not deterministic for %s: %v vs %v", key, a, b)
		}
		seen := map[NodeID]bool{}
		for _, n := range a {
			if seen[n] {
				t.Fatalf("Pick(%s) returned %s twice: %v", key, n, a)
			}
			seen[n] = true
		}
		// A shorter pick is a prefix of a longer one.
		c, _ := r.Pick(key, 3)
		if !slices.Equal(c, a[:3]) {
			t.Fatalf("Pick(%s, 3) = %v, not a prefix of %v", key, c, a)
		}
	}
}

func TestPickExclude(t *testing.T) {
	r := NewRing(nodes(6)...)
	for i := 0; i < 500; i++ {
		key := fmt.Sprint("k", i)
		full, _ := r.Pick(key, 3)
		got, err := r.Pick(key, 3, full[0])
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(got, full[0]) {
			t.Fatalf("excluded node %s returned: %v", full[0], got)
		}
		// Excluding the first choice keeps the others, in order.
		if got[0] != full[1] || got[1] != full[2] {
			t.Fatalf("exclude reshuffled: full %v, excluded-first %v", full, got)
		}
	}
	if _, err := r.Pick("k", 4, nodes(3)...); !errors.Is(err, ErrNotEnoughNodes) {
		t.Fatalf("err = %v, want ErrNotEnoughNodes", err)
	}
	if _, err := r.Pick("k", 3, "unknown-node"); err != nil {
		t.Fatalf("excluding an unknown node: %v", err)
	}
	if _, err := NewRing().Pick("k", 1); !errors.Is(err, ErrNotEnoughNodes) {
		t.Fatalf("empty ring: %v", err)
	}
}

// TestDistribution checks primaries spread evenly across 6 nodes.
func TestDistribution(t *testing.T) {
	r := NewRing(nodes(6)...)
	const keys = 60000
	count := map[NodeID]int{}
	for i := 0; i < keys; i++ {
		p, _ := r.Pick(fmt.Sprintf("b/obj-%d/v/0", i), 1)
		count[p[0]]++
	}
	mean := float64(keys) / 6
	for n, c := range count {
		if dev := math.Abs(float64(c)-mean) / mean; dev > 0.15 {
			t.Errorf("%s holds %d primaries, %.1f%% from the mean %.0f", n, c, dev*100, mean)
		}
	}
}

// TestMinimalMovementOnAdd: adding a 7th node should move about 1/7 of
// replica placements, and only onto the new node.
func TestMinimalMovementOnAdd(t *testing.T) {
	r := NewRing(nodes(6)...)
	const keys = 20000
	before := make([][]NodeID, keys)
	for i := range before {
		before[i], _ = r.Pick(fmt.Sprint("key-", i), 3)
	}
	r.Add("storage-7", 0)
	moved := 0
	for i := range before {
		after, _ := r.Pick(fmt.Sprint("key-", i), 3)
		for _, n := range after {
			if !slices.Contains(before[i], n) {
				moved++
				if n != "storage-7" {
					t.Fatalf("key %d: placement moved to existing node %s (%v -> %v)", i, n, before[i], after)
				}
			}
		}
	}
	frac := float64(moved) / float64(keys*3)
	if frac < 0.08 || frac > 0.22 {
		t.Fatalf("%.1f%% of replica placements moved; expected about 1/7 (14%%)", frac*100)
	}
	r.Remove("storage-7")
	for i := range before {
		if after, _ := r.Pick(fmt.Sprint("key-", i), 3); !slices.Equal(after, before[i]) {
			t.Fatalf("removing the added node did not restore placement for key %d", i)
		}
	}
}

// TestPickDuplicateExcludes: listing a node twice in exclude must not make
// Pick think fewer nodes are eligible (found by the repair tests).
func TestPickDuplicateExcludes(t *testing.T) {
	r := NewRing(nodes(3)...)
	got, err := r.Pick("k", 2, "storage-1", "storage-1", "storage-1")
	if err != nil || len(got) != 2 || slices.Contains(got, "storage-1") {
		t.Fatalf("Pick = %v, %v", got, err)
	}
}

func TestNodesAndRemove(t *testing.T) {
	r := NewRing("b", "a")
	r.Add("c", 10)
	r.Remove("a")
	r.Remove("never-added")
	if got := r.Nodes(); !slices.Equal(got, []NodeID{"b", "c"}) {
		t.Fatalf("Nodes() = %v", got)
	}
}

func TestConcurrentPickAndAdd(t *testing.T) {
	r := NewRing(nodes(6)...)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if g == 0 && i%50 == 0 {
					r.Add(NodeID(fmt.Sprint("extra-", i)), 16)
				}
				if _, err := r.Pick(fmt.Sprint(g, "-", i), 3); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
