package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strconv"
	"sync"
)

// DefaultVNodes is the number of virtual nodes per member. More vnodes give
// a more even spread at the cost of a larger ring.
const DefaultVNodes = 128

// Ring is a consistent-hash ring with virtual nodes. It implements
// Placement and is safe for concurrent use.
type Ring struct {
	mu     sync.RWMutex
	vnodes map[NodeID]int
	points []point // sorted by hash
}

type point struct {
	hash uint64
	node NodeID
}

var _ Placement = (*Ring)(nil)

// NewRing returns a ring containing nodes, each with DefaultVNodes.
func NewRing(nodes ...NodeID) *Ring {
	r := &Ring{vnodes: map[NodeID]int{}}
	for _, n := range nodes {
		r.vnodes[n] = DefaultVNodes
	}
	r.rebuild()
	return r
}

// hash64 is the first 8 bytes of SHA-256: well distributed for short,
// similar strings (vnode labels, object keys), which simpler hashes are not.
func hash64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

func (r *Ring) rebuild() {
	pts := make([]point, 0, len(r.vnodes)*DefaultVNodes)
	for n, v := range r.vnodes {
		for i := 0; i < v; i++ {
			pts = append(pts, point{hash: hash64(string(n) + "#" + strconv.Itoa(i)), node: n})
		}
	}
	slices.SortFunc(pts, func(a, b point) int {
		switch {
		case a.hash < b.hash:
			return -1
		case a.hash > b.hash:
			return 1
		case a.node < b.node: // deterministic order on (astronomically rare) collisions
			return -1
		case a.node > b.node:
			return 1
		}
		return 0
	})
	r.points = pts
}

// Add implements Placement.
func (r *Ring) Add(id NodeID, vnodes int) {
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.vnodes[id] = vnodes
	r.rebuild()
}

// Remove implements Placement.
func (r *Ring) Remove(id NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.vnodes[id]; !ok {
		return
	}
	delete(r.vnodes, id)
	r.rebuild()
}

// Nodes implements Placement.
func (r *Ring) Nodes() []NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]NodeID, 0, len(r.vnodes))
	for n := range r.vnodes {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

// Pick implements Placement: it walks clockwise from the key's position and
// returns the first n distinct nodes not in exclude.
func (r *Ring) Pick(key string, n int, exclude ...NodeID) ([]NodeID, error) {
	if n <= 0 {
		return nil, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	excluded := make(map[NodeID]bool, len(exclude))
	for _, e := range exclude {
		if _, ok := r.vnodes[e]; ok {
			excluded[e] = true // a node listed twice is excluded once
		}
	}
	eligible := len(r.vnodes) - len(excluded)
	if eligible < n {
		return nil, ErrNotEnoughNodes
	}
	h := hash64(key)
	start, _ := slices.BinarySearchFunc(r.points, h, func(p point, h uint64) int {
		switch {
		case p.hash < h:
			return -1
		case p.hash > h:
			return 1
		}
		return 0
	})
	out := make([]NodeID, 0, n)
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		node := r.points[(start+i)%len(r.points)].node
		if slices.Contains(out, node) || slices.Contains(exclude, node) {
			continue
		}
		out = append(out, node)
	}
	return out, nil
}
