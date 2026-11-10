package dataplane

import (
	"context"
	"sync"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage"
)

// Health tracks which storage nodes are currently believed to be down, so
// writes avoid them and reads try them last. A node is marked down when an
// operation on it fails with storage.ErrUnavailable, and by the optional
// background prober; it is considered up again after a cooldown or as soon
// as a probe succeeds.
type Health struct {
	mu        sync.RWMutex
	downUntil map[placement.NodeID]time.Time
	cooldown  time.Duration
	now       func() time.Time
}

// NewHealth returns a tracker with the given cooldown (0: 5s).
func NewHealth(cooldown time.Duration) *Health {
	if cooldown <= 0 {
		cooldown = 5 * time.Second
	}
	return &Health{downUntil: map[placement.NodeID]time.Time{}, cooldown: cooldown, now: time.Now}
}

// MarkDown records that node failed.
func (h *Health) MarkDown(node placement.NodeID) {
	h.mu.Lock()
	h.downUntil[node] = h.now().Add(h.cooldown)
	h.mu.Unlock()
}

// MarkUp clears a node's down state.
func (h *Health) MarkUp(node placement.NodeID) {
	h.mu.Lock()
	delete(h.downUntil, node)
	h.mu.Unlock()
}

// IsDown reports whether node is currently considered down.
func (h *Health) IsDown(node placement.NodeID) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	until, ok := h.downUntil[node]
	return ok && h.now().Before(until)
}

// Down returns the nodes currently considered down.
func (h *Health) Down() []placement.NodeID {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []placement.NodeID
	now := h.now()
	for n, until := range h.downUntil {
		if now.Before(until) {
			out = append(out, n)
		}
	}
	return out
}

// Probe checks every node once.
func (h *Health) Probe(ctx context.Context, nodes map[placement.NodeID]storage.Storage) {
	var wg sync.WaitGroup
	for id, s := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			if err := s.Health(pctx); err != nil {
				h.MarkDown(id)
			} else {
				h.MarkUp(id)
			}
		}()
	}
	wg.Wait()
}

// Run probes every interval until ctx is done.
func (h *Health) Run(ctx context.Context, nodes map[placement.NodeID]storage.Storage, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		h.Probe(ctx, nodes)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
