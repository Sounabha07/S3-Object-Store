package raftmeta

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/metadatatest"
)

var bg = context.Background()

// tnode is one cluster member in a test. Its addresses are fixed for the
// life of the test so it can be stopped and restarted.
type tnode struct {
	cfg     Config
	node    *Node
	srv     *http.Server
	httpURL string
}

type cluster struct {
	t     *testing.T
	nodes []*tnode
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// raftLogs sends hashicorp/raft logs to stderr when S3OS_RAFT_LOGS=1.
func raftLogs() io.Writer {
	if os.Getenv("S3OS_RAFT_LOGS") == "1" {
		return os.Stderr
	}
	return io.Discard
}

func newCluster(t *testing.T, size int, tweak func(*Config)) *cluster {
	t.Helper()
	c := &cluster{t: t}
	var peers []Peer
	dir := t.TempDir()
	for i := 0; i < size; i++ {
		id := fmt.Sprintf("meta-%d", i+1)
		p := Peer{ID: id, RaftAddr: freeAddr(t), HTTPAddr: "http://" + freeAddr(t)}
		peers = append(peers, p)
		cfg := Config{
			ID: id, RaftBind: p.RaftAddr, DataDir: filepath.Join(dir, id),
			Timing: FastTiming(), RaftLogOutput: raftLogs(),
		}
		if tweak != nil {
			tweak(&cfg)
		}
		c.nodes = append(c.nodes, &tnode{cfg: cfg, httpURL: p.HTTPAddr})
	}
	for _, n := range c.nodes {
		n.cfg.Peers = peers
	}
	for i := range c.nodes {
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.stop(i)
		}
	})
	return c
}

func (c *cluster) start(i int) {
	c.t.Helper()
	n := c.nodes[i]
	node, err := Start(n.cfg)
	if err != nil {
		c.t.Fatalf("start %s: %v", n.cfg.ID, err)
	}
	addr := strings.TrimPrefix(n.httpURL, "http://")
	var ln net.Listener
	for attempt := 0; ; attempt++ { // the port may linger briefly after a restart
		if ln, err = net.Listen("tcp", addr); err == nil {
			break
		}
		if attempt > 50 {
			c.t.Fatalf("listen %s: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	n.node = node
	n.srv = &http.Server{Handler: Handler(node), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = n.srv.Serve(ln) }()
}

// stop simulates a crash of node i: its HTTP API and Raft stop at once.
func (c *cluster) stop(i int) {
	n := c.nodes[i]
	if n.node == nil {
		return
	}
	_ = n.srv.Close()
	if err := n.node.Shutdown(); err != nil {
		c.t.Logf("shutdown %s: %v", n.cfg.ID, err)
	}
	n.node, n.srv = nil, nil
}

func (c *cluster) running() []int {
	var out []int
	for i, n := range c.nodes {
		if n.node != nil {
			out = append(out, i)
		}
	}
	return out
}

func (c *cluster) endpoints() []string {
	var out []string
	for _, n := range c.nodes {
		out = append(out, n.httpURL)
	}
	return out
}

func (c *cluster) client() *Client { return NewClient(c.endpoints(), nil) }

// waitLeader waits until exactly one running node is leader and every
// running node agrees on it, and returns its index.
func (c *cluster) waitLeader(timeout time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader, agree := -1, true
		var leaderID string
		for _, i := range c.running() {
			st := c.nodes[i].node.Status()
			if st.State == "Leader" {
				if leader >= 0 {
					agree = false
				}
				leader = i
			}
			if leaderID == "" {
				leaderID = st.LeaderID
			} else if st.LeaderID != leaderID {
				agree = false
			}
		}
		if leader >= 0 && agree && leaderID == c.nodes[leader].cfg.ID {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no agreed leader within %v", timeout)
	return -1
}

// dump returns a canonical description of a node's state machine.
func dump(t *testing.T, n *Node) string {
	t.Helper()
	var b strings.Builder
	buckets, err := n.store.ListBuckets(bg)
	if err != nil {
		t.Fatal(err)
	}
	for _, bk := range buckets {
		fmt.Fprintf(&b, "bucket %s\n", bk.Name)
		opts := metadata.ListOptions{}
		for {
			res, err := n.store.ListObjects(bg, bk.Name, opts)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range res.Objects {
				obj, err := n.store.GetObject(bg, bk.Name, o.Key)
				if err != nil {
					t.Fatal(err)
				}
				fmt.Fprintf(&b, "  %s %s\n", o.Key, obj.VersionID)
			}
			if !res.IsTruncated {
				break
			}
			opts.ContinuationToken = res.NextContinuationToken
		}
	}
	return b.String()
}

// waitConverged waits until all running nodes have applied the same index
// and hold identical state, and returns that state.
func (c *cluster) waitConverged(timeout time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		var idx []uint64
		for _, i := range c.running() {
			idx = append(idx, c.nodes[i].node.Status().AppliedIndex)
		}
		same := true
		for _, v := range idx[1:] {
			if v != idx[0] {
				same = false
			}
		}
		if same {
			states := map[string]bool{}
			for _, i := range c.running() {
				last = dump(c.t, c.nodes[i].node)
				states[last] = true
			}
			if len(states) == 1 {
				return last
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, i := range c.running() {
		c.t.Logf("%s: %+v", c.nodes[i].cfg.ID, c.nodes[i].node.Status())
	}
	c.t.Fatalf("nodes did not converge within %v", timeout)
	return ""
}

func TestLeaderElection(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader(10 * time.Second)
	for i, n := range c.nodes {
		st := n.node.Status()
		if (i == leader) != (st.State == "Leader") {
			t.Fatalf("%s state %s, leader index %d", st.ID, st.State, leader)
		}
		if st.LeaderHTTP != c.nodes[leader].httpURL {
			t.Fatalf("%s reports leader HTTP %q, want %q", st.ID, st.LeaderHTTP, c.nodes[leader].httpURL)
		}
	}
}

func TestConformanceThroughCluster(t *testing.T) {
	metadatatest.Run(t, func(t *testing.T) metadata.Metadata {
		c := newCluster(t, 3, nil)
		c.waitLeader(10 * time.Second)
		return c.client()
	})
}

func TestWritesReplicateToAllNodes(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.waitLeader(10 * time.Second)
	cl := c.client()
	metadatatest.MustCreateBucket(t, cl, "b")
	for i := 0; i < 50; i++ {
		metadatatest.MustPut(t, cl, "b", fmt.Sprint("k", i))
	}
	state := c.waitConverged(10 * time.Second)
	if n := strings.Count(state, "\n  k"); n != 50 {
		t.Fatalf("converged state has %d objects, want 50", n)
	}
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader(10 * time.Second)
	follower := (leader + 1) % 3

	// Raw HTTP to a follower: 421 with the leader's address.
	resp, err := http.Get(c.nodes[follower].httpURL + "/v1/buckets")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusMisdirectedRequest || !strings.Contains(string(body), c.nodes[leader].httpURL) {
		t.Fatalf("follower answered %d %s", resp.StatusCode, body)
	}

	// A client that only knows the follower still succeeds.
	cl := NewClient([]string{c.nodes[follower].httpURL}, nil)
	metadatatest.MustCreateBucket(t, cl, "via-follower")
	if _, err := cl.HeadBucket(bg, "via-follower"); err != nil {
		t.Fatal(err)
	}
}

// TestReadYourWritesFromAnyEntryPoint: after a write is acknowledged, a
// read through a client that starts at a different node observes it.
func TestReadYourWritesFromAnyEntryPoint(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.waitLeader(10 * time.Second)
	writer := c.client()
	metadatatest.MustCreateBucket(t, writer, "b")
	for i := 0; i < 20; i++ {
		key := fmt.Sprint("k", i)
		metadatatest.MustPut(t, writer, "b", key)
		reader := NewClient([]string{c.nodes[i%3].httpURL}, nil)
		if _, err := reader.GetObject(bg, "b", key); err != nil {
			t.Fatalf("read of acknowledged write %s via %s: %v", key, c.nodes[i%3].cfg.ID, err)
		}
	}
}

// TestLeaderKillDuringWrites runs concurrent writers, kills the leader
// mid-stream, and checks that every acknowledged write survives on every
// node, including the restarted old leader.
func TestLeaderKillDuringWrites(t *testing.T) {
	c := newCluster(t, 3, nil)
	oldLeader := c.waitLeader(10 * time.Second)
	cl := c.client()
	metadatatest.MustCreateBucket(t, cl, "b")

	var (
		mu        sync.Mutex
		acked     = map[string]string{} // key -> version
		failed    atomic.Int64
		lastAckNs atomic.Int64
		stop      atomic.Bool
		wg        sync.WaitGroup
	)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				obj := metadatatest.Object("b", fmt.Sprintf("w%d-%05d", w, i))
				ctx, cancel := context.WithTimeout(bg, 10*time.Second)
				err := cl.PutObject(ctx, obj)
				cancel()
				if err != nil {
					failed.Add(1)
					continue
				}
				mu.Lock()
				acked[obj.Key] = obj.VersionID
				mu.Unlock()
				lastAckNs.Store(time.Now().UnixNano())
			}
		}(w)
	}

	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	beforeKill := len(acked)
	mu.Unlock()
	killedAt := time.Now()
	c.stop(oldLeader)

	var recovery time.Duration
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if ns := lastAckNs.Load(); ns > killedAt.UnixNano() {
			recovery = time.Unix(0, ns).Sub(killedAt)
			break
		}
	}
	if recovery == 0 {
		stop.Store(true)
		wg.Wait()
		t.Fatal("no write was acknowledged within 15s of killing the leader")
	}
	time.Sleep(500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	c.start(oldLeader)
	c.waitLeader(10 * time.Second)
	c.waitConverged(15 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if beforeKill == 0 || len(acked) <= beforeKill {
		t.Fatalf("acked %d writes before the kill and %d in total; expected progress on both sides", beforeKill, len(acked))
	}
	for i, n := range c.nodes {
		missing := 0
		for key, version := range acked {
			obj, err := n.node.store.GetObject(bg, "b", key)
			if err != nil || obj.VersionID != version {
				missing++
			}
		}
		if missing > 0 {
			t.Errorf("node %d (%s) is missing %d of %d acknowledged writes", i, n.cfg.ID, missing, len(acked))
		}
	}
	t.Logf("acknowledged writes: %d (%d before kill), failed attempts: %d, first ack after leader kill: %v",
		len(acked), beforeKill, failed.Load(), recovery.Round(time.Millisecond))
}

// TestNoAcknowledgementWithoutQuorum: with two of three nodes down, writes
// must fail (never be acknowledged), and the cluster recovers afterwards.
func TestNoAcknowledgementWithoutQuorum(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader(10 * time.Second)
	cl := c.client()
	metadatatest.MustCreateBucket(t, cl, "b")
	for i := range c.nodes {
		if i != leader {
			c.stop(i)
		}
	}
	ctx, cancel := context.WithTimeout(bg, 2*time.Second)
	defer cancel()
	err := cl.PutObject(ctx, metadatatest.Object("b", "lonely"))
	if err == nil {
		t.Fatal("write acknowledged by a minority")
	}
	if !errors.Is(err, metadata.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if _, err := cl.ListBuckets(ctx); err == nil {
		t.Fatal("linearizable read served by a minority")
	}

	for i := range c.nodes {
		if i != leader {
			c.start(i)
		}
	}
	c.waitLeader(10 * time.Second)
	metadatatest.MustPut(t, cl, "b", "after-recovery")
	c.waitConverged(10 * time.Second)
}

// TestFollowerRestartCatchesUpViaSnapshot: a follower that misses more
// entries than the leader retains must catch up by installing a snapshot.
func TestFollowerRestartCatchesUpViaSnapshot(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) {
		cfg.SnapshotThreshold = 50
		cfg.TrailingLogs = 10
		cfg.SnapshotInterval = 100 * time.Millisecond
	})
	leader := c.waitLeader(10 * time.Second)
	follower := (leader + 1) % 3
	cl := c.client()
	metadatatest.MustCreateBucket(t, cl, "b")
	c.stop(follower)

	for i := 0; i < 300; i++ {
		metadatatest.MustPut(t, cl, "b", fmt.Sprintf("k%03d", i))
	}
	leader = c.waitLeader(10 * time.Second)
	if err := c.nodes[leader].node.Snapshot(); err != nil {
		t.Fatalf("leader snapshot: %v", err)
	}

	c.start(follower)
	state := c.waitConverged(20 * time.Second)
	if n := strings.Count(state, "\n  k"); n != 300 {
		t.Fatalf("converged state has %d objects, want 300", n)
	}
	if st := c.nodes[follower].node.Status(); st.LastSnapshotIndex == 0 {
		t.Fatalf("follower caught up without installing a snapshot: %+v", st)
	}
}

// TestFullClusterRestart: all nodes stop and start again; state survives
// and the cluster accepts writes.
func TestFullClusterRestart(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.SnapshotThreshold = 20; cfg.TrailingLogs = 5 })
	c.waitLeader(10 * time.Second)
	cl := c.client()
	metadatatest.MustCreateBucket(t, cl, "b")
	for i := 0; i < 60; i++ {
		metadatatest.MustPut(t, cl, "b", fmt.Sprint("k", i))
	}
	before := c.waitConverged(10 * time.Second)
	for i := range c.nodes {
		c.stop(i)
	}
	for i := range c.nodes {
		c.start(i)
	}
	c.waitLeader(10 * time.Second)
	if after := c.waitConverged(10 * time.Second); after != before {
		t.Fatalf("state changed across restart:\n%s\nwas:\n%s", after, before)
	}
	metadatatest.MustPut(t, cl, "b", "after-restart")
	c.waitConverged(10 * time.Second)
}

// TestRecoverLostStateMachine: a node whose fsm.db is deleted rebuilds it
// from its latest snapshot plus the Raft log.
func TestRecoverLostStateMachine(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader(10 * time.Second)
	victim := (leader + 1) % 3
	cl := c.client()
	metadatatest.MustCreateBucket(t, cl, "b")
	for i := 0; i < 30; i++ {
		metadatatest.MustPut(t, cl, "b", fmt.Sprint("a", i))
	}
	c.waitConverged(10 * time.Second)
	if err := c.nodes[victim].node.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for i := 0; i < 30; i++ { // entries after the snapshot, only in the log
		metadatatest.MustPut(t, cl, "b", fmt.Sprint("b", i))
	}
	want := c.waitConverged(10 * time.Second)

	c.stop(victim)
	if err := os.Remove(filepath.Join(c.nodes[victim].cfg.DataDir, "fsm.db")); err != nil {
		t.Fatal(err)
	}
	c.start(victim)
	if got := c.waitConverged(10 * time.Second); got != want {
		t.Fatalf("recovered state differs:\n%s\nwant:\n%s", got, want)
	}
}

func TestParsePeers(t *testing.T) {
	peers, err := ParsePeers("meta-1@meta-1:7001@http://meta-1:7000/, meta-2@10.0.0.2:7001@http://10.0.0.2:7000")
	if err != nil {
		t.Fatal(err)
	}
	want := []Peer{{"meta-1", "meta-1:7001", "http://meta-1:7000"}, {"meta-2", "10.0.0.2:7001", "http://10.0.0.2:7000"}}
	if fmt.Sprint(peers) != fmt.Sprint(want) {
		t.Fatalf("got %+v, want %+v", peers, want)
	}
	for _, bad := range []string{"", "a@b", "@b@c", "a@@c"} {
		if _, err := ParsePeers(bad); err == nil {
			t.Errorf("ParsePeers(%q) succeeded", bad)
		}
	}
}

// TestStartFailureReleasesResources: a node that fails to start (here: its
// Raft port is taken) must close the databases it opened, so that a retry
// on the same data directory works instead of hanging on the file locks.
func TestStartFailureReleasesResources(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := busy.Addr().String()
	cfg := Config{
		ID: "meta-1", RaftBind: addr, DataDir: t.TempDir(), Timing: FastTiming(), RaftLogOutput: raftLogs(),
		Peers: []Peer{{ID: "meta-1", RaftAddr: addr, HTTPAddr: "http://127.0.0.1:1"}},
	}
	if _, err := Start(cfg); err == nil {
		t.Fatal("Start succeeded on a busy port")
	}
	busy.Close()

	done := make(chan error, 1)
	go func() {
		n, err := Start(cfg)
		if err == nil {
			err = n.Shutdown()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("restart after failed start: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("restart after failed start hung; the first attempt left files locked")
	}
}
