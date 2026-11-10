// Package raftmeta replicates the metadata state machine (boltstore) across
// a static Raft cluster using hashicorp/raft, and exposes it over HTTP.
//
// Writes are Raft log entries. Reads are linearizable: each read waits for a
// Raft barrier issued after the read arrived, so it reflects every write
// acknowledged before it started. Concurrent reads share one barrier.
package raftmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
)

// Peer is one member of the static cluster.
type Peer struct {
	ID       string // Raft server ID, e.g. "meta-1"
	RaftAddr string // host:port of the Raft transport
	HTTPAddr string // base URL of the HTTP API, e.g. "http://meta-1:7000"
}

// ParsePeers parses "id@raftaddr@httpurl,id@raftaddr@httpurl,...".
func ParsePeers(s string) ([]Peer, error) {
	var peers []Peer
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, "@", 3)
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return nil, fmt.Errorf("raftmeta: bad peer %q, want id@raftaddr@httpurl", item)
		}
		peers = append(peers, Peer{ID: parts[0], RaftAddr: parts[1], HTTPAddr: strings.TrimRight(parts[2], "/")})
	}
	if len(peers) == 0 {
		return nil, errors.New("raftmeta: no peers")
	}
	return peers, nil
}

// Timing holds Raft timeouts. The zero value selects production defaults.
type Timing struct {
	Heartbeat   time.Duration
	Election    time.Duration
	LeaderLease time.Duration
	Commit      time.Duration
}

// FastTiming is for tests: elections complete in a few hundred ms.
func FastTiming() Timing {
	return Timing{Heartbeat: 100 * time.Millisecond, Election: 100 * time.Millisecond,
		LeaderLease: 50 * time.Millisecond, Commit: 5 * time.Millisecond}
}

// Config configures one node.
type Config struct {
	ID            string
	RaftBind      string // listen address for Raft traffic
	RaftAdvertise string // address peers dial; defaults to RaftBind
	DataDir       string
	Peers         []Peer // full membership, including this node

	Timing            Timing
	SnapshotThreshold uint64        // log entries between snapshots (0: raft default)
	SnapshotInterval  time.Duration // how often to check the threshold (0: raft default)
	TrailingLogs      uint64        // entries kept after a snapshot (0: raft default)

	Log           *slog.Logger
	RaftLogOutput io.Writer // hashicorp/raft's own log; nil means stderr
	RaftLogLevel  string    // e.g. "warn" (default), "info", "debug"
}

// Node is one running member of the metadata cluster.
type Node struct {
	cfg       Config
	log       *slog.Logger
	store     *boltstore.Store
	logStore  *raftboltdb.BoltStore
	snaps     *raft.FileSnapshotStore
	transport *raft.NetworkTransport
	raft      *raft.Raft
	httpAddrs map[raft.ServerID]string

	readReqs chan chan error
	stop     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// errOutcomeUnknown: the write may or may not have been committed (e.g.
// leadership was lost after the entry was appended). Safe to report to the
// client only as "unknown"; never as success or as definite failure.
var errOutcomeUnknown = errors.New("raftmeta: write outcome unknown")

// notLeaderError carries the current leader's HTTP address, if known.
type notLeaderError struct{ leader string }

func (e *notLeaderError) Error() string {
	if e.leader == "" {
		return "raftmeta: not the leader (leader unknown)"
	}
	return "raftmeta: not the leader; leader is " + e.leader
}

func (e *notLeaderError) Unwrap() error { return metadata.ErrNotLeader }

// Start opens local state, starts Raft and bootstraps the cluster if this
// node has never been part of it.
func Start(cfg Config) (*Node, error) {
	if cfg.ID == "" || cfg.DataDir == "" || cfg.RaftBind == "" {
		return nil, errors.New("raftmeta: ID, DataDir and RaftBind are required")
	}
	if cfg.RaftAdvertise == "" {
		cfg.RaftAdvertise = cfg.RaftBind
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("raftmeta: data dir: %w", err)
	}
	n := &Node{
		cfg:       cfg,
		log:       cfg.Log.With("node", cfg.ID),
		httpAddrs: map[raft.ServerID]string{},
		readReqs:  make(chan chan error, 1024),
		stop:      make(chan struct{}),
	}
	for _, p := range cfg.Peers {
		n.httpAddrs[raft.ServerID(p.ID)] = p.HTTPAddr
	}
	if err := n.open(); err != nil {
		if n.raft != nil {
			err = errors.Join(err, n.raft.Shutdown().Error())
		}
		return nil, errors.Join(err, n.closeStores())
	}
	n.wg.Add(1)
	go n.readLoop()
	return n, nil
}

func (n *Node) open() error {
	cfg := n.cfg
	var err error
	if n.store, err = boltstore.Open(filepath.Join(cfg.DataDir, "fsm.db")); err != nil {
		return err
	}
	if n.logStore, err = raftboltdb.New(raftboltdb.Options{Path: filepath.Join(cfg.DataDir, "raft.db")}); err != nil {
		return fmt.Errorf("raftmeta: log store: %w", err)
	}
	logOut := cfg.RaftLogOutput
	if logOut == nil {
		logOut = os.Stderr
	}
	if n.snaps, err = raft.NewFileSnapshotStore(cfg.DataDir, 2, logOut); err != nil {
		return fmt.Errorf("raftmeta: snapshot store: %w", err)
	}
	if err := n.recoverFSM(); err != nil {
		return err
	}

	advertise, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvertise)
	if err != nil {
		return fmt.Errorf("raftmeta: advertise address: %w", err)
	}
	if n.transport, err = raft.NewTCPTransport(cfg.RaftBind, advertise, 3, 10*time.Second, logOut); err != nil {
		return fmt.Errorf("raftmeta: transport: %w", err)
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.ID)
	rc.NoSnapshotRestoreOnStart = true // the FSM is persistent; see fsm.go
	level := cfg.RaftLogLevel
	if level == "" {
		level = "warn"
	}
	rc.LogOutput, rc.LogLevel = logOut, level
	if t := cfg.Timing; t != (Timing{}) {
		rc.HeartbeatTimeout, rc.ElectionTimeout, rc.LeaderLeaseTimeout, rc.CommitTimeout = t.Heartbeat, t.Election, t.LeaderLease, t.Commit
	}
	if cfg.SnapshotThreshold > 0 {
		rc.SnapshotThreshold = cfg.SnapshotThreshold
	}
	if cfg.SnapshotInterval > 0 {
		rc.SnapshotInterval = cfg.SnapshotInterval
	}
	if cfg.TrailingLogs > 0 {
		rc.TrailingLogs = cfg.TrailingLogs
	}

	hasState, err := raft.HasExistingState(n.logStore, n.logStore, n.snaps)
	if err != nil {
		return fmt.Errorf("raftmeta: inspect state: %w", err)
	}
	f := &fsm{store: n.store, log: n.log}
	if n.raft, err = raft.NewRaft(rc, f, n.logStore, n.logStore, n.snaps, n.transport); err != nil {
		return fmt.Errorf("raftmeta: start raft: %w", err)
	}
	if !hasState {
		// Every node bootstraps with the same static configuration. Raft
		// accepts this from all members; only empty nodes do it.
		var servers []raft.Server
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.RaftAddr)})
		}
		if err := n.raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return fmt.Errorf("raftmeta: bootstrap: %w", err)
		}
	}
	n.log.Info("raft started", "raft_addr", cfg.RaftAdvertise, "existing_state", hasState)
	return nil
}

// recoverFSM restores the newest snapshot into the state machine if the
// local database is behind it (for example, fsm.db was lost). Raft itself
// will not do this because NoSnapshotRestoreOnStart is set.
func (n *Node) recoverFSM() error {
	snaps, err := n.snaps.List()
	if err != nil || len(snaps) == 0 {
		return err
	}
	applied, err := n.store.AppliedIndex()
	if err != nil {
		return err
	}
	latest := snaps[0]
	if applied >= latest.Index {
		return nil
	}
	_, rc, err := n.snaps.Open(latest.ID)
	if err != nil {
		return fmt.Errorf("raftmeta: open snapshot %s: %w", latest.ID, err)
	}
	defer rc.Close()
	n.log.Warn("state machine behind latest snapshot; restoring", "applied", applied, "snapshot_index", latest.Index)
	return n.store.Restore(rc)
}

// Shutdown stops the node. It is safe to call more than once.
func (n *Node) Shutdown() error {
	var err error
	n.stopOnce.Do(func() {
		close(n.stop)
		if n.raft != nil {
			err = n.raft.Shutdown().Error()
		}
		n.wg.Wait()
		err = errors.Join(err, n.closeStores())
	})
	return err
}

func (n *Node) closeStores() error {
	var errs []error
	if n.transport != nil {
		errs = append(errs, n.transport.Close())
	}
	if n.logStore != nil {
		errs = append(errs, n.logStore.Close())
	}
	if n.store != nil {
		errs = append(errs, n.store.Close())
	}
	return errors.Join(errs...)
}

func (n *Node) notLeader() error {
	_, id := n.raft.LeaderWithID()
	return &notLeaderError{leader: n.httpAddrs[id]}
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// Apply proposes cmd and waits for it to be committed and applied. The
// returned error is the command's result, metadata.ErrNotLeader (via
// notLeaderError) if this node cannot accept writes, or errOutcomeUnknown.
func (n *Node) Apply(ctx context.Context, cmd boltstore.Command) error {
	if err := cmd.Validate(); err != nil {
		return err
	}
	if n.raft.State() != raft.Leader {
		return n.notLeader()
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	fut := n.raft.Apply(data, 5*time.Second)
	done := make(chan error, 1)
	go func() { done <- fut.Error() }()
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", errOutcomeUnknown, ctx.Err())
	case err := <-done:
		switch {
		case err == nil:
			res, _ := fut.Response().(applyResult)
			return res.err
		case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipTransferInProgress):
			return n.notLeader() // the entry was never appended
		case errors.Is(err, raft.ErrEnqueueTimeout):
			return fmt.Errorf("%w: %v", metadata.ErrUnavailable, err) // never appended
		default:
			return fmt.Errorf("%w: %v", errOutcomeUnknown, err)
		}
	}
}

// linearize blocks until the state machine reflects every write committed
// before the call. It fails with notLeaderError on followers.
func (n *Node) linearize(ctx context.Context) error {
	ch := make(chan error, 1)
	select {
	case n.readReqs <- ch:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stop:
		return fmt.Errorf("%w: node shutting down", metadata.ErrUnavailable)
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// readLoop batches pending reads behind a single Raft barrier. A barrier is
// a log entry: once it is committed and applied, this node was leader when
// it committed and every earlier entry is applied. Each batch's barrier is
// issued after all of its readers arrived, which is what linearizability
// needs.
func (n *Node) readLoop() {
	defer n.wg.Done()
	for {
		var batch []chan error
		select {
		case first := <-n.readReqs:
			batch = append(batch, first)
		case <-n.stop:
			return
		}
	drain:
		for len(batch) < cap(n.readReqs) {
			select {
			case c := <-n.readReqs:
				batch = append(batch, c)
			default:
				break drain
			}
		}
		err := n.barrier()
		for _, c := range batch {
			c <- err
		}
	}
}

func (n *Node) barrier() error {
	if n.raft.State() != raft.Leader {
		return n.notLeader()
	}
	err := n.raft.Barrier(5 * time.Second).Error()
	switch {
	case err == nil:
		return nil
	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, raft.ErrLeadershipTransferInProgress):
		return n.notLeader()
	default:
		return fmt.Errorf("%w: %v", metadata.ErrUnavailable, err)
	}
}

// Linearizable reads.

func (n *Node) ListBuckets(ctx context.Context) ([]metadata.BucketInfo, error) {
	if err := n.linearize(ctx); err != nil {
		return nil, err
	}
	return n.store.ListBuckets(ctx)
}

func (n *Node) HeadBucket(ctx context.Context, name string) (metadata.BucketInfo, error) {
	if err := n.linearize(ctx); err != nil {
		return metadata.BucketInfo{}, err
	}
	return n.store.HeadBucket(ctx, name)
}

func (n *Node) GetObject(ctx context.Context, bucket, key string) (metadata.ObjectMeta, error) {
	if err := n.linearize(ctx); err != nil {
		return metadata.ObjectMeta{}, err
	}
	return n.store.GetObject(ctx, bucket, key)
}

func (n *Node) ListObjects(ctx context.Context, bucket string, opts metadata.ListOptions) (metadata.ListResult, error) {
	if err := n.linearize(ctx); err != nil {
		return metadata.ListResult{}, err
	}
	return n.store.ListObjects(ctx, bucket, opts)
}

func (n *Node) WalkChunks(ctx context.Context, fn func(bucket, key, versionID string, chunkIndex int, c metadata.Chunk) error) error {
	if err := n.linearize(ctx); err != nil {
		return err
	}
	return n.store.WalkChunks(ctx, fn)
}

func (n *Node) GetUpload(ctx context.Context, uploadID string) (metadata.UploadInfo, error) {
	if err := n.linearize(ctx); err != nil {
		return metadata.UploadInfo{}, err
	}
	return n.store.GetUpload(ctx, uploadID)
}

func (n *Node) ListParts(ctx context.Context, uploadID string, opts metadata.ListPartsOptions) (metadata.ListPartsResult, error) {
	if err := n.linearize(ctx); err != nil {
		return metadata.ListPartsResult{}, err
	}
	return n.store.ListParts(ctx, uploadID, opts)
}

// Status describes the node for operators and tests.
type Status struct {
	ID                string `json:"id"`
	State             string `json:"state"`
	LeaderID          string `json:"leader_id"`
	LeaderHTTP        string `json:"leader_http"`
	Term              uint64 `json:"term"`
	CommitIndex       uint64 `json:"commit_index"`
	AppliedIndex      uint64 `json:"applied_index"` // as recorded by the state machine
	LastSnapshotIndex uint64 `json:"last_snapshot_index"`
}

// Status returns a point-in-time view of the node (not linearizable).
func (n *Node) Status() Status {
	_, leader := n.raft.LeaderWithID()
	stats := n.raft.Stats()
	applied, _ := n.store.AppliedIndex()
	parse := func(k string) uint64 { v, _ := strconv.ParseUint(stats[k], 10, 64); return v }
	return Status{
		ID:                n.cfg.ID,
		State:             n.raft.State().String(),
		LeaderID:          string(leader),
		LeaderHTTP:        n.httpAddrs[leader],
		Term:              parse("term"),
		CommitIndex:       n.raft.CommitIndex(),
		AppliedIndex:      applied,
		LastSnapshotIndex: parse("last_snapshot_index"),
	}
}

// Snapshot forces a Raft snapshot now (used by tests and operators).
func (n *Node) Snapshot() error { return n.raft.Snapshot().Error() }
