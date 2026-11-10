package raftmeta

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/hashicorp/raft"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata/boltstore"
)

// fsm adapts boltstore.Store to raft.FSM.
//
// The store is persistent and records the last applied log index in the same
// transaction as each mutation, so:
//   - on restart Raft is configured not to restore the latest snapshot
//     (NoSnapshotRestoreOnStart); replayed entries at or below the stored
//     index are skipped by the store;
//   - a snapshot may include state newer than its own index (it is read when
//     persisted, not when started). That is safe: it carries its applied
//     index, and entries the snapshot already contains are skipped on replay.
type fsm struct {
	store *boltstore.Store
	log   *slog.Logger
}

// applyResult is what Raft hands back to the proposer via ApplyFuture.
type applyResult struct{ err error }

func (f *fsm) Apply(l *raft.Log) any {
	var cmd boltstore.Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		// Still consume the index; the command is rejected deterministically.
		cmd = boltstore.Command{Op: "Undecodable"}
	}
	err := f.store.Apply(l.Index, cmd)
	if errors.Is(err, boltstore.ErrStorage) {
		// The local database failed. Continuing would leave this replica
		// diverged from the others, so stop the process; on restart it
		// recovers from its last committed state plus the Raft log.
		f.log.Error("fatal: state machine storage failure", "index", l.Index, "err", err)
		panic(fmt.Sprintf("raftmeta: state machine storage failure at index %d: %v", l.Index, err))
	}
	return applyResult{err: err}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) { return &fsmSnapshot{store: f.store}, nil }

func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := f.store.Restore(rc); err != nil {
		return fmt.Errorf("raftmeta: restore snapshot: %w", err)
	}
	return nil
}

type fsmSnapshot struct{ store *boltstore.Store }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.store.Snapshot(sink); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("raftmeta: persist snapshot: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
