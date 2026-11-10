// Command metadata runs one member of the Raft metadata cluster.
//
// Each node is started with the full, static peer list; nodes with no prior
// state bootstrap the cluster from it.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"

	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/metadata/raftmeta"
	"github.com/Sounabha07/S3-Object-Store/internal/metrics"
)

func main() {
	nodeID := flag.String("id", "meta-1", "raft node id (must appear in -peers)")
	dataDir := flag.String("data", "./data/metadata", "directory for the raft log, snapshots and state machine")
	raftBind := flag.String("raft-bind", ":7001", "listen address for raft traffic")
	raftAdvertise := flag.String("raft-advertise", "", "raft address peers dial (default: this node's address in -peers)")
	peers := flag.String("peers", "", "cluster members: id@raftaddr@httpurl,... (including this node)")
	raftLogLevel := flag.String("raft-log-level", "warn", "hashicorp/raft log level")

	httpserv.Main(httpserv.Service{
		Name:        "metadata",
		DefaultAddr: ":7000",
		Setup: func(ctx context.Context, log *slog.Logger) (httpserv.Setup, error) {
			members, err := raftmeta.ParsePeers(*peers)
			if err != nil {
				return httpserv.Setup{}, err
			}
			advertise := *raftAdvertise
			for _, p := range members {
				if p.ID == *nodeID && advertise == "" {
					advertise = p.RaftAddr
				}
			}
			if advertise == "" {
				return httpserv.Setup{}, errors.New("this node's -id is not in -peers and -raft-advertise is unset")
			}
			node, err := raftmeta.Start(raftmeta.Config{
				ID: *nodeID, RaftBind: *raftBind, RaftAdvertise: advertise, DataDir: *dataDir,
				Peers: members, Log: log, RaftLogOutput: os.Stderr, RaftLogLevel: *raftLogLevel,
			})
			if err != nil {
				return httpserv.Setup{}, err
			}
			log.Info("starting", "id", *nodeID, "raft", advertise, "peers", len(members))
			reg := metrics.NewProcess()
			status := func(f func(raftmeta.Status) float64) func() float64 {
				return func() float64 { return f(node.Status()) }
			}
			metrics.Gauge(reg, "raft_is_leader", "1 if this node is the Raft leader.", status(func(s raftmeta.Status) float64 {
				if s.State == "Leader" {
					return 1
				}
				return 0
			}))
			metrics.Gauge(reg, "raft_term", "Current Raft term.", status(func(s raftmeta.Status) float64 { return float64(s.Term) }))
			metrics.Gauge(reg, "raft_commit_index", "Raft commit index.", status(func(s raftmeta.Status) float64 { return float64(s.CommitIndex) }))
			metrics.Gauge(reg, "raft_applied_index", "Last index applied to the state machine.", status(func(s raftmeta.Status) float64 { return float64(s.AppliedIndex) }))
			metrics.Gauge(reg, "raft_last_snapshot_index", "Index of the latest snapshot.", status(func(s raftmeta.Status) float64 { return float64(s.LastSnapshotIndex) }))
			return httpserv.Setup{
				Handler: metrics.InstrumentHTTP(reg, "metadata", raftmeta.Handler(node)),
				Close:   node.Shutdown, Metrics: metrics.Handler(reg),
			}, nil
		},
	})
}
