package integration

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Sounabha07/S3-Object-Store/internal/metadata/raftmeta"
)

// metaCluster is a 3-node in-process Raft metadata cluster. Its peer list is
// saved in dir, so starting it again on the same dir (a restart) reuses the
// same addresses and data.
type metaCluster struct {
	nodes []*raftmeta.Node
	srvs  []*http.Server
	urls  []string
}

func startMetaCluster(t *testing.T, dir string) *metaCluster {
	t.Helper()
	peersFile := filepath.Join(dir, "peers.json")
	var peers []raftmeta.Peer
	if b, err := os.ReadFile(peersFile); err == nil {
		if err := json.Unmarshal(b, &peers); err != nil {
			t.Fatal(err)
		}
	} else {
		for i := 1; i <= 3; i++ {
			peers = append(peers, raftmeta.Peer{
				ID: fmt.Sprintf("meta-%d", i), RaftAddr: freeAddr(t), HTTPAddr: "http://" + freeAddr(t),
			})
		}
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(peers)
		if err := os.WriteFile(peersFile, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	mc := &metaCluster{}
	for _, p := range peers {
		node, err := raftmeta.Start(raftmeta.Config{
			ID: p.ID, RaftBind: p.RaftAddr, DataDir: filepath.Join(dir, p.ID), Peers: peers,
			Timing: raftmeta.FastTiming(), RaftLogOutput: io.Discard,
		})
		if err != nil {
			_ = mc.stop()
			t.Fatalf("start %s: %v", p.ID, err)
		}
		ln, err := listenRetry(p.HTTPAddr[len("http://"):])
		if err != nil {
			_ = node.Shutdown()
			_ = mc.stop()
			t.Fatal(err)
		}
		srv := &http.Server{Handler: raftmeta.Handler(node), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = srv.Serve(ln) }()
		mc.nodes = append(mc.nodes, node)
		mc.srvs = append(mc.srvs, srv)
		mc.urls = append(mc.urls, p.HTTPAddr)
	}
	return mc
}

func (mc *metaCluster) stop() error {
	var errs []error
	for _, s := range mc.srvs {
		errs = append(errs, s.Close())
	}
	for _, n := range mc.nodes {
		errs = append(errs, n.Shutdown())
	}
	mc.srvs, mc.nodes = nil, nil
	return errors.Join(errs...)
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

func listenRetry(addr string) (net.Listener, error) {
	var err error
	for i := 0; i < 50; i++ {
		var ln net.Listener
		if ln, err = net.Listen("tcp", addr); err == nil {
			return ln, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("listen %s: %w", addr, err)
}
