package integration

import (
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/placement"
	"github.com/Sounabha07/S3-Object-Store/internal/storage/disk"
	"github.com/Sounabha07/S3-Object-Store/internal/storagenode"
)

// storageCluster is a set of in-process storage nodes served over HTTP.
type storageCluster struct {
	nodes []*storageNode
}

type storageNode struct {
	t    *testing.T
	id   placement.NodeID
	dir  string
	addr string // fixed, so a revived node is reachable at the same URL
	disk *disk.Disk
	srv  *httptest.Server
}

func startStorageCluster(t *testing.T, dir string, n int) *storageCluster {
	t.Helper()
	sc := &storageCluster{}
	for i := 1; i <= n; i++ {
		id := placement.NodeID(fmt.Sprintf("storage-%d", i))
		sc.nodes = append(sc.nodes, startStorageNode(t, id, filepath.Join(dir, string(id))))
	}
	return sc
}

func startStorageNode(t *testing.T, id placement.NodeID, dir string) *storageNode {
	t.Helper()
	s := &storageNode{t: t, id: id, dir: dir, addr: freeAddr(t)}
	s.revive()
	return s
}

// revive (re)starts the node on its fixed address with its existing data.
func (s *storageNode) revive() {
	s.t.Helper()
	if s.disk == nil {
		d, err := disk.Open(s.dir)
		if err != nil {
			s.t.Fatal(err)
		}
		s.disk = d
	}
	ln, err := listenRetry(s.addr)
	if err != nil {
		s.t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(httpserv.Root(storagenode.Handler(s.disk, nil), s.disk.Health))
	_ = srv.Listener.Close()
	srv.Listener = ln
	srv.Start()
	s.srv = srv
}

func (s *storageNode) url() string { return "http://" + s.addr }

// kill stops a node abruptly: its port refuses connections from now on.
func (s *storageNode) kill() {
	if s.srv != nil {
		s.srv.CloseClientConnections()
		s.srv.Close()
		s.srv = nil
	}
}

func (sc *storageCluster) urls() map[placement.NodeID]string {
	out := map[placement.NodeID]string{}
	for _, n := range sc.nodes {
		out[n.id] = n.url()
	}
	return out
}

func (sc *storageCluster) stop() error {
	for _, n := range sc.nodes {
		n.kill()
		if n.disk != nil {
			_ = n.disk.Close()
			n.disk = nil
		}
	}
	return nil
}
