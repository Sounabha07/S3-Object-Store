package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Sounabha07/S3-Object-Store/internal/dataplane"
	"github.com/Sounabha07/S3-Object-Store/internal/gateway"
	"github.com/Sounabha07/S3-Object-Store/internal/httpserv"
	"github.com/Sounabha07/S3-Object-Store/internal/rebalance"
	"github.com/Sounabha07/S3-Object-Store/internal/storagenode"
)

// clusterGateway is a gateway over an in-process storage cluster, with
// direct access to its parts for fault injection.
type clusterGateway struct {
	g      *gateway.Gateway
	sc     *storageCluster
	srv    *httptest.Server
	client *s3.Client
}

func startClusterGateway(t *testing.T, storageNodes int) *clusterGateway {
	t.Helper()
	return startClusterGatewayCfg(t, storageNodes, gateway.Config{})
}

// startClusterGatewayCfg is startClusterGateway with extra gateway settings.
func startClusterGatewayCfg(t *testing.T, storageNodes int, cfg gateway.Config) *clusterGateway {
	t.Helper()
	dir := t.TempDir()
	sc := startStorageCluster(t, filepath.Join(dir, "storage"), storageNodes)
	t.Cleanup(func() { _ = sc.stop() })
	cfg.DataDir, cfg.StorageNodes = dir, sc.urls()
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 256 << 10
	}
	cfg.HealthInterval = 100 * time.Millisecond
	g, err := gateway.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	srv := httptest.NewServer(httpserv.Root(g.Handler, g.Health))
	t.Cleanup(srv.Close)
	return &clusterGateway{g: g, sc: sc, srv: srv, client: newClient(srv.URL)}
}

// TestObjectsReadableWithAnyStorageNodeKilled writes objects across six
// storage nodes, then kills each node in turn: every object must still read
// back intact, and new writes must still succeed with 3 replicas.
func TestObjectsReadableWithAnyStorageNodeKilled(t *testing.T) {
	cg := startClusterGateway(t, 6)
	b := newBucket(t, cg.client, "m3-faults")
	objects := map[string][]byte{}
	for i, size := range []int{0, 1, 1000, 256 << 10, 300_000, 1 << 20, 3<<20 + 7} {
		key := fmt.Sprintf("obj-%d", i)
		objects[key] = randomBytes(t, size)
		put(t, cg.client, b, key, objects[key])
	}
	for _, victim := range cg.sc.nodes {
		victim.kill()
		for key, want := range objects {
			if got := get(t, cg.client, b, key); !bytes.Equal(got, want) {
				t.Fatalf("%s killed: %s content mismatch", victim.id, key)
			}
		}
		key := "written-while-" + string(victim.id) + "-down"
		data := randomBytes(t, 600_000)
		put(t, cg.client, b, key, data)
		objects[key] = data

		obj, err := cg.g.Meta.GetObject(ctx, b, key)
		if err != nil {
			t.Fatal(err)
		}
		for i, c := range obj.Chunks {
			if len(c.Shards) != 3 {
				t.Fatalf("%s chunk %d has %d replicas", key, i, len(c.Shards))
			}
			for _, s := range c.Shards {
				if s.Node == victim.id {
					t.Fatalf("%s chunk %d placed on dead node %s", key, i, victim.id)
				}
			}
		}
		victim.revive()
		waitNodeUp(t, cg.g, victim)
	}
}

func waitNodeUp(t *testing.T, g *gateway.Gateway, n *storageNode) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if !g.Plane.Health().IsDown(n.id) {
			return
		}
	}
	t.Fatalf("%s not seen as up again", n.id)
}

// TestRebalanceWhenNodeAdded adds a seventh storage node, triggers a
// rebalance through the admin API, and checks placement, data and fault
// tolerance afterwards.
func TestRebalanceWhenNodeAdded(t *testing.T) {
	cg := startClusterGateway(t, 6)
	b := newBucket(t, cg.client, "rebal")
	objects := map[string][]byte{}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("obj-%02d", i)
		objects[key] = randomBytes(t, 600_000) // 3 chunks of 256 KiB
		put(t, cg.client, b, key, objects[key])
	}

	newNode := startStorageNode(t, "storage-7", filepath.Join(t.TempDir(), "storage-7"))
	t.Cleanup(func() { newNode.kill(); _ = newNode.disk.Close() })
	cg.g.Plane.AddNode("storage-7", storagenode.NewClient(newNode.url(), nil))

	resp, err := http.Post(cg.srv.URL+"/_admin/rebalance", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var st rebalance.Stats
	_ = json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || st.ShardsMoved == 0 || st.Errors != 0 || st.Skipped != 0 {
		t.Fatalf("rebalance: HTTP %d, stats %+v", resp.StatusCode, st)
	}
	t.Logf("rebalance moved %d of %d shard placements", st.ShardsMoved, 40*3*3)

	for key := range objects {
		obj, err := cg.g.Meta.GetObject(ctx, b, key)
		if err != nil {
			t.Fatal(err)
		}
		for i, c := range obj.Chunks {
			want, _ := cg.g.Plane.Placement().Pick(dataplane.PlacementKey(dataplane.ObjectKey(b, key, obj.VersionID), i), 3)
			var got []string
			for _, s := range c.Shards {
				got = append(got, string(s.Node))
			}
			ws := make([]string, len(want))
			for j, w := range want {
				ws[j] = string(w)
			}
			slices.Sort(got)
			slices.Sort(ws)
			if !slices.Equal(got, ws) {
				t.Fatalf("%s chunk %d on %v, ring wants %v", key, i, got, ws)
			}
		}
	}
	// Data still intact, including with any one original node down.
	for _, victim := range cg.sc.nodes[:2] {
		victim.kill()
		for key, want := range objects {
			out, err := cg.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(key)})
			if err != nil {
				t.Fatalf("%s down: %v", victim.id, err)
			}
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(out.Body)
			out.Body.Close()
			if !bytes.Equal(buf.Bytes(), want) {
				t.Fatalf("%s down: %s mismatch", victim.id, key)
			}
		}
		victim.revive()
		waitNodeUp(t, cg.g, victim)
	}
}
