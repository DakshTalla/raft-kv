package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/DakshTalla/raft-kv/kv"
)

// This test runs a cluster the "real" way: every node has its own TCP
// listener on localhost and a wall-clock ticker. Nothing is simulated.

func freeAddrs(t *testing.T, n int) map[int]string {
	t.Helper()
	addrs := map[int]string{}
	for id := 1; id <= n; id++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[id] = ln.Addr().String()
		ln.Close()
	}
	return addrs
}

type testCluster struct {
	t     *testing.T
	dir   string
	addrs map[int]string
	nodes map[int]*Server
}

func (c *testCluster) start(id int) {
	c.t.Helper()
	s, err := Start(Config{ID: id, Addrs: c.addrs, DataDir: filepath.Join(c.dir, fmt.Sprintf("node-%d", id)), SnapshotThreshold: 50})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = s
}

func (c *testCluster) stop(id int) {
	c.nodes[id].Close()
	delete(c.nodes, id)
}

func (c *testCluster) leader() int {
	for id, s := range c.nodes {
		if s.Status().Role == "leader" {
			return id
		}
	}
	return 0
}

// do behaves like a client library: try a node, follow "not leader" hints,
// retry until the deadline. The same (client, seq) is reused across retries.
func (c *testCluster) do(op, key, value string, seq uint64) (kv.Result, error) {
	deadline := time.Now().Add(5 * time.Second)
	target := 0
	for time.Now().Before(deadline) {
		s := c.nodes[target]
		if s == nil {
			for id := range c.nodes {
				target, s = id, c.nodes[id]
				break
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		res, err := s.Do(ctx, op, key, value, 4242, seq)
		cancel()
		if err == nil {
			return res, nil
		}
		var nl *NotLeaderError
		if errors.As(err, &nl) && nl.Leader != 0 && nl.Leader != target {
			target = nl.Leader
		} else {
			target = 0
			time.Sleep(20 * time.Millisecond)
		}
	}
	return kv.Result{}, errors.New("timed out")
}

func TestRealClusterOverTCP(t *testing.T) {
	c := &testCluster{t: t, dir: t.TempDir(), addrs: freeAddrs(t, 5), nodes: map[int]*Server{}}
	for id := 1; id <= 5; id++ {
		c.start(id)
	}
	defer func() {
		for id := range c.nodes {
			c.stop(id)
		}
	}()

	seq := uint64(0)
	put := func(k, v string) {
		t.Helper()
		seq++
		if _, err := c.do(kv.OpPut, k, v, seq); err != nil {
			t.Fatalf("put %s=%s: %v", k, v, err)
		}
	}
	get := func(k, want string) {
		t.Helper()
		seq++
		res, err := c.do(kv.OpGet, k, "", seq)
		if err != nil || res.Value != want {
			t.Fatalf("get %s = %q, %v; want %q", k, res.Value, err, want)
		}
	}

	// 1. Elect a leader over real sockets and replicate some writes.
	for i := 0; i < 100; i++ {
		put(fmt.Sprintf("k%d", i%20), fmt.Sprint(i))
	}
	get("k19", "99")

	// 2. Stop the leader; the rest must elect a new one.
	old := c.leader()
	if old == 0 {
		t.Fatal("no leader")
	}
	c.stop(old)
	put("after-crash", "1")
	if l := c.leader(); l == 0 || l == old {
		t.Fatalf("no new leader after stopping node %d", old)
	}

	// 3. Stop a second node: 3 of 5 still make progress.
	for id := range c.nodes {
		if id != c.leader() {
			c.stop(id)
			break
		}
	}
	for i := 0; i < 60; i++ {
		put("three-left", fmt.Sprint(i))
	}

	// 4. Bring the old leader back. The others compacted their logs while it
	//    was away (threshold 50), so it recovers from disk and then needs a
	//    snapshot over the wire to catch up.
	c.start(old)
	put("rejoined", "1")
	get("k0", "80")
	deadline := time.Now().Add(5 * time.Second)
	for {
		want, same := "", true
		for _, s := range c.nodes {
			dump, _ := s.Store().Dump(-1)
			if d := fmt.Sprint(dump); want == "" {
				want = d
			} else if d != want {
				same = false
			}
		}
		if same {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replicas did not converge to the same data")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st := c.nodes[old].Status(); st.SnapshotIndex == 0 {
		t.Error("restarted node should hold a snapshot")
	}
}
