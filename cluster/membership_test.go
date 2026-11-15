package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/DakshTalla/raft-kv/checker"
	"github.com/DakshTalla/raft-kv/kv"
)

func leaderVoters(t *testing.T, c *Cluster) []int {
	t.Helper()
	l := c.WaitLeader(3 * time.Second)
	if l == 0 {
		t.Fatal("no leader")
	}
	st, _ := c.Status(l, 0)
	return st.Voters
}

// TestAddNode: a new node catches up (here via a snapshot, because the log
// was compacted), becomes a voter, and changes the quorum from 3 of 5 to 4 of 6.
func TestAddNode(t *testing.T) {
	c, obs := newTestCluster(t, Options{SnapshotThreshold: 20})
	for i := 0; i < 60; i++ {
		mustPut(t, c, fmt.Sprintf("k%d", i%10), fmt.Sprint(i))
	}
	id, err := c.AddNode(ctxT(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if id != 6 {
		t.Fatalf("new node got id %d, want 6", id)
	}
	if v := leaderVoters(t, c); !slices.Equal(v, []int{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("voters = %v", v)
	}
	if obs.count("restore", 6) == 0 {
		t.Error("new node should have caught up from a snapshot")
	}
	mustPut(t, c, "after-add", "1")
	waitConverged(t, c, 3*time.Second)
	assertLogsConsistent(t, c)

	// A restarted joiner remembers its membership (it is in its log).
	c.Restart(6)
	mustPut(t, c, "after-restart", "1")
	if st, _ := c.Status(6, 0); !slices.Contains(st.Voters, 6) {
		t.Fatalf("node 6 forgot it is a voter after restart: %v", st.Voters)
	}

	// Quorum is now 4 of 6: two failures are fine, three are not.
	var victims []int
	for _, x := range c.IDs() {
		if x != c.Leader() && len(victims) < 3 {
			victims = append(victims, x)
		}
	}
	c.Kill(victims[0])
	c.Kill(victims[1])
	mustPut(t, c, "two-down", "1")
	c.Kill(victims[2])
	if _, err := c.Put(ctxT(1500*time.Millisecond), "three-down", "1"); err == nil {
		t.Fatal("write committed with only 3 of 6 voters alive")
	}
	obs.assertElectionSafety(t)
}

// TestRemoveFollower: the cluster shrinks from 5 to 4 voters (quorum 3).
func TestRemoveFollower(t *testing.T) {
	c, _ := newTestCluster(t, Options{})
	mustPut(t, c, "k", "v")
	victim := c.Leader()%5 + 1
	if err := c.RemoveNode(ctxT(5*time.Second), victim); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(c.IDs(), victim) {
		t.Fatal("removed node is still listed")
	}
	if v := leaderVoters(t, c); len(v) != 4 || slices.Contains(v, victim) {
		t.Fatalf("voters = %v", v)
	}
	// 3 of 4 is still a majority.
	for _, x := range c.IDs() {
		if x != c.Leader() {
			c.Kill(x)
			break
		}
	}
	mustPut(t, c, "k", "v2")
	mustGet(t, c, "k", "v2")
}

// TestRemoveLeader: the leader removes itself, commits that change without
// counting its own vote, steps down, and the others elect a new leader.
func TestRemoveLeader(t *testing.T) {
	c, obs := newTestCluster(t, Options{})
	mustPut(t, c, "k", "v")
	old := c.Leader()
	if err := c.RemoveNode(ctxT(5*time.Second), old); err != nil {
		t.Fatal(err)
	}
	mustPut(t, c, "k", "v2")
	if l := c.Leader(); l == old || l == 0 {
		t.Fatalf("leader after removing node %d is %d", old, l)
	}
	if v := leaderVoters(t, c); len(v) != 4 || slices.Contains(v, old) {
		t.Fatalf("voters = %v", v)
	}
	waitConverged(t, c, 3*time.Second)
	assertLogsConsistent(t, c)
	obs.assertElectionSafety(t)
}

// TestOneChangeAtATime: while a change is pending, a second one is refused.
func TestOneChangeAtATime(t *testing.T) {
	c, _ := newTestCluster(t, Options{})
	mustPut(t, c, "k", "v")
	c.Pause() // freeze time so the first change cannot commit
	leader, _, _ := c.get(c.Leader())
	if err := leader.RemoveMember(c.Leader()%5 + 1); err != nil {
		t.Fatal(err)
	}
	if err := leader.RemoveMember((c.Leader()+1)%5 + 1); err == nil {
		t.Fatal("second membership change accepted while the first is pending")
	}
	c.Resume()
}

// TestMembershipChangesUnderLoad: clients keep reading and writing while the
// cluster grows to 7 nodes and shrinks back to 3 (including removing leaders).
// The history must stay linearizable and the replicas identical.
func TestMembershipChangesUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in -short mode")
	}
	c, obs := newTestCluster(t, Options{SnapshotThreshold: 30, Seed: 11})
	c.WaitLeader(3 * time.Second)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var hmu sync.Mutex
	var history []checker.Op
	keys := []string{"a", "b", "c"}
	for cl := 0; cl < 4; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(cl) + 100))
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				op := checker.Op{Key: keys[rng.Intn(len(keys))], Call: time.Now().UnixNano()}
				req := Request{Key: op.Key}
				if rng.Intn(2) == 0 {
					op.Kind, req.Op = "get", kv.OpGet
				} else {
					op.Kind, req.Op = "put", kv.OpPut
					op.In = fmt.Sprintf("c%d-%d", cl, n)
					req.Value = op.In
				}
				res, err := c.Do(ctxT(2*time.Second), 0, req)
				op.Return = time.Now().UnixNano()
				if err != nil {
					op.Unknown = true
				} else {
					op.Out, op.Found = res.Value, res.Found
				}
				hmu.Lock()
				history = append(history, op)
				hmu.Unlock()
				time.Sleep(time.Duration(rng.Intn(10)) * time.Millisecond)
			}
		}(cl)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if _, err := c.AddNode(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Shrink to 3, always removing the current leader first: the hardest case.
	for len(c.IDs()) > 3 {
		victim := c.WaitLeader(3 * time.Second)
		if err := c.RemoveNode(ctx, victim); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	mustPut(t, c, "final", "1")
	waitConverged(t, c, 5*time.Second)
	assertLogsConsistent(t, c)
	obs.assertElectionSafety(t)
	if v := leaderVoters(t, c); len(v) != 3 {
		t.Fatalf("expected 3 voters, got %v", v)
	}
	completed := 0
	for _, op := range history {
		if !op.Unknown {
			completed++
		}
	}
	t.Logf("%d operations completed during 2 additions and 4 removals", completed)
	if completed < 30 {
		t.Errorf("only %d operations completed", completed)
	}
	if ok, key := checker.Check(history); !ok {
		t.Fatalf("history is NOT linearizable (key %q)", key)
	}
}

// TestClusterRestartKeepsMembership: after adding and removing nodes, closing
// the whole cluster and starting it from the same directory brings back the
// same set of nodes with the same data.
func TestClusterRestartKeepsMembership(t *testing.T) {
	dir := t.TempDir()
	c, err := New(Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, c, "k", "v")
	if _, err := c.AddNode(ctxT(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveNode(ctxT(5*time.Second), 2); err != nil {
		t.Fatal(err)
	}
	mustPut(t, c, "k", "v2")
	c.Close()

	c, err = New(Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if ids := c.IDs(); !slices.Equal(ids, []int{1, 3, 4, 5, 6}) {
		t.Fatalf("after restart the cluster has nodes %v, want [1 3 4 5 6]", ids)
	}
	mustGet(t, c, "k", "v2")
	if v := leaderVoters(t, c); !slices.Equal(v, []int{1, 3, 4, 5, 6}) {
		t.Fatalf("voters after restart = %v", v)
	}
}
