package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/DakshTalla/raft-kv/checker"
	"github.com/DakshTalla/raft-kv/events"
	"github.com/DakshTalla/raft-kv/kv"
)

// ---------------------------------------------------------------------------
// Test harness

// observer records every event so tests can assert on what happened.
type observer struct {
	mu      sync.Mutex
	leaders map[uint64]int // term -> node that became leader in it
	kinds   map[string]map[int]int
	errs    []string
}

func watch(t *testing.T, c *Cluster) *observer {
	o := &observer{leaders: map[uint64]int{}, kinds: map[string]map[int]int{}}
	ch, _, cancel := c.Hub.Subscribe(1 << 20)
	t.Cleanup(cancel)
	go func() {
		for e := range ch {
			o.record(e)
		}
	}()
	return o
}

func (o *observer) record(e events.Event) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.kinds[e.Kind] == nil {
		o.kinds[e.Kind] = map[int]int{}
	}
	o.kinds[e.Kind][e.Node]++
	if e.Kind == "role" && e.Role == "leader" {
		// ELECTION SAFETY: at most one leader can be elected in a given term.
		if prev, ok := o.leaders[e.Term]; ok && prev != e.Node {
			o.errs = append(o.errs, fmt.Sprintf("term %d has two leaders: node %d and node %d", e.Term, prev, e.Node))
		}
		o.leaders[e.Term] = e.Node
	}
}

func (o *observer) count(kind string, node int) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.kinds[kind][node]
}

func (o *observer) assertElectionSafety(t *testing.T) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, e := range o.errs {
		t.Error("election safety violated: " + e)
	}
	t.Logf("election safety held across %d terms with a leader", len(o.leaders))
}

func newTestCluster(t *testing.T, opts Options) (*Cluster, *observer) {
	t.Helper()
	opts.DataDir = t.TempDir()
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, watch(t, c)
}

func ctxT(d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	time.AfterFunc(d, cancel)
	return ctx
}

func mustPut(t *testing.T, c *Cluster, k, v string) Response {
	t.Helper()
	r, err := c.Put(ctxT(5*time.Second), k, v)
	if err != nil {
		t.Fatalf("put %s=%s: %v", k, v, err)
	}
	return r
}

func mustGet(t *testing.T, c *Cluster, k, want string) {
	t.Helper()
	r, err := c.Get(ctxT(5*time.Second), k)
	if err != nil {
		t.Fatalf("get %s: %v", k, err)
	}
	if r.Value != want {
		t.Fatalf("get %s = %q (found=%v), want %q", k, r.Value, r.Found, want)
	}
}

// waitConverged waits until every live node has the same, fully committed
// and applied log.
func waitConverged(t *testing.T, c *Cluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, last := true, uint64(0)
		for i, id := range c.IDs() {
			st, alive := c.Status(id, 0)
			if !alive {
				continue
			}
			if i == 0 || last == 0 {
				last = st.LastIndex
			}
			if st.LastIndex != last || st.CommitIndex != last || st.LastApplied != last {
				ok = false
			}
		}
		if ok && c.Leader() != 0 {
			return
		}
		if time.Now().After(deadline) {
			for _, id := range c.IDs() {
				st, alive := c.Status(id, 0)
				t.Logf("node %d alive=%v role=%s term=%d last=%d commit=%d applied=%d", id, alive, st.Role, st.Term, st.LastIndex, st.CommitIndex, st.LastApplied)
			}
			t.Fatal("cluster did not converge")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertLogsConsistent checks the LOG MATCHING and STATE MACHINE SAFETY
// properties: wherever two logs overlap they hold identical entries, and all
// replicas hold identical data.
func assertLogsConsistent(t *testing.T, c *Cluster) {
	t.Helper()
	type entryKey struct{ term uint64; data string }
	ref := map[uint64]entryKey{}
	owner := map[uint64]int{}
	var refDump []kv.Pair
	refNode := 0
	for _, id := range c.IDs() {
		st, alive := c.Status(id, -1)
		if !alive {
			continue
		}
		for _, e := range st.Log {
			k := entryKey{e.Term, string(e.Data)}
			if prev, ok := ref[e.Index]; ok && prev != k {
				t.Fatalf("log mismatch at index %d: node %d has term %d %q, node %d has term %d %q",
					e.Index, owner[e.Index], prev.term, kv.Describe([]byte(prev.data)), id, e.Term, kv.Describe(e.Data))
			}
			ref[e.Index], owner[e.Index] = k, id
		}
		dump, _ := c.Store(id).Dump(-1)
		if refNode == 0 {
			refDump, refNode = dump, id
		} else if fmt.Sprint(dump) != fmt.Sprint(refDump) {
			t.Fatalf("state machines diverged: node %d has %v, node %d has %v", refNode, refDump, id, dump)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests

func TestInitialElection(t *testing.T) {
	c, obs := newTestCluster(t, Options{})
	if c.WaitLeader(3*time.Second) == 0 {
		t.Fatal("no leader elected")
	}
	time.Sleep(300 * time.Millisecond) // let it settle
	leaders, term := 0, uint64(0)
	for _, id := range c.IDs() {
		st, _ := c.Status(id, 0)
		if st.Role == "leader" {
			leaders++
		}
		if term == 0 {
			term = st.Term
		} else if st.Term != term {
			t.Errorf("node %d is in term %d, others in %d", id, st.Term, term)
		}
	}
	if leaders != 1 {
		t.Fatalf("want exactly 1 leader, got %d", leaders)
	}
	obs.assertElectionSafety(t)
}

func TestBasicReplication(t *testing.T) {
	c, _ := newTestCluster(t, Options{})
	mustPut(t, c, "a", "1")
	mustPut(t, c, "b", "2")
	mustGet(t, c, "a", "1")
	if _, err := c.Delete(ctxT(5*time.Second), "a"); err != nil {
		t.Fatal(err)
	}
	r, err := c.Get(ctxT(5*time.Second), "a")
	if err != nil || r.Found {
		t.Fatalf("deleted key still visible: %+v err=%v", r, err)
	}
	waitConverged(t, c, 3*time.Second)
	assertLogsConsistent(t, c)
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	c, _ := newTestCluster(t, Options{})
	leader := c.WaitLeader(3 * time.Second)
	mustPut(t, c, "warm", "up") // make sure followers know the leader
	follower := leader%5 + 1
	_, err := c.Do(ctxT(2*time.Second), follower, Request{Op: kv.OpPut, Key: "k", Value: "v"})
	nl, ok := err.(*NotLeaderError)
	if !ok {
		t.Fatalf("want NotLeaderError from follower %d, got %v", follower, err)
	}
	if nl.Leader != leader {
		t.Fatalf("redirect hint = node %d, want leader %d", nl.Leader, leader)
	}
}

func TestReElectionAfterLeaderCrash(t *testing.T) {
	c, obs := newTestCluster(t, Options{})
	mustPut(t, c, "k", "v1")
	old := c.Leader()
	c.Kill(old)
	mustPut(t, c, "k", "v2") // only succeeds once a new leader exists
	if l := c.Leader(); l == old || l == 0 {
		t.Fatalf("leader after crash = %d (old leader %d)", l, old)
	}

	// 5 nodes tolerate 2 failures...
	second := c.Leader()
	c.Kill(second)
	mustPut(t, c, "k", "v3")

	// ...but not 3: with only 2 of 5 alive nothing can commit.
	c.Kill(c.Leader())
	if _, err := c.Put(ctxT(1500*time.Millisecond), "k", "v4"); err == nil {
		t.Fatal("write succeeded without a majority")
	}

	// Bring one back: majority restored, progress resumes.
	if err := c.Restart(old); err != nil {
		t.Fatal(err)
	}
	mustPut(t, c, "k", "v5")
	mustGet(t, c, "k", "v5")
	obs.assertElectionSafety(t)
}

func TestLogConsistencyAcrossPartition(t *testing.T) {
	c, obs := newTestCluster(t, Options{})
	mustPut(t, c, "x", "before")
	waitConverged(t, c, 3*time.Second)

	// Isolate the leader together with one follower: a 2-node minority.
	old := c.Leader()
	buddy := old%5 + 1
	var majority []int
	for _, id := range c.IDs() {
		if id != old && id != buddy {
			majority = append(majority, id)
		}
	}
	c.Partition([][]int{{old, buddy}, majority})

	// A write sent to the stranded leader is appended to its log but can
	// never commit: it only reaches 2 of 5 nodes.
	_, err := c.Do(ctxT(800*time.Millisecond), old, Request{Op: kv.OpPut, Key: "x", Value: "minority"})
	if err == nil {
		t.Fatal("minority leader committed a write")
	}
	before, _ := c.Status(old, 0)

	// The majority side elects a new leader and keeps working.
	deadline := time.Now().Add(5 * time.Second)
	newLeader := 0
	for newLeader == 0 && time.Now().Before(deadline) {
		for _, id := range majority {
			if st, _ := c.Status(id, 0); st.Role == "leader" && st.Term > before.Term {
				newLeader = id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if newLeader == 0 {
		t.Fatal("majority side did not elect a leader")
	}
	if _, err := c.Do(ctxT(3*time.Second), newLeader, Request{Op: kv.OpPut, Key: "x", Value: "majority"}); err != nil {
		t.Fatalf("majority write failed: %v", err)
	}

	// Heal: the old leader must step down, discard its uncommitted entry and
	// adopt the majority's log.
	c.Heal()
	waitConverged(t, c, 5*time.Second)
	assertLogsConsistent(t, c)
	mustGet(t, c, "x", "majority")
	if obs.count("truncate", old)+obs.count("truncate", buddy) == 0 {
		t.Error("expected the minority side to truncate its conflicting entries")
	}
	obs.assertElectionSafety(t)
}

func TestPersistenceAcrossFullRestart(t *testing.T) {
	c, _ := newTestCluster(t, Options{})
	for i := 0; i < 20; i++ {
		mustPut(t, c, fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i))
	}
	waitConverged(t, c, 3*time.Second)
	termBefore := uint64(0)
	for _, id := range c.IDs() {
		st, _ := c.Status(id, 0)
		termBefore = max(termBefore, st.Term)
		c.Kill(id)
	}
	for _, id := range c.IDs() {
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
		// Term and log come back from disk; commitIndex is volatile.
		st, _ := c.Status(id, 0)
		if st.Term < termBefore || st.LastIndex < 20 {
			t.Fatalf("node %d lost persisted state: term=%d lastIndex=%d", id, st.Term, st.LastIndex)
		}
	}
	for i := 0; i < 20; i++ {
		mustGet(t, c, fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i))
	}
	assertLogsConsistent(t, c)
}

func TestSnapshotInstallOnLaggingFollower(t *testing.T) {
	c, obs := newTestCluster(t, Options{SnapshotThreshold: 20})
	mustPut(t, c, "seed", "0")
	leader := c.Leader()
	lagging := leader%5 + 1
	c.Kill(lagging)
	for i := 0; i < 100; i++ {
		mustPut(t, c, fmt.Sprintf("k%d", i%10), fmt.Sprint(i))
	}
	if st, _ := c.Status(c.Leader(), 0); st.SnapshotIndex == 0 {
		t.Fatal("leader did not compact its log")
	}
	// The entries the lagging node needs no longer exist in any log, so the
	// leader must ship a snapshot instead.
	c.Restart(lagging)
	waitConverged(t, c, 5*time.Second)
	if obs.count("restore", lagging) == 0 {
		t.Error("lagging follower caught up without InstallSnapshot")
	}
	assertLogsConsistent(t, c)
	mustGet(t, c, "k9", "99")

	// Snapshots must also survive a crash.
	c.Restart(lagging)
	waitConverged(t, c, 5*time.Second)
	assertLogsConsistent(t, c)
}

// TestChaosLinearizability is the main safety test: concurrent clients run
// while nodes crash, restart, get partitioned and the network drops packets.
// Afterwards we verify (1) election safety, (2) log matching and identical
// state machines, (3) the client-visible history is linearizable.
func TestChaosLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in -short mode")
	}
	c, obs := newTestCluster(t, Options{SnapshotThreshold: 30, Seed: 42})
	c.WaitLeader(3 * time.Second)
	c.SetNetwork(2*time.Millisecond, 12*time.Millisecond, 0.05)

	const clients, duration = 4, 6 * time.Second
	keys := []string{"a", "b", "c"}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var hmu sync.Mutex
	var history []checker.Op

	for cl := 0; cl < clients; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(cl) + 1))
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				op := checker.Op{Key: keys[rng.Intn(len(keys))], Call: time.Now().UnixNano()}
				req := Request{Key: op.Key}
				switch r := rng.Intn(10); {
				case r < 5:
					op.Kind, req.Op = "get", kv.OpGet
				case r < 9:
					op.Kind, req.Op = "put", kv.OpPut
					op.In = fmt.Sprintf("c%d-%d", cl, n) // unique values make stale reads detectable
					req.Value = op.In
				default:
					op.Kind, req.Op = "delete", kv.OpDelete
				}
				res, err := c.Do(ctxT(time.Second), 0, req)
				op.Return = time.Now().UnixNano()
				if err != nil {
					op.Unknown = true
				} else {
					op.Out, op.Found = res.Value, res.Found
				}
				hmu.Lock()
				history = append(history, op)
				hmu.Unlock()
				time.Sleep(time.Duration(rng.Intn(15)) * time.Millisecond)
			}
		}(cl)
	}

	// The chaos monkey. It never takes down more than 2 nodes at once so the
	// cluster can usually make progress between faults.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(7))
		dead := map[int]bool{}
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(150+rng.Intn(350)) * time.Millisecond):
			}
			switch rng.Intn(6) {
			case 0: // crash the leader
				if l := c.Leader(); l != 0 && len(dead) < 2 {
					c.Kill(l)
					dead[l] = true
				}
			case 1: // crash a random node
				if id := 1 + rng.Intn(5); len(dead) < 2 && !dead[id] {
					c.Kill(id)
					dead[id] = true
				}
			case 2: // recover a node
				for id := range dead {
					c.Restart(id)
					delete(dead, id)
					break
				}
			case 3: // random 2/3 split
				p := rng.Perm(5)
				c.Partition([][]int{{p[0] + 1, p[1] + 1}, {p[2] + 1, p[3] + 1, p[4] + 1}})
			case 4: // isolate the leader
				if l := c.Leader(); l != 0 {
					var rest []int
					for _, id := range c.IDs() {
						if id != l {
							rest = append(rest, id)
						}
					}
					c.Partition([][]int{{l}, rest})
				}
			case 5:
				c.Heal()
			}
		}
	}()

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// Repair everything and let the cluster settle.
	c.Heal()
	c.SetNetwork(2*time.Millisecond, 8*time.Millisecond, 0)
	for _, id := range c.IDs() {
		if !c.Alive(id) {
			c.Restart(id)
		}
	}
	mustPut(t, c, "final", "write")
	waitConverged(t, c, 10*time.Second)

	obs.assertElectionSafety(t)
	assertLogsConsistent(t, c)

	okOps, unknown := 0, 0
	for _, op := range history {
		if op.Unknown {
			unknown++
		} else {
			okOps++
		}
	}
	t.Logf("history: %d completed ops, %d timed out; kills=%d restarts=%d partitions=%d",
		okOps, unknown, obs.count("kill", 1)+obs.count("kill", 2)+obs.count("kill", 3)+obs.count("kill", 4)+obs.count("kill", 5),
		obs.count("restart", 1)+obs.count("restart", 2)+obs.count("restart", 3)+obs.count("restart", 4)+obs.count("restart", 5),
		obs.count("partition", 0))
	if okOps < 50 {
		t.Errorf("only %d operations completed — cluster made too little progress", okOps)
	}
	if ok, key := checker.Check(history); !ok {
		t.Fatalf("history is NOT linearizable (key %q)", key)
	}
}
