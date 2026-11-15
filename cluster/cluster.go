// Package cluster wires N Raft nodes, their KV stores, the simulated network
// and the event hub into one runnable unit, and exposes the chaos controls
// (kill, restart, partition, latency, packet loss, slow motion).
package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	"github.com/DakshTalla/raft-kv/events"
	"github.com/DakshTalla/raft-kv/kv"
	"github.com/DakshTalla/raft-kv/raft"
	"github.com/DakshTalla/raft-kv/sim"
)

// Options configures a cluster. Zero values get sensible defaults.
type Options struct {
	N       int    // number of nodes (default 5)
	DataDir string // each node persists under DataDir/node-<id> (required)
	Fsync   bool   // fsync every write (durable against power loss, slower)

	Tick           time.Duration // virtual time per Raft tick (default 10ms)
	ElectionTicks  int           // min election timeout in ticks (default 15 = 150ms)
	HeartbeatTicks int           // heartbeat interval in ticks (default 5 = 50ms)
	MaxBatch       int
	// SnapshotThreshold: compact the log after this many applied entries.
	SnapshotThreshold uint64

	Scale      float64       // slow-motion factor (1 = real time)
	LatencyMin time.Duration // one-way network latency range (virtual)
	LatencyMax time.Duration
	Seed       int64
	// Sessions is the max number of concurrent client requests (default 256).
	Sessions int
}

func (o *Options) defaults() {
	if o.N == 0 {
		o.N = 5
	}
	if o.Tick == 0 {
		o.Tick = 10 * time.Millisecond
	}
	if o.ElectionTicks == 0 {
		o.ElectionTicks = 15
	}
	if o.HeartbeatTicks == 0 {
		o.HeartbeatTicks = 5
	}
	if o.Scale == 0 {
		o.Scale = 1
	}
	if o.LatencyMax == 0 {
		o.LatencyMin, o.LatencyMax = 2*time.Millisecond, 8*time.Millisecond
	}
	if o.Seed == 0 {
		o.Seed = time.Now().UnixNano()
	}
	if o.Sessions == 0 {
		o.Sessions = 256
	}
}

type member struct {
	id         int
	alive      bool
	node       *raft.Node
	store      *kv.Store
	cancelTick func()
	last       raft.Status // status captured when the node was killed
}

type clientSession struct{ id, seq uint64 }

// Cluster is a running set of Raft nodes.
type Cluster struct {
	opts  Options
	Clock *sim.Clock
	Net   *sim.Network
	Hub   *events.Hub

	mu       sync.Mutex
	members  map[int]*member
	ids      []int
	sessions chan *clientSession
	cancel   context.CancelFunc
	done     chan struct{}
}

// New builds and starts a cluster. Call Close to stop it.
func New(opts Options) (*Cluster, error) {
	opts.defaults()
	if opts.DataDir == "" {
		return nil, fmt.Errorf("cluster: DataDir is required")
	}
	c := &Cluster{
		opts: opts, Clock: sim.NewClock(opts.Scale), Hub: events.NewHub(),
		members: map[int]*member{}, sessions: make(chan *clientSession, opts.Sessions),
		done: make(chan struct{}),
	}
	c.Net = sim.NewNetwork(c.Clock, opts.Seed, opts.LatencyMin, opts.LatencyMax, sim.Observer{
		OnSend:    func(f sim.Flight) { c.publishFlight("msg_send", f) },
		OnDeliver: func(f sim.Flight) { c.publishFlight("msg_recv", f) },
		OnDrop:    func(f sim.Flight) { c.publishFlight("msg_drop", f) },
	})
	// WHY random session ids: the duplicate-detection table is part of the
	// replicated state and survives restarts of this process. Reusing ids
	// 1..N after a restart would make new requests look like old duplicates.
	rng := rand.New(rand.NewSource(opts.Seed ^ time.Now().UnixNano()))
	for i := 0; i < opts.Sessions; i++ {
		c.sessions <- &clientSession{id: rng.Uint64()>>1 | 1}
	}
	for id := 1; id <= opts.N; id++ {
		c.ids = append(c.ids, id)
		c.members[id] = &member{id: id}
	}
	for _, id := range c.ids {
		if err := c.startNode(id); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go func() {
		c.Clock.Run(ctx)
		close(c.done)
	}()
	go c.broadcastState(ctx)
	return c, nil
}

// Close stops the scheduler and all nodes.
func (c *Cluster) Close() {
	c.cancel()
	<-c.done
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.members {
		if m.alive {
			m.cancelTick()
			m.node.Stop()
			m.alive = false
		}
	}
}

// startNode (re)creates node id from whatever is on its disk.
func (c *Cluster) startNode(id int) error {
	st, err := raft.OpenStorage(filepath.Join(c.opts.DataDir, fmt.Sprintf("node-%d", id)), c.opts.Fsync)
	if err != nil {
		return err
	}
	store := kv.NewStore()
	node, err := raft.NewNode(raft.Config{
		ID: id, Peers: c.ids,
		ElectionTicks: c.opts.ElectionTicks, HeartbeatTicks: c.opts.HeartbeatTicks,
		MaxBatch: c.opts.MaxBatch, SnapshotThreshold: c.opts.SnapshotThreshold,
		Storage: st, StateMachine: store,
		Send:    c.Net.Send,
		OnEvent: c.publishRaft,
		Seed:    c.opts.Seed + int64(id)*1_000_003 + time.Now().UnixNano(),
	})
	if err != nil {
		st.Close()
		return err
	}
	c.Net.Attach(id, node.Step)
	cancel := c.Clock.Every(c.opts.Tick, node.Tick)
	c.mu.Lock()
	m := c.members[id]
	m.node, m.store, m.alive, m.cancelTick = node, store, true, cancel
	c.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// Chaos controls

// Kill crashes a node: it stops instantly and loses all in-memory state.
func (c *Cluster) Kill(id int) error {
	c.mu.Lock()
	m := c.members[id]
	if m == nil {
		c.mu.Unlock()
		return fmt.Errorf("no such node %d", id)
	}
	if !m.alive {
		c.mu.Unlock()
		return nil
	}
	m.alive = false
	node, cancel := m.node, m.cancelTick
	c.mu.Unlock()

	c.Net.Detach(id)
	cancel()
	st := node.Status(logTail)
	node.Stop()
	c.mu.Lock()
	m.last = st
	c.mu.Unlock()
	c.publish(events.Event{Kind: "kill", Node: id, Term: st.Term})
	return nil
}

// Restart brings a node back, recovering term, vote, log and snapshot from
// disk. Restarting a live node is a crash followed by a recovery.
func (c *Cluster) Restart(id int) error {
	if err := c.Kill(id); err != nil {
		return err
	}
	if err := c.startNode(id); err != nil {
		return err
	}
	c.publish(events.Event{Kind: "restart", Node: id})
	return nil
}

// Partition splits the network into groups that cannot talk to each other.
func (c *Cluster) Partition(groups [][]int) {
	c.Net.Partition(groups)
	c.publish(events.Event{Kind: "partition", Detail: fmt.Sprint(groups)})
}

// Heal removes every partition.
func (c *Cluster) Heal() {
	c.Net.Heal()
	c.publish(events.Event{Kind: "heal"})
}

// SetNetwork changes latency (virtual time) and the packet drop rate (0..1).
func (c *Cluster) SetNetwork(latMin, latMax time.Duration, drop float64) {
	c.Net.SetConditions(latMin, latMax, drop)
	c.publish(events.Event{Kind: "network", Detail: fmt.Sprintf("latency %v-%v, drop %.0f%%", latMin, latMax, drop*100)})
}

// SetScale sets the slow-motion factor (1 = real time, 20 = 20x slower).
func (c *Cluster) SetScale(scale float64) {
	c.Clock.SetScale(scale)
	c.publish(events.Event{Kind: "speed", Detail: fmt.Sprintf("%gx slower", scale)})
}

// Pause freezes the whole simulation; Resume continues it.
func (c *Cluster) Pause()  { c.Clock.Pause(); c.publish(events.Event{Kind: "pause"}) }
func (c *Cluster) Resume() { c.Clock.Resume(); c.publish(events.Event{Kind: "resume"}) }

// ---------------------------------------------------------------------------
// Introspection

// IDs returns all node ids.
func (c *Cluster) IDs() []int { return append([]int(nil), c.ids...) }

// Alive reports whether node id is running.
func (c *Cluster) Alive(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	return m != nil && m.alive
}

func (c *Cluster) get(id int) (node *raft.Node, store *kv.Store, alive bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	if m == nil {
		return nil, nil, false
	}
	return m.node, m.store, m.alive
}

// Status returns the Raft status of one node (the last known one if dead).
func (c *Cluster) Status(id int, tail int) (raft.Status, bool) {
	node, _, alive := c.get(id)
	if node == nil {
		return raft.Status{ID: id}, false
	}
	if !alive {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.members[id].last, false
	}
	return node.Status(tail), true
}

// Store returns node id's local state machine (for tests and display).
func (c *Cluster) Store(id int) *kv.Store {
	_, s, _ := c.get(id)
	return s
}

// Leader returns the live node that claims leadership with the highest term,
// or 0. WHY "highest term": during a partition a deposed leader on the
// minority side still believes it leads, in an older term.
func (c *Cluster) Leader() int {
	best, bestTerm := 0, uint64(0)
	for _, id := range c.ids {
		if st, alive := c.Status(id, 0); alive && st.Role == "leader" && st.Term >= bestTerm {
			best, bestTerm = id, st.Term
		}
	}
	return best
}

// WaitLeader blocks until some node is leader or the timeout (real time) hits.
func (c *Cluster) WaitLeader(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != 0 {
			return l
		}
		time.Sleep(2 * time.Millisecond)
	}
	return 0
}

// ---------------------------------------------------------------------------
// Event publishing

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func (c *Cluster) publish(e events.Event) {
	e.T = ms(c.Clock.Now())
	c.Hub.Publish(e)
}

func (c *Cluster) publishRaft(e raft.Event) {
	if !c.Hub.Active() {
		return
	}
	c.publish(events.Event{
		Kind: e.Kind, Node: e.Node, Term: e.Term, Role: e.Role,
		Index: e.Index, Peer: e.Peer, Detail: e.Detail,
	})
}

func (c *Cluster) publishFlight(kind string, f sim.Flight) {
	if !c.Hub.Active() {
		return
	}
	m := f.Msg
	c.publish(events.Event{Kind: kind, Msg: &events.Msg{
		ID: f.ID, Type: m.Type.String(), From: m.From, To: m.To, Term: m.Term,
		SentAt: ms(f.SentAt), DeliverAt: ms(f.DeliverAt),
		Entries: len(m.Entries), Heartbeat: m.IsHeartbeat(), Success: m.Success,
		Reason: f.Reason,
	}})
}
