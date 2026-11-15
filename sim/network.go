package sim

import (
	"math/rand"
	"sync"
	"time"

	"github.com/DakshTalla/raft-kv/raft"
)

// Flight describes one message travelling through the network. It is what the
// visualizer animates.
type Flight struct {
	ID        uint64
	Msg       raft.Message
	SentAt    time.Duration // virtual time
	DeliverAt time.Duration // virtual time (planned)
	Reason    string        // for drops: "partition", "loss" or "down"
}

// Observer receives network events. Callbacks must not block.
type Observer struct {
	OnSend    func(Flight)
	OnDeliver func(Flight)
	OnDrop    func(Flight)
}

// Network is an in-memory message bus between Raft nodes with fault injection.
type Network struct {
	clock *Clock
	obs   Observer

	mu         sync.Mutex
	rng        *rand.Rand
	handlers   map[int]func(raft.Message)
	down       map[int]bool
	group      map[int]int // partition id per node; equal ids can talk
	latencyMin time.Duration
	latencyMax time.Duration
	dropRate   float64
	lastArrive map[[2]int]time.Duration
	nextID     uint64
}

// NewNetwork creates a healthy network with the given one-way latency range.
func NewNetwork(clock *Clock, seed int64, latMin, latMax time.Duration, obs Observer) *Network {
	return &Network{
		clock: clock, obs: obs, rng: rand.New(rand.NewSource(seed)),
		handlers: map[int]func(raft.Message){}, down: map[int]bool{}, group: map[int]int{},
		latencyMin: latMin, latencyMax: latMax, lastArrive: map[[2]int]time.Duration{},
	}
}

// Attach registers (or replaces) the message handler for a node and marks it up.
func (n *Network) Attach(id int, h func(raft.Message)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.down[id] = false
}

// Detach marks a node as crashed: it can neither send nor receive.
func (n *Network) Detach(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = true
	delete(n.handlers, id)
}

// Partition splits the cluster: nodes in the same group can talk, nodes in
// different groups cannot. Nodes not listed end up isolated on their own.
func (n *Network) Partition(groups [][]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[int]int{}
	for gi, g := range groups {
		for _, id := range g {
			n.group[id] = gi + 1
		}
	}
	next := len(groups) + 1
	for id := range n.handlers {
		if _, ok := n.group[id]; !ok {
			n.group[id] = next
			next++
		}
	}
}

// Heal removes all partitions.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[int]int{}
}

// Groups returns the partition id of every node (all equal = healthy).
func (n *Network) Groups() map[int]int {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[int]int{}
	for k, v := range n.group {
		out[k] = v
	}
	return out
}

// SetConditions changes latency and packet loss.
func (n *Network) SetConditions(latMin, latMax time.Duration, dropRate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if latMax < latMin {
		latMax = latMin
	}
	n.latencyMin, n.latencyMax = latMin, latMax
	n.dropRate = min(max(dropRate, 0), 1)
}

// Conditions returns the current latency range and drop rate.
func (n *Network) Conditions() (latMin, latMax time.Duration, dropRate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.latencyMin, n.latencyMax, n.dropRate
}

func (n *Network) connectedLocked(a, b int) bool { return n.group[a] == n.group[b] }

// Send queues m for delivery. It never blocks, which is what lets the Raft
// node call it while holding its own lock.
func (n *Network) Send(m raft.Message) {
	n.mu.Lock()
	if n.down[m.From] {
		n.mu.Unlock()
		return // a crashed node sends nothing
	}
	n.nextID++
	now := n.clock.Now()
	f := Flight{ID: n.nextID, Msg: m, SentAt: now}
	lat := n.latencyMin
	if span := n.latencyMax - n.latencyMin; span > 0 {
		lat += time.Duration(n.rng.Int63n(int64(span)))
	}
	f.DeliverAt = now + lat

	if !n.connectedLocked(m.From, m.To) {
		f.Reason = "partition"
	} else if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		f.Reason = "loss"
	}
	if f.Reason != "" {
		n.mu.Unlock()
		if n.obs.OnDrop != nil {
			n.obs.OnDrop(f)
		}
		return
	}
	// WHY per-link FIFO: real Raft runs over TCP, which never reorders
	// messages on one connection. Without this, random latency would reorder
	// AppendEntries and cause needless rejections.
	link := [2]int{m.From, m.To}
	if last := n.lastArrive[link]; f.DeliverAt < last {
		f.DeliverAt = last
	}
	n.lastArrive[link] = f.DeliverAt
	n.mu.Unlock()

	if n.obs.OnSend != nil {
		n.obs.OnSend(f)
	}
	n.clock.At(f.DeliverAt, func() { n.deliver(f) })
}

func (n *Network) deliver(f Flight) {
	n.mu.Lock()
	h := n.handlers[f.Msg.To]
	switch {
	case n.down[f.Msg.To] || h == nil:
		f.Reason = "down"
	case !n.connectedLocked(f.Msg.From, f.Msg.To):
		f.Reason = "partition" // the partition appeared while it was in flight
	}
	n.mu.Unlock()
	if f.Reason != "" {
		if n.obs.OnDrop != nil {
			n.obs.OnDrop(f)
		}
		return
	}
	if n.obs.OnDeliver != nil {
		n.obs.OnDeliver(f)
	}
	h(f.Msg)
}
