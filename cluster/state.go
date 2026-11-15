package cluster

import (
	"context"
	"time"

	"github.com/DakshTalla/raft-kv/events"
	"github.com/DakshTalla/raft-kv/kv"
	"github.com/DakshTalla/raft-kv/raft"
)

// logTail is how many trailing log entries per node the UI receives.
const logTail = 24

// LogEntry is a log entry rendered for display.
type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Cmd   string `json:"cmd"`
}

// NodeState is everything the UI shows about one node.
type NodeState struct {
	raft.Status
	Alive bool       `json:"alive"`
	Group int        `json:"group"` // partition group (0 = no partition)
	Log   []LogEntry `json:"log"`   // shadows Status.Log with readable commands
	Keys  int        `json:"keys"`
}

// NetworkState describes the current fault-injection settings.
type NetworkState struct {
	LatencyMinMs float64 `json:"latencyMinMs"`
	LatencyMaxMs float64 `json:"latencyMaxMs"`
	DropRate     float64 `json:"dropRate"`
	Partitioned  bool    `json:"partitioned"`
}

// State is the full cluster snapshot pushed to the UI ten times a second.
type State struct {
	Now     float64      `json:"now"` // virtual ms
	Scale   float64      `json:"scale"`
	Paused  bool         `json:"paused"`
	TickMs  float64      `json:"tickMs"`
	Leader  int          `json:"leader"`
	Nodes   []NodeState  `json:"nodes"`
	Network NetworkState `json:"network"`
	KV      []kv.Pair    `json:"kv"` // contents of the leader's store (first 100 keys)
	KVTotal int          `json:"kvTotal"`
}

// State builds a snapshot of the whole cluster.
func (c *Cluster) State() State {
	s := State{
		Now: ms(c.Clock.Now()), Scale: c.Clock.Scale(), Paused: c.Clock.Paused(),
		TickMs: ms(c.opts.Tick), Leader: c.Leader(),
	}
	groups := c.Net.Groups()
	seen := map[int]bool{}
	for _, g := range groups {
		seen[g] = true
	}
	s.Network.Partitioned = len(seen) > 1
	lmin, lmax, drop := c.Net.Conditions()
	s.Network.LatencyMinMs, s.Network.LatencyMaxMs, s.Network.DropRate = ms(lmin), ms(lmax), drop

	for _, id := range c.ids {
		st, alive := c.Status(id, logTail)
		ns := NodeState{Status: st, Alive: alive, Log: make([]LogEntry, len(st.Log))}
		if s.Network.Partitioned {
			ns.Group = groups[id]
		}
		for i, e := range st.Log {
			ns.Log[i] = LogEntry{Index: e.Index, Term: e.Term, Cmd: kv.Describe(e.Data)}
		}
		ns.Status.Log = nil
		if store := c.Store(id); store != nil && alive {
			_, ns.Keys = store.Dump(0)
		}
		s.Nodes = append(s.Nodes, ns)
	}
	// Show the KV contents of the leader, or of the most advanced live node.
	src := s.Leader
	if src == 0 {
		var best uint64
		for _, n := range s.Nodes {
			if n.Alive && n.LastApplied >= best {
				src, best = n.ID, n.LastApplied
			}
		}
	}
	if store := c.Store(src); store != nil {
		s.KV, s.KVTotal = store.Dump(100)
	}
	return s
}

// broadcastState pushes the cluster state to subscribers at 10Hz (real time).
func (c *Cluster) broadcastState(ctx context.Context) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if c.Hub.Active() {
				st := c.State()
				c.Hub.Publish(events.Event{Kind: "state", T: st.Now, State: st})
			}
		}
	}
}
