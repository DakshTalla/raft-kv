package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// Cluster membership changes, one server at a time (Raft thesis §4.1).
//
// The configuration — which nodes vote — is itself stored in the log as a
// special entry (EntryConfig). Rules:
//
//  1. A node always uses the LATEST configuration in its log, committed or
//     not. (If that entry is later truncated, it falls back to the previous
//     one.)
//  2. Only one change may be in progress: the leader refuses a new change
//     until the previous configuration entry has committed.
//  3. Each change adds or removes exactly ONE voter.
//
// WHY one at a time is safe: when you add or remove a single server, every
// majority of the old configuration overlaps every majority of the new one
// (e.g. 3 of {1..5} and 4 of {1..6} must share a node). So there is no moment
// at which two disjoint majorities could elect two leaders — which is exactly
// what can go wrong when you switch from {1,2,3} to {1,4,5} in one step, and
// why arbitrary changes need the more complex "joint consensus".
//
// Adding a node happens in two phases:
//
//	learner:  the leader replicates the log (or a snapshot) to the new node,
//	          but it does not vote and does not count toward commits.
//	promote:  once it has caught up to the commit index, the leader appends
//	          the configuration entry that makes it a voter.
//
// WHY the learner phase: a brand-new node has an empty log. If it became a
// voter immediately, a 3-node cluster would turn into a 4-node cluster that
// needs 3 acks — with one node unable to ack anything for a while — so a
// single additional failure would stall all writes.

// EntryType distinguishes client commands from configuration changes.
type EntryType int

const (
	EntryNormal EntryType = iota
	EntryConfig           // Data is a JSON list of voter ids
)

// Errors returned by AddMember / RemoveMember.
var (
	ErrNotLeader     = errors.New("not the leader")
	ErrChangePending = errors.New("another membership change is still in progress")
)

// AddMember starts adding node id to the cluster. The node is first caught
// up as a learner and promoted to voter automatically; watch Status().Voters
// to see when that has committed.
func (n *Node) AddMember(id int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.checkCanChange(); err != nil {
		return err
	}
	if n.isVoter(id) {
		return fmt.Errorf("node %d is already a member", id)
	}
	if len(n.learners) > 0 {
		return ErrChangePending
	}
	n.learners[id] = true
	n.next[id] = n.lastIndex() + 1
	n.match[id] = 0
	n.inflight[id] = false
	n.emit(Event{Kind: "learner", Peer: id, Detail: "catching up before it may vote"})
	n.sendAppend(id)
	n.ready()
	return nil
}

// RemoveMember removes voter id. If id is this leader, it keeps leading until
// the change commits and then steps down.
func (n *Node) RemoveMember(id int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.checkCanChange(); err != nil {
		return err
	}
	if !n.isVoter(id) {
		return fmt.Errorf("node %d is not a member", id)
	}
	if len(n.voters) == 1 {
		return errors.New("cannot remove the last member")
	}
	n.appendConfig(slices.DeleteFunc(slices.Clone(n.voters), func(v int) bool { return v == id }))
	n.ready()
	return nil
}

func (n *Node) checkCanChange() error {
	if n.stopped || n.role != Leader {
		return ErrNotLeader
	}
	if n.configIndex > n.commitIndex {
		return ErrChangePending
	}
	// WHY: a new leader must commit an entry of its own term (its no-op)
	// before changing the configuration. Otherwise an uncommitted change from
	// the previous leader and this one could overlap — a known bug in the
	// original single-server algorithm, fixed in Ongaro's errata.
	if t, _ := n.termAt(n.commitIndex); t != n.term {
		return ErrChangePending
	}
	return nil
}

// maybePromote turns a learner into a voter once it has caught up.
func (n *Node) maybePromote(peer int) {
	if !n.learners[peer] || n.match[peer] < n.commitIndex || n.checkCanChange() != nil {
		return
	}
	delete(n.learners, peer)
	voters := append(slices.Clone(n.voters), peer)
	slices.Sort(voters)
	n.appendConfig(voters)
}

// appendConfig appends a configuration entry and starts using it at once
// (rule 1: a node uses the latest configuration in its log).
func (n *Node) appendConfig(voters []int) {
	data, _ := json.Marshal(voters)
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Type: EntryConfig, Data: data}
	n.log = append(n.log, e)
	n.recomputeConfig()
	n.emit(Event{Kind: "append", Index: e.Index, Detail: "config " + fmt.Sprint(voters)})
	n.broadcastAppend(false)
	if len(n.others()) == 0 {
		n.maybeCommit()
	}
}

// recomputeConfig sets voters from the newest configuration entry in the
// log, falling back to the configuration stored with the snapshot (or the
// initial one). Called whenever config entries are appended or truncated.
func (n *Node) recomputeConfig() {
	n.configDirty = false
	n.voters, n.configIndex = n.baseVoters, n.log[0].Index
	for i := len(n.log) - 1; i >= 1; i-- {
		if n.log[i].Type == EntryConfig {
			n.voters, n.configIndex = decodeVoters(n.log[i].Data), n.log[i].Index
			break
		}
	}
	for id := range n.learners {
		if n.isVoter(id) {
			delete(n.learners, id)
		}
	}
}

// configAt returns the configuration in effect at log index idx (used when
// taking a snapshot at idx).
func (n *Node) configAt(idx uint64) []int {
	first := n.log[0].Index
	for i := idx; i > first; i-- {
		if e := n.log[i-first]; e.Type == EntryConfig {
			return decodeVoters(e.Data)
		}
	}
	return n.baseVoters
}

func decodeVoters(data []byte) []int {
	var v []int
	json.Unmarshal(data, &v)
	slices.Sort(v)
	return v
}

func (n *Node) isVoter(id int) bool { return slices.Contains(n.voters, id) }

// others returns the voters other than this node: who to ask for votes.
func (n *Node) others() []int {
	out := make([]int, 0, len(n.voters))
	for _, v := range n.voters {
		if v != n.id {
			out = append(out, v)
		}
	}
	return out
}

// replicas returns every node the leader sends the log to: voters and learners.
func (n *Node) replicas() []int {
	out := n.others()
	for id := range n.learners {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// countVoters counts the "yes" answers that come from current voters.
func (n *Node) countVoters(answers map[int]bool) int {
	c := 0
	for id, ok := range answers {
		if ok && n.isVoter(id) {
			c++
		}
	}
	return c
}

// DescribeConfig renders a configuration entry's payload ("CONFIG 1,2,3").
func DescribeConfig(data []byte) string {
	v := decodeVoters(data)
	s := "CONFIG "
	for i, id := range v {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprint(id)
	}
	return s
}
