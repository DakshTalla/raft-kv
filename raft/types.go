// Package raft is a from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout, 2014): leader election, log replication,
// commitment, persistence and log compaction via snapshots.
//
// WHY a message-passing design (like etcd/raft) instead of blocking RPC calls
// (like the MIT 6.824 labs): every RPC request and every RPC response is a
// plain Message value handed to a Send function. The node never blocks on the
// network. That makes the core easy to drive from a simulated network (for
// chaos testing and for the live visualizer, which wants to see every single
// message), and keeps all state changes inside two entry points: Tick() and
// Step(msg).
package raft

import "fmt"

// Role is the state a Raft server is in (paper §5.1, Figure 4).
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("role(%d)", int(r))
}

// Entry is one slot of the replicated log. Data == nil means a no-op entry,
// which a new leader appends to commit entries from earlier terms (§5.4.2).
type Entry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data,omitempty"`
}

// Snapshot is a compacted prefix of the log: the state machine state after
// applying every entry up to and including LastIndex (§7).
type Snapshot struct {
	LastIndex uint64 `json:"lastIndex"`
	LastTerm  uint64 `json:"lastTerm"`
	Data      []byte `json:"data"`
}

// MsgType enumerates the three RPCs of Raft and their responses.
type MsgType int

const (
	MsgVote        MsgType = iota // RequestVote request
	MsgVoteResp                   // RequestVote response
	MsgApp                        // AppendEntries request (also the heartbeat)
	MsgAppResp                    // AppendEntries response
	MsgSnap                       // InstallSnapshot request
	MsgSnapResp                   // InstallSnapshot response
	MsgPreVote                    // "would you vote for me?" — asked before a real election
	MsgPreVoteResp                // answer to MsgPreVote
)

func (t MsgType) String() string {
	switch t {
	case MsgVote:
		return "RequestVote"
	case MsgVoteResp:
		return "RequestVoteResp"
	case MsgApp:
		return "AppendEntries"
	case MsgAppResp:
		return "AppendEntriesResp"
	case MsgSnap:
		return "InstallSnapshot"
	case MsgSnapResp:
		return "InstallSnapshotResp"
	case MsgPreVote:
		return "PreVote"
	case MsgPreVoteResp:
		return "PreVoteResp"
	}
	return fmt.Sprintf("msg(%d)", int(t))
}

// IsResponse reports whether t is a reply to another message.
func (t MsgType) IsResponse() bool {
	return t == MsgVoteResp || t == MsgAppResp || t == MsgSnapResp || t == MsgPreVoteResp
}

// Message is a single RPC request or response. One struct covers all six
// kinds; only the fields relevant to Type are set.
type Message struct {
	Type MsgType
	From int
	To   int
	Term uint64 // sender's current term — every message carries it (§5.1)

	// RequestVote: the candidate's last log position, used for the
	// "at least as up-to-date" check (§5.4.1).
	LastLogIndex uint64
	LastLogTerm  uint64

	// AppendEntries
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64

	// Responses. Success = vote granted / append accepted.
	Success bool
	// AppendEntriesResp on success: highest index known to match the leader.
	MatchIndex uint64
	// AppendEntriesResp on failure: hints so the leader can skip back a whole
	// term at a time instead of one entry per round trip (§5.3, "fast backup").
	ConflictIndex uint64
	ConflictTerm  uint64

	// InstallSnapshot
	Snapshot *Snapshot
}

// IsHeartbeat reports whether m is an AppendEntries carrying no entries.
func (m Message) IsHeartbeat() bool { return m.Type == MsgApp && len(m.Entries) == 0 }

// StateMachine is the application replicated by Raft (here: the KV store).
// Apply is called exactly once per committed entry, in log order.
type StateMachine interface {
	Apply(e Entry)
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Event is an observable thing that happened inside a node. The cluster turns
// these into the WebSocket stream that drives the visualizer.
type Event struct {
	Kind   string // "role", "prevote", "vote", "append", "commit", "snapshot", "restore", "truncate"
	Node   int
	Term   uint64
	Role   string
	Index  uint64
	Peer   int
	Detail string
}
