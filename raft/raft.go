package raft

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"time"
)

// Config configures one Raft node. Time is measured in ticks: the owner calls
// Tick() at a fixed interval (10ms of simulated time in this project).
type Config struct {
	ID int
	// Peers is the initial configuration: every member, including ID. It is
	// only used until the log or a snapshot says otherwise.
	Peers []int
	// Join: this node is being added to a running cluster. It starts with no
	// configuration and waits for the leader to send it one.
	Join bool

	// ElectionTicks is the minimum election timeout. The actual timeout is
	// randomized in [ElectionTicks, 2*ElectionTicks).
	ElectionTicks  int
	HeartbeatTicks int
	// MaxBatch caps the number of entries in one AppendEntries message.
	MaxBatch int
	// SnapshotThreshold: take a snapshot once this many applied entries have
	// accumulated in the log. 0 disables log compaction.
	SnapshotThreshold uint64

	// NoPreVote and NoCheckQuorum switch off the two liveness extensions
	// described at startPreVote and checkQuorum. Both are on by default.
	NoPreVote     bool
	NoCheckQuorum bool

	Storage      *Storage // nil = in-memory only (no crash recovery)
	StateMachine StateMachine
	Send         func(Message) // must not block; called with the node lock held
	OnEvent      func(Event)   // optional; must not block
	Seed         int64
}

// Node is a single Raft server. All methods are safe for concurrent use.
type Node struct {
	mu  sync.Mutex
	cfg Config
	id  int
	// Cluster configuration (see membership.go).
	voters      []int        // who votes now: from the newest config entry in the log
	baseVoters  []int        // configuration at the snapshot point (or the initial one)
	configIndex uint64       // index of the entry that defined voters
	configDirty bool         // config entries were appended or truncated
	learners    map[int]bool // leader only: nodes catching up before they may vote
	rng         *rand.Rand

	// Persistent state (written to Storage before any message is sent).
	term     uint64
	votedFor int
	// log[0] is a sentinel holding the index/term of the last entry covered
	// by the snapshot (0/0 if none). WHY a sentinel: prevLogIndex/prevLogTerm
	// checks never need a special case for "the entry before the first one".
	log      []Entry
	snapshot *Snapshot

	// Volatile state.
	role        Role
	leader      int
	commitIndex uint64
	lastApplied uint64
	votes       map[int]bool // candidate: who answered, and how
	preVoting   bool         // follower that is asking "could I win an election?"
	preVotes    map[int]bool

	// Leader-only volatile state (§5.3).
	next           map[int]uint64
	match          map[int]uint64
	inflight       map[int]bool // an AppendEntries to that peer awaits its reply
	active         map[int]bool // peers heard from since the last quorum check
	quorumElapsed  int
	stepDownReason string // shown in the event stream when a leader steps down

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	outbox      []Message
	savedHS     HardState
	stable      uint64 // WAL holds exactly log[1..stable]
	needRewrite bool   // WAL holds entries we have since truncated
	stopped     bool
}

// NewNode creates a node, restoring persisted state from cfg.Storage if any.
func NewNode(cfg Config) (*Node, error) {
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 15
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 5
	}
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 128
	}
	if cfg.HeartbeatTicks >= cfg.ElectionTicks {
		return nil, fmt.Errorf("heartbeat (%d ticks) must be shorter than the election timeout (%d ticks)", cfg.HeartbeatTicks, cfg.ElectionTicks)
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano() + int64(cfg.ID)*7919
	}
	n := &Node{
		cfg:      cfg,
		id:       cfg.ID,
		rng:      rand.New(rand.NewSource(seed)),
		log:      []Entry{{Index: 0, Term: 0}},
		votes:    map[int]bool{},
		next:     map[int]uint64{},
		match:    map[int]uint64{},
		inflight: map[int]bool{},
		active:   map[int]bool{},
		preVotes: map[int]bool{},
		learners: map[int]bool{},
	}
	if !cfg.Join {
		n.baseVoters = slices.Clone(cfg.Peers)
		slices.Sort(n.baseVoters)
	}
	if cfg.Storage != nil {
		hs, snap, entries, err := cfg.Storage.Load()
		if err != nil {
			return nil, err
		}
		n.term, n.votedFor, n.savedHS = hs.Term, hs.VotedFor, hs
		if snap != nil {
			n.snapshot = snap
			n.log[0] = Entry{Index: snap.LastIndex, Term: snap.LastTerm}
			if len(snap.Voters) > 0 {
				n.baseVoters = snap.Voters
			}
			if err := cfg.StateMachine.Restore(snap.Data); err != nil {
				return nil, err
			}
			// Everything in a snapshot is committed by definition.
			n.commitIndex, n.lastApplied = snap.LastIndex, snap.LastIndex
		}
		for _, e := range entries {
			if e.Index != n.lastIndex()+1 {
				break // gap: ignore the rest (cannot happen with our WAL)
			}
			n.log = append(n.log, e)
		}
		n.stable = n.lastIndex()
		// WHY commitIndex is not persisted: it is volatile in the paper. After
		// a restart the leader's next AppendEntries tells us how far we can
		// apply, and the KV store is rebuilt by re-applying from the snapshot.
	}
	n.recomputeConfig()
	n.resetElectionTimeout()
	return n, nil
}

// ---------------------------------------------------------------------------
// Public API

// Tick advances the node's logical clock by one tick.
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend(true)
		}
		n.quorumElapsed++
		if n.quorumElapsed >= n.cfg.ElectionTicks {
			n.quorumElapsed = 0
			n.checkQuorum()
		}
	} else if !n.isVoter(n.id) {
		// Not a voting member (a learner still catching up, or a node that
		// was removed): it must never start an election.
	} else {
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			if n.cfg.NoPreVote {
				n.campaign()
			} else {
				n.startPreVote()
			}
		}
	}
	n.ready()
}

// Step processes one incoming message.
func (n *Node) Step(m Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	// Pre-votes are handled before the term rule below on purpose: they
	// carry a term the sender has NOT adopted yet, so they must never make
	// us change our own term.
	if m.Type == MsgPreVote {
		n.handlePreVote(m)
		n.ready()
		return
	}
	if m.Type == MsgPreVoteResp {
		n.handlePreVoteResp(m)
		n.ready()
		return
	}
	// Rule for all servers (§5.1): a higher term means we are out of date.
	if m.Term > n.term {
		leader := 0
		if m.Type == MsgApp || m.Type == MsgSnap {
			leader = m.From
		}
		n.becomeFollower(m.Term, leader)
	} else if m.Term < n.term {
		// Stale sender. Reply to requests with our term so a deposed leader or
		// candidate learns it must step down; drop stale responses.
		switch m.Type {
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From})
		case MsgApp:
			n.send(Message{Type: MsgAppResp, To: m.From})
		case MsgSnap:
			n.send(Message{Type: MsgSnapResp, To: m.From})
		}
		n.ready()
		return
	}

	if n.role == Leader {
		n.active[m.From] = true // any current-term message proves the peer can reach us
	}
	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.handleAppend(m)
	case MsgAppResp:
		n.handleAppendResp(m)
	case MsgSnap:
		n.handleSnapshot(m)
	case MsgSnapResp:
		n.handleSnapshotResp(m)
	}
	n.ready()
}

// Propose appends a client command to the leader's log. It returns the index
// the entry will occupy if committed. isLeader=false means the caller must
// retry on another node (see Leader() for a hint).
func (n *Node) Propose(data []byte) (index, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.role != Leader {
		return 0, n.term, false
	}
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: data}
	n.log = append(n.log, e)
	n.emit(Event{Kind: "append", Index: e.Index})
	// WHY broadcastAppend(false): only peers with no AppendEntries in flight
	// get a message now. Peers that are busy will receive this entry batched
	// together with others when their reply arrives. This is what makes
	// throughput scale with concurrency instead of one RPC per command.
	n.broadcastAppend(false)
	if len(n.others()) == 0 {
		n.maybeCommit()
	}
	n.ready()
	return e.Index, e.Term, true
}

// Stop simulates a crash: the node stops processing and closes its storage.
// In-memory state is lost; a new Node built from the same Storage directory
// recovers the persisted state.
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stopped = true
	if n.cfg.Storage != nil {
		n.cfg.Storage.Close()
	}
}

// Leader returns the id of the leader this node believes in (0 = unknown).
func (n *Node) Leader() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// Status is a read-only view of a node, used by the API and the visualizer.
type Status struct {
	ID               int            `json:"id"`
	Role             string         `json:"role"`
	Term             uint64         `json:"term"`
	VotedFor         int            `json:"votedFor"`
	Leader           int            `json:"leader"`
	CommitIndex      uint64         `json:"commitIndex"`
	LastApplied      uint64         `json:"lastApplied"`
	LastIndex        uint64         `json:"lastIndex"`
	LastTerm         uint64         `json:"lastTerm"`
	SnapshotIndex    uint64         `json:"snapshotIndex"`
	SnapshotTerm     uint64         `json:"snapshotTerm"`
	ElectionElapsed  int            `json:"electionElapsed"`
	ElectionTimeout  int            `json:"electionTimeout"`
	HeartbeatElapsed int            `json:"heartbeatElapsed"`
	HeartbeatTicks   int            `json:"heartbeatTicks"`
	PreVoting        bool           `json:"preVoting,omitempty"`
	Votes            map[int]bool   `json:"votes,omitempty"`
	NextIndex        map[int]uint64 `json:"nextIndex,omitempty"`
	MatchIndex       map[int]uint64 `json:"matchIndex,omitempty"`
	Voters           []int          `json:"voters"`
	ConfigIndex      uint64         `json:"configIndex"`
	Learners         []int          `json:"learners,omitempty"`
	Log              []Entry        `json:"log"` // the last `tail` entries
}

// Status returns a snapshot of the node's state including the last tail log
// entries (tail < 0 means the whole log).
func (n *Node) Status(tail int) Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := Status{
		ID: n.id, Role: n.role.String(), Term: n.term, VotedFor: n.votedFor,
		Leader: n.leader, CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastIndex: n.lastIndex(), LastTerm: n.lastTerm(),
		SnapshotIndex: n.log[0].Index, SnapshotTerm: n.log[0].Term,
		ElectionElapsed: n.electionElapsed, ElectionTimeout: n.electionTimeout,
		HeartbeatElapsed: n.heartbeatElapsed, HeartbeatTicks: n.cfg.HeartbeatTicks,
		PreVoting:   n.preVoting,
		Voters:      slices.Clone(n.voters),
		ConfigIndex: n.configIndex,
	}
	for id := range n.learners {
		s.Learners = append(s.Learners, id)
	}
	slices.Sort(s.Learners)
	if n.role == Candidate {
		s.Votes = copyMap(n.votes)
	}
	if n.role == Leader {
		s.NextIndex = copyMap(n.next)
		s.MatchIndex = copyMap(n.match)
		s.MatchIndex[n.id] = n.lastIndex()
	}
	body := n.log[1:]
	if tail >= 0 && len(body) > tail {
		body = body[len(body)-tail:]
	}
	s.Log = append([]Entry(nil), body...)
	return s
}

// ---------------------------------------------------------------------------
// Elections (§5.2)

func (n *Node) resetElectionTimeout() {
	// WHY randomized: if every follower timed out at the same moment they
	// would all become candidates, split the vote, and retry forever.
	// Randomization makes it likely that one node times out first and wins.
	n.electionTimeout = n.cfg.ElectionTicks + n.rng.Intn(n.cfg.ElectionTicks)
}

func (n *Node) becomeFollower(term uint64, leader int) {
	changed := n.role != Follower || term != n.term
	if term > n.term {
		n.term = term
		n.votedFor = 0 // new term: we have not voted yet
	}
	n.role = Follower
	n.leader = leader
	n.preVoting = false
	clear(n.learners)
	n.electionElapsed = 0
	n.resetElectionTimeout()
	if changed {
		n.emit(Event{Kind: "role", Role: "follower", Detail: n.stepDownReason})
	}
	n.stepDownReason = ""
}

// startPreVote runs when the election timer fires. Instead of starting an
// election straight away, the node first asks its peers whether they WOULD
// vote for it, without touching its own term (Raft thesis §9.6).
//
// WHY: a node cut off from the cluster keeps timing out. Without pre-vote it
// would bump its term on every timeout, and when the network heals its
// inflated term would force the perfectly healthy leader to step down. With
// pre-vote it cannot gather a majority of "yes" while isolated, so its term
// never moves and its return is a non-event.
func (n *Node) startPreVote() {
	n.preVoting = true
	n.leader = 0
	n.preVotes = map[int]bool{n.id: true}
	n.electionElapsed = 0
	n.resetElectionTimeout()
	n.emit(Event{Kind: "prevote"})
	if n.quorum() == 1 {
		n.campaign()
		return
	}
	for _, p := range n.others() {
		// Term is the one we would campaign in, not our current term.
		n.send(Message{Type: MsgPreVote, To: p, Term: n.term + 1, LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm()})
	}
}

func (n *Node) handlePreVote(m Message) {
	upToDate := m.LastLogTerm > n.lastTerm() ||
		(m.LastLogTerm == n.lastTerm() && m.LastLogIndex >= n.lastIndex())
	// "Do I still have a leader?" If we heard a heartbeat recently, the
	// cluster is fine from where we stand and an election would only disrupt
	// it. Two missed heartbeats count as "the leader is gone".
	quiet := n.cfg.ElectionTicks - n.cfg.HeartbeatTicks
	leaderAlive := n.role == Leader || (n.leader != 0 && n.electionElapsed < quiet)
	grant := m.Term > n.term && upToDate && !leaderAlive
	// Nothing is recorded: a pre-vote is not a promise, so it needs no disk
	// write and does not reset our election timer.
	resp := Message{Type: MsgPreVoteResp, To: m.From, Success: grant, Term: n.term}
	if grant {
		resp.Term = m.Term // echo, so the asker can match it to this round
	}
	n.send(resp)
}

func (n *Node) handlePreVoteResp(m Message) {
	if !m.Success && m.Term > n.term {
		n.becomeFollower(m.Term, 0) // we are behind; catch up on the term
		return
	}
	if !n.preVoting || !m.Success || m.Term != n.term+1 {
		return
	}
	n.preVotes[m.From] = true
	if n.countVoters(n.preVotes) >= n.quorum() {
		n.campaign() // a majority would vote for us: now run the real election
	}
}

// checkQuorum makes a leader step down if it has not heard from a majority
// for a whole election timeout.
//
// WHY: a leader that is partitioned away from the majority would otherwise
// keep believing it leads forever. It can never commit anything, so safety is
// not at stake, but its clients would hang until they time out. Stepping down
// lets them fail fast and find the real leader.
func (n *Node) checkQuorum() {
	if n.cfg.NoCheckQuorum {
		return
	}
	heard := 0
	if n.isVoter(n.id) {
		heard = 1 // ourselves
	}
	for _, p := range n.others() {
		if n.active[p] {
			heard++
		}
		n.active[p] = false
	}
	if heard < n.quorum() {
		n.stepDownReason = "lost contact with a majority"
		n.becomeFollower(n.term, 0)
	}
}

func (n *Node) campaign() {
	n.preVoting = false
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.leader = 0
	n.votes = map[int]bool{n.id: true}
	n.electionElapsed = 0
	n.resetElectionTimeout()
	n.emit(Event{Kind: "role", Role: "candidate"})
	if n.quorum() == 1 {
		n.becomeLeader()
		return
	}
	for _, p := range n.others() {
		n.send(Message{Type: MsgVote, To: p, LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm()})
	}
}

func (n *Node) handleVote(m Message) {
	// Election restriction (§5.4.1): only vote for a candidate whose log is at
	// least as up-to-date as ours. WHY: a leader must already contain every
	// committed entry, because Raft never sends entries from follower to
	// leader. Since a committed entry lives on a majority and a candidate needs
	// a majority of votes, at least one voter has the entry and refuses any
	// candidate that lacks it.
	upToDate := m.LastLogTerm > n.lastTerm() ||
		(m.LastLogTerm == n.lastTerm() && m.LastLogIndex >= n.lastIndex())
	grant := (n.votedFor == 0 || n.votedFor == m.From) && upToDate
	if grant {
		n.votedFor = m.From
		n.electionElapsed = 0 // granting a vote also resets our timer
		n.emit(Event{Kind: "vote", Peer: m.From})
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Success: grant})
}

func (n *Node) handleVoteResp(m Message) {
	if n.role != Candidate {
		return
	}
	n.votes[m.From] = m.Success
	if n.countVoters(n.votes) >= n.quorum() {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.heartbeatElapsed = 0
	n.quorumElapsed = 0
	for _, p := range n.replicas() {
		n.next[p] = n.lastIndex() + 1
		n.match[p] = 0
		n.inflight[p] = false
		n.active[p] = true // the election itself proved we can reach a majority
	}
	n.emit(Event{Kind: "role", Role: "leader"})
	// WHY a no-op entry: a leader may only commit entries from its own term by
	// counting replicas (§5.4.2, Figure 8). Appending an empty entry in the new
	// term lets it commit — and thereby also commit every older entry before it
	// — right away, instead of waiting for the next client write.
	noop := Entry{Index: n.lastIndex() + 1, Term: n.term}
	n.log = append(n.log, noop)
	n.emit(Event{Kind: "append", Index: noop.Index, Detail: "no-op"})
	n.broadcastAppend(true)
	if len(n.others()) == 0 {
		n.maybeCommit()
	}
}

// ---------------------------------------------------------------------------
// Log replication (§5.3)

func (n *Node) broadcastAppend(heartbeat bool) {
	for _, p := range n.replicas() {
		// WHY resend on heartbeat even if a request is in flight: the in-flight
		// message (or its reply) may have been dropped. The heartbeat doubles as
		// the retransmission timer, so we need no per-message timeouts.
		if heartbeat || !n.inflight[p] {
			n.sendAppend(p)
		}
	}
}

func (n *Node) sendAppend(to int) {
	first := n.log[0].Index
	next := n.next[to]
	if next <= first {
		// The entries this follower needs were compacted away (§7).
		n.send(Message{Type: MsgSnap, To: to, Snapshot: n.snapshot})
		n.inflight[to] = true
		return
	}
	prev := next - 1
	prevTerm, _ := n.termAt(prev)
	end := n.lastIndex()
	if limit := next + uint64(n.cfg.MaxBatch) - 1; end > limit {
		end = limit
	}
	var entries []Entry
	if next <= end {
		// Copy: the message outlives this call and our log may be truncated.
		entries = append([]Entry(nil), n.log[next-first:end-first+1]...)
	}
	n.send(Message{
		Type: MsgApp, To: to, PrevLogIndex: prev, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: n.commitIndex,
	})
	n.inflight[to] = true
}

func (n *Node) handleAppend(m Message) {
	if n.role == Leader {
		// Same term, two leaders: Election Safety is broken. This can only
		// happen through a bug, so fail loudly (the tests rely on this).
		panic(fmt.Sprintf("raft: node %d and node %d are both leader in term %d", n.id, m.From, n.term))
	}
	if n.role == Candidate {
		n.becomeFollower(n.term, m.From) // someone else won this term
	}
	n.leader = m.From
	n.preVoting = false
	n.electionElapsed = 0

	resp := Message{Type: MsgAppResp, To: m.From}
	first := n.log[0].Index

	// Consistency check: do we have the entry right before the new ones?
	if m.PrevLogIndex > n.lastIndex() {
		resp.ConflictIndex = n.lastIndex() + 1 // "my log is too short"
		n.send(resp)
		return
	}
	if m.PrevLogIndex >= first {
		if t, _ := n.termAt(m.PrevLogIndex); t != m.PrevLogTerm {
			// Report the conflicting term and the first index we hold for it,
			// so the leader can jump over the whole term in one round trip.
			ci := m.PrevLogIndex
			for ci-1 > first {
				if pt, _ := n.termAt(ci - 1); pt != t {
					break
				}
				ci--
			}
			resp.ConflictTerm, resp.ConflictIndex = t, ci
			n.send(resp)
			return
		}
	}
	// (If PrevLogIndex < first it lies inside our snapshot: committed, so it
	// matches by the Log Matching property.)

	for i, e := range m.Entries {
		if e.Index <= first {
			continue // already compacted into our snapshot
		}
		if e.Index <= n.lastIndex() {
			if t, _ := n.termAt(e.Index); t == e.Term {
				continue // already have it (duplicate / retransmission)
			}
			// Conflict: delete this entry and everything after it (§5.3).
			if e.Index <= n.commitIndex {
				panic(fmt.Sprintf("raft: node %d asked to truncate committed index %d", n.id, e.Index))
			}
			n.truncateFrom(e.Index)
		}
		n.log = append(n.log, m.Entries[i:]...)
		for _, ne := range m.Entries[i:] {
			if ne.Type == EntryConfig {
				n.configDirty = true // rule 1: use the new configuration right away
			}
		}
		break
	}

	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if m.LeaderCommit > n.commitIndex {
		// WHY min(…, lastNew): we may only trust entries this leader has just
		// confirmed match its log; anything beyond lastNew could be stale.
		c := min(m.LeaderCommit, lastNew)
		if c > n.commitIndex {
			n.commitIndex = c
			n.emit(Event{Kind: "commit", Index: c})
		}
	}
	resp.Success = true
	resp.MatchIndex = lastNew
	n.send(resp)
}

func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader {
		return
	}
	n.inflight[m.From] = false
	if m.Success {
		n.advance(m.From, m.MatchIndex)
	} else {
		next := m.ConflictIndex
		if m.ConflictTerm != 0 {
			if idx, ok := n.lastIndexOfTerm(m.ConflictTerm); ok {
				next = idx + 1
			}
		}
		next = max(next, n.match[m.From]+1, 1)
		n.next[m.From] = next
	}
	if !m.Success || n.next[m.From] <= n.lastIndex() {
		n.sendAppend(m.From) // keep the pipe full / retry with the new nextIndex
	}
}

// advance records that peer's log matches ours up to index.
func (n *Node) advance(peer int, index uint64) {
	if index > n.match[peer] {
		n.match[peer] = index
	}
	if n.match[peer]+1 > n.next[peer] {
		n.next[peer] = n.match[peer] + 1
	}
	n.maybeCommit()
	n.maybePromote(peer)
}

func (n *Node) maybeCommit() {
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		t, _ := n.termAt(idx)
		if t < n.term {
			// WHY stop here: entries from older terms are never committed by
			// counting replicas (Figure 8 shows how that loses data). They get
			// committed indirectly once an entry of the current term commits.
			break
		}
		count := 0
		if n.isVoter(n.id) {
			count = 1 // ourselves (a leader that is removing itself does not count)
		}
		for _, p := range n.others() {
			if n.match[p] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			n.emit(Event{Kind: "commit", Index: idx})
			// Followers learn the new commit index on the next AppendEntries
			// (piggy-backed); clients only wait for the leader to apply.
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Snapshots / log compaction (§7)

func (n *Node) maybeSnapshot() {
	th := n.cfg.SnapshotThreshold
	if th == 0 || n.lastApplied-n.log[0].Index < th {
		return
	}
	data, err := n.cfg.StateMachine.Snapshot()
	if err != nil {
		panic(fmt.Sprintf("raft: node %d snapshot failed: %v", n.id, err))
	}
	idx := n.lastApplied
	t, _ := n.termAt(idx)
	snap := &Snapshot{LastIndex: idx, LastTerm: t, Voters: n.configAt(idx), Data: data}
	first := n.log[0].Index
	rest := append([]Entry{{Index: idx, Term: t}}, n.log[idx-first+1:]...)
	n.log = rest
	n.snapshot = snap
	if n.cfg.Storage != nil {
		must(n.cfg.Storage.SaveSnapshot(*snap, n.log[1:]))
		n.stable, n.needRewrite = n.lastIndex(), false
	}
	n.emit(Event{Kind: "snapshot", Index: idx})
}

func (n *Node) handleSnapshot(m Message) {
	if n.role == Candidate {
		n.becomeFollower(n.term, m.From)
	}
	n.leader = m.From
	n.preVoting = false
	n.electionElapsed = 0
	s := m.Snapshot
	resp := Message{Type: MsgSnapResp, To: m.From, Success: true, MatchIndex: s.LastIndex}
	if s.LastIndex <= n.commitIndex {
		n.send(resp) // we already have everything it contains
		return
	}
	var rest []Entry
	if t, ok := n.termAt(s.LastIndex); ok && t == s.LastTerm {
		// Our log extends past the snapshot and agrees with it: keep the tail.
		first := n.log[0].Index
		rest = append(rest, n.log[s.LastIndex-first+1:]...)
	}
	n.log = append([]Entry{{Index: s.LastIndex, Term: s.LastTerm}}, rest...)
	n.snapshot = s
	if len(s.Voters) > 0 {
		n.baseVoters = s.Voters
	}
	n.configDirty = true
	if err := n.cfg.StateMachine.Restore(s.Data); err != nil {
		panic(fmt.Sprintf("raft: node %d restore failed: %v", n.id, err))
	}
	n.commitIndex, n.lastApplied = s.LastIndex, s.LastIndex
	if n.cfg.Storage != nil {
		must(n.cfg.Storage.SaveSnapshot(*s, n.log[1:]))
		n.stable, n.needRewrite = n.lastIndex(), false
	}
	n.emit(Event{Kind: "restore", Index: s.LastIndex})
	n.send(resp)
}

func (n *Node) handleSnapshotResp(m Message) {
	if n.role != Leader {
		return
	}
	n.inflight[m.From] = false
	if m.Success {
		n.advance(m.From, m.MatchIndex)
	}
	if n.next[m.From] <= n.lastIndex() {
		n.sendAppend(m.From)
	}
}

// ---------------------------------------------------------------------------
// Plumbing

// ready runs at the end of every Tick/Step/Propose.
//
// WHY this order — persist, then apply, then send: Raft's safety argument
// assumes that a node's reply reflects state it will remember after a crash.
// If we sent "vote granted" before writing votedFor to disk, a crash and
// restart could make us vote twice in the same term.
func (n *Node) ready() {
	if n.configDirty {
		n.recomputeConfig()
	}
	n.persist()
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		first := n.log[0].Index
		e := n.log[n.lastApplied-first]
		if e.Type == EntryConfig {
			// Configuration entries are Raft's own business, not the KV store's.
			if n.role == Leader {
				n.emit(Event{Kind: "config", Index: e.Index, Detail: DescribeConfig(e.Data)})
			}
			continue
		}
		n.cfg.StateMachine.Apply(e)
	}
	// A leader that removed itself steps down once that change is committed:
	// from then on the new configuration can make progress without it.
	if n.role == Leader && !n.isVoter(n.id) && n.configIndex <= n.commitIndex {
		n.stepDownReason = "removed from the cluster"
		n.becomeFollower(n.term, 0)
	}
	n.maybeSnapshot()
	out := n.outbox
	n.outbox = nil
	for _, m := range out {
		n.cfg.Send(m)
	}
}

func (n *Node) persist() {
	st := n.cfg.Storage
	if st == nil {
		return
	}
	hs := HardState{Term: n.term, VotedFor: n.votedFor}
	if hs != n.savedHS {
		must(st.SaveHardState(hs))
		n.savedHS = hs
	}
	if n.needRewrite {
		must(st.RewriteLog(n.log[1:]))
		n.needRewrite = false
		n.stable = n.lastIndex()
	} else if n.lastIndex() > n.stable {
		first := n.log[0].Index
		from := max(n.stable, first) + 1
		must(st.Append(n.log[from-first:]))
		n.stable = n.lastIndex()
	}
}

func (n *Node) truncateFrom(index uint64) {
	first := n.log[0].Index
	n.log = n.log[:index-first]
	n.configDirty = true
	if n.stable >= index {
		n.needRewrite = true
		n.stable = index - 1
	}
	n.emit(Event{Kind: "truncate", Index: index})
}

func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term // pre-vote messages set their own term
	}
	n.outbox = append(n.outbox, m)
}

func (n *Node) emit(e Event) {
	if n.cfg.OnEvent == nil {
		return
	}
	e.Node = n.id
	e.Term = n.term
	n.cfg.OnEvent(e)
}

func (n *Node) quorum() int { return len(n.voters)/2 + 1 }

func (n *Node) lastIndex() uint64 { return n.log[len(n.log)-1].Index }
func (n *Node) lastTerm() uint64  { return n.log[len(n.log)-1].Term }

// termAt returns the term of the entry at index, if we still have it.
func (n *Node) termAt(index uint64) (uint64, bool) {
	first := n.log[0].Index
	if index < first || index > n.lastIndex() {
		return 0, false
	}
	return n.log[index-first].Term, true
}

func (n *Node) lastIndexOfTerm(term uint64) (uint64, bool) {
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Term == term {
			return n.log[i].Index, true
		}
		if n.log[i].Term < term {
			break
		}
	}
	return 0, false
}

// WHY panic on storage errors: a node that cannot persist its state must not
// keep participating — it could forget a vote or an acknowledged entry.
// Crashing turns a disk fault into an ordinary crash failure, which Raft
// already tolerates.
func must(err error) {
	if err != nil {
		panic("raft: storage failure: " + err.Error())
	}
}

func copyMap[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
