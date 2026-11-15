// Package kv is the replicated state machine: a string key-value store.
// Every node owns one Store; Raft guarantees they all apply the same commands
// in the same order, so they all hold the same data.
package kv

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/DakshTalla/raft-kv/raft"
)

// Operations understood by the store.
const (
	OpGet    = "get"
	OpPut    = "put"
	OpDelete = "delete"
)

// Command is what gets serialized into a Raft log entry.
//
// WHY Client+Seq: Raft guarantees a command is *committed* at most once per
// log slot, but a client that times out and retries would create a second log
// entry. Tagging every request with (client session, sequence number) lets the
// state machine recognise the retry and return the first result instead of
// executing twice. That is what turns at-least-once delivery into
// exactly-once semantics (§8 of the Raft paper).
type Command struct {
	Op     string `json:"op"`
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Client uint64 `json:"client"`
	Seq    uint64 `json:"seq"`
}

// Encode serializes a command for the log.
func (c Command) Encode() []byte {
	b, _ := json.Marshal(c)
	return b
}

// Describe renders a log entry's payload for humans ("PUT x=1").
func Describe(data []byte) string {
	if data == nil {
		return "no-op"
	}
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return "?"
	}
	switch c.Op {
	case OpPut:
		return fmt.Sprintf("PUT %s=%s", c.Key, c.Value)
	case OpDelete:
		return "DEL " + c.Key
	case OpGet:
		return "GET " + c.Key
	}
	return c.Op
}

// Result is the outcome of applying one command.
type Result struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
	Index uint64 `json:"index"` // log index the command was applied at
	Term  uint64 `json:"term"`
}

type session struct {
	Seq    uint64 `json:"seq"`
	Result Result `json:"result"`
}

type waitKey struct{ client, seq uint64 }

// Store implements raft.StateMachine.
type Store struct {
	mu       sync.Mutex
	data     map[string]string
	sessions map[uint64]session
	waiters  map[waitKey]chan Result
	applied  uint64
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]session{}, waiters: map[waitKey]chan Result{}}
}

// Apply executes one committed log entry. Called by Raft in log order.
func (s *Store) Apply(e raft.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = e.Index
	if e.Data == nil {
		return // leader's no-op
	}
	var c Command
	if err := json.Unmarshal(e.Data, &c); err != nil {
		return
	}
	var res Result
	if last, ok := s.sessions[c.Client]; ok && c.Seq <= last.Seq {
		res = last.Result // duplicate of an already-applied request
	} else {
		res = Result{Index: e.Index, Term: e.Term}
		switch c.Op {
		case OpPut:
			s.data[c.Key] = c.Value
			res.Value, res.Found = c.Value, true
		case OpDelete:
			res.Value, res.Found = s.data[c.Key], false
			if _, ok := s.data[c.Key]; ok {
				res.Found = true
			}
			delete(s.data, c.Key)
		case OpGet:
			res.Value, res.Found = s.data[c.Key]
		}
		s.sessions[c.Client] = session{Seq: c.Seq, Result: res}
	}
	k := waitKey{c.Client, c.Seq}
	if ch, ok := s.waiters[k]; ok {
		ch <- res // buffered (cap 1), never blocks
		delete(s.waiters, k)
	}
}

// Wait registers interest in the result of (client, seq). It must be called
// before proposing the command so the result cannot be missed.
func (s *Store) Wait(client, seq uint64) <-chan Result {
	ch := make(chan Result, 1)
	s.mu.Lock()
	s.waiters[waitKey{client, seq}] = ch
	s.mu.Unlock()
	return ch
}

// Cancel removes a waiter registered with Wait.
func (s *Store) Cancel(client, seq uint64) {
	s.mu.Lock()
	delete(s.waiters, waitKey{client, seq})
	s.mu.Unlock()
}

type snapshotData struct {
	Data     map[string]string  `json:"data"`
	Sessions map[uint64]session `json:"sessions"`
	Applied  uint64             `json:"applied"`
}

// Snapshot serializes the full state.
// WHY the session table is part of the snapshot: duplicate detection must
// survive log compaction, otherwise a retry arriving after a snapshot would be
// executed a second time.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(snapshotData{Data: s.data, Sessions: s.sessions, Applied: s.applied})
}

// Restore replaces the state with a snapshot.
func (s *Store) Restore(b []byte) error {
	var sd snapshotData
	if err := json.Unmarshal(b, &sd); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.sessions, s.applied = sd.Data, sd.Sessions, sd.Applied
	if s.data == nil {
		s.data = map[string]string{}
	}
	if s.sessions == nil {
		s.sessions = map[uint64]session{}
	}
	return nil
}

// Pair is one key/value for display.
type Pair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Dump returns up to limit pairs sorted by key, and the total key count.
// It reads local state directly — used only for display and for tests that
// compare replicas, never for client reads.
func (s *Store) Dump(limit int) ([]Pair, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	total := len(keys)
	if limit >= 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]Pair, len(keys))
	for i, k := range keys {
		out[i] = Pair{k, s.data[k]}
	}
	return out, total
}

// Applied returns the index of the last applied entry.
func (s *Store) Applied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}
