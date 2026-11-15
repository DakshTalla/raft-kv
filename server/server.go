// Package server runs ONE Raft node as a real process: its own TCP listener
// for Raft messages, its own data directory, a real-time ticker. Start five
// of these (cmd/raftnode) and you have a cluster of independent processes
// that talk over the network — no simulator involved.
package server

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/DakshTalla/raft-kv/kv"
	"github.com/DakshTalla/raft-kv/raft"
	"github.com/DakshTalla/raft-kv/transport"
)

// Config configures one node process.
type Config struct {
	ID      int
	Addrs   map[int]string // raft address (host:port) of every node, including this one
	DataDir string
	Fsync   bool

	Tick              time.Duration // default 10ms
	ElectionTicks     int           // default 15 (150–300 ms)
	HeartbeatTicks    int           // default 5 (50 ms)
	SnapshotThreshold uint64
}

// NotLeaderError carries the id of the node the client should talk to.
type NotLeaderError struct{ Leader int }

func (e *NotLeaderError) Error() string {
	if e.Leader == 0 {
		return "not the leader (leader unknown)"
	}
	return fmt.Sprintf("not the leader; try node %d", e.Leader)
}

// Server is a running node.
type Server struct {
	cfg   Config
	node  *raft.Node
	store *kv.Store
	tr    *transport.TCP
	stop  chan struct{}
	done  chan struct{}

	// sessions is a pool of client sessions for requests that do not bring
	// their own id. A session is used by one request at a time, so its
	// sequence numbers are applied in order (see kv.Command).
	sessions chan *session
}

type session struct{ id, seq uint64 }

// Start opens the node's storage and network endpoint and begins ticking.
func Start(cfg Config) (*Server, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	ids := make([]int, 0, len(cfg.Addrs))
	for id := range cfg.Addrs {
		ids = append(ids, id)
	}
	st, err := raft.OpenStorage(cfg.DataDir, cfg.Fsync)
	if err != nil {
		return nil, err
	}
	tr, err := transport.Listen(cfg.ID, cfg.Addrs)
	if err != nil {
		st.Close()
		return nil, err
	}
	store := kv.NewStore()
	node, err := raft.NewNode(raft.Config{
		ID: cfg.ID, Peers: ids,
		ElectionTicks: cfg.ElectionTicks, HeartbeatTicks: cfg.HeartbeatTicks,
		SnapshotThreshold: cfg.SnapshotThreshold,
		Storage:           st, StateMachine: store, Send: tr.Send,
	})
	if err != nil {
		tr.Close()
		st.Close()
		return nil, err
	}
	s := &Server{
		cfg: cfg, node: node, store: store, tr: tr,
		stop: make(chan struct{}), done: make(chan struct{}),
		sessions: make(chan *session, 256),
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID)))
	for i := 0; i < cap(s.sessions); i++ {
		s.sessions <- &session{id: rng.Uint64()>>1 | 1}
	}
	tr.Start(node.Step)
	go s.tickLoop()
	return s, nil
}

// tickLoop is the node's clock: in real mode a tick is simply 10 ms of
// wall-clock time.
func (s *Server) tickLoop() {
	defer close(s.done)
	t := time.NewTicker(s.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.node.Tick()
		}
	}
}

// Close stops the node (like a crash: only what is on disk survives).
func (s *Server) Close() {
	close(s.stop)
	<-s.done
	s.tr.Close()
	s.node.Stop()
}

// Status returns the node's Raft state.
func (s *Server) Status() raft.Status { return s.node.Status(0) }

// Store exposes the local state machine (tests and diagnostics only).
func (s *Server) Store() *kv.Store { return s.store }

// Do runs one operation on THIS node. If it is not the leader the caller
// gets a NotLeaderError and must go elsewhere — exactly what a real client
// library does.
//
// client/seq identify the request for exactly-once semantics. A client that
// retries after a timeout must reuse the same pair. Pass client == 0 to let
// the server assign one (then a retry is a new request).
func (s *Server) Do(ctx context.Context, op, key, value string, client, seq uint64) (kv.Result, error) {
	if client == 0 {
		var sess *session
		select {
		case sess = <-s.sessions:
		case <-ctx.Done():
			return kv.Result{}, ctx.Err()
		}
		defer func() { s.sessions <- sess }()
		sess.seq++
		client, seq = sess.id, sess.seq
	}
	cmd := kv.Command{Op: op, Key: key, Value: value, Client: client, Seq: seq}
	ch := s.store.Wait(client, seq)
	if _, _, ok := s.node.Propose(cmd.Encode()); !ok {
		s.store.Cancel(client, seq)
		return kv.Result{}, &NotLeaderError{Leader: s.node.Leader()}
	}
	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		s.store.Cancel(client, seq)
		return kv.Result{}, errors.New("not committed in time (lost leadership or no majority)")
	}
}
