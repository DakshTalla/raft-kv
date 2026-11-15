// Package transport carries Raft messages between separate processes over
// real TCP connections. It is the production counterpart of sim.Network: both
// offer the same two things to a raft.Node — a non-blocking Send, and calls to
// Step for every message that arrives.
package transport

import (
	"encoding/gob"
	"net"
	"sync"
	"time"

	"github.com/DakshTalla/raft-kv/raft"
)

// TCP is one node's endpoint: a listener for incoming messages and one
// outgoing connection per peer.
type TCP struct {
	id      int
	addrs   map[int]string
	ln      net.Listener
	handler func(raft.Message)

	mu     sync.Mutex
	queues map[int]chan raft.Message
	conns  map[net.Conn]struct{}
	closed chan struct{}
	wg     sync.WaitGroup
}

// queueSize bounds how many messages may wait for one peer.
const queueSize = 256

// Listen opens node id's listening socket. addrs maps every node id
// (including id) to its host:port. Call Start to begin delivering messages.
func Listen(id int, addrs map[int]string) (*TCP, error) {
	ln, err := net.Listen("tcp", addrs[id])
	if err != nil {
		return nil, err
	}
	return &TCP{
		id: id, addrs: addrs, ln: ln,
		queues: map[int]chan raft.Message{}, conns: map[net.Conn]struct{}{},
		closed: make(chan struct{}),
	}, nil
}

// Start begins accepting connections; every received message is passed to
// handler (the node's Step method).
func (t *TCP) Start(handler func(raft.Message)) {
	t.handler = handler
	t.wg.Add(1)
	go t.acceptLoop()
}

// Send queues m for its destination and returns immediately.
//
// WHY drop instead of block when the queue is full: Raft calls Send while
// holding the node's lock. If a slow or dead peer could block it, one bad
// connection would freeze the whole node. Raft already tolerates lost
// messages — the next heartbeat retransmits — so dropping is always safe.
func (t *TCP) Send(m raft.Message) {
	t.mu.Lock()
	q, ok := t.queues[m.To]
	if !ok {
		q = make(chan raft.Message, queueSize)
		t.queues[m.To] = q
		t.wg.Add(1)
		go t.writeLoop(m.To, q)
	}
	t.mu.Unlock()
	select {
	case q <- m:
	default:
	}
}

// writeLoop owns the connection to one peer: it dials on demand, encodes
// messages onto the stream, and reconnects after any error.
func (t *TCP) writeLoop(to int, q chan raft.Message) {
	defer t.wg.Done()
	var conn net.Conn
	var enc *gob.Encoder
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	for {
		var m raft.Message
		select {
		case <-t.closed:
			return
		case m = <-q:
		}
		if conn == nil {
			c, err := net.DialTimeout("tcp", t.addrs[to], 200*time.Millisecond)
			if err != nil {
				continue // peer is down: drop this message, try again on the next
			}
			conn, enc = c, gob.NewEncoder(c)
		}
		conn.SetWriteDeadline(time.Now().Add(time.Second))
		if err := enc.Encode(&m); err != nil {
			conn.Close()
			conn, enc = nil, nil
		}
	}
}

func (t *TCP) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			return // listener closed
		}
		t.mu.Lock()
		t.conns[conn] = struct{}{}
		t.mu.Unlock()
		t.wg.Add(1)
		go t.readLoop(conn)
	}
}

// readLoop decodes messages from one incoming connection until it breaks.
// WHY one long-lived stream per peer: TCP delivers bytes in order, so
// messages from one sender arrive in the order they were sent — the same
// per-link FIFO guarantee the simulator provides.
func (t *TCP) readLoop(conn net.Conn) {
	defer t.wg.Done()
	defer func() {
		conn.Close()
		t.mu.Lock()
		delete(t.conns, conn)
		t.mu.Unlock()
	}()
	dec := gob.NewDecoder(conn)
	for {
		var m raft.Message
		if err := dec.Decode(&m); err != nil {
			return
		}
		if m.To != t.id {
			continue // misrouted; ignore
		}
		t.handler(m)
	}
}

// Addr returns the address the listener is bound to.
func (t *TCP) Addr() string { return t.ln.Addr().String() }

// Close stops the listener and all connections and waits for the goroutines.
func (t *TCP) Close() {
	close(t.closed)
	t.ln.Close()
	t.mu.Lock()
	for c := range t.conns {
		c.Close()
	}
	t.mu.Unlock()
	t.wg.Wait()
}
