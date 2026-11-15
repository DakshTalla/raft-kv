// Package events is a tiny publish/subscribe hub. The cluster publishes
// everything that happens (role changes, RPCs, commits, chaos actions) and the
// WebSocket handler forwards it to browsers.
package events

import "sync"

// Msg describes one RPC message in flight, for the visualizer.
type Msg struct {
	ID        uint64  `json:"id"`
	Type      string  `json:"type"`
	From      int     `json:"from"`
	To        int     `json:"to"`
	Term      uint64  `json:"term"`
	SentAt    float64 `json:"sentAt"`    // virtual ms
	DeliverAt float64 `json:"deliverAt"` // virtual ms
	Entries   int     `json:"entries"`
	Heartbeat bool    `json:"heartbeat,omitempty"`
	Success   bool    `json:"success"`
	Reason    string  `json:"reason,omitempty"` // why it was dropped
}

// Event is one item of the stream.
type Event struct {
	Seq    uint64  `json:"seq"`
	T      float64 `json:"t"` // virtual time in ms
	Kind   string  `json:"kind"`
	Node   int     `json:"node,omitempty"`
	Term   uint64  `json:"term,omitempty"`
	Role   string  `json:"role,omitempty"`
	Index  uint64  `json:"index,omitempty"`
	Peer   int     `json:"peer,omitempty"`
	Detail string  `json:"detail,omitempty"`
	Msg    *Msg    `json:"msg,omitempty"`
	State  any     `json:"state,omitempty"`
}

// Hub fans events out to subscribers.
type Hub struct {
	mu      sync.Mutex
	seq     uint64
	subs    map[int]chan Event
	nextSub int
	history []Event // recent non-message events, replayed to new subscribers
}

const historySize = 200

// NewHub creates an empty hub.
func NewHub() *Hub { return &Hub{subs: map[int]chan Event{}} }

// Publish delivers e to all subscribers.
//
// WHY non-blocking sends: Publish is called from inside the Raft node (with
// its lock held). A slow browser must never be able to stall consensus, so if
// a subscriber's buffer is full the event is dropped for that subscriber.
func (h *Hub) Publish(e Event) {
	h.mu.Lock()
	h.seq++
	e.Seq = h.seq
	if e.Msg == nil && e.Kind != "state" {
		h.history = append(h.history, e)
		if len(h.history) > historySize {
			h.history = h.history[len(h.history)-historySize:]
		}
	}
	for _, ch := range h.subs {
		select {
		case ch <- e:
		default:
		}
	}
	h.mu.Unlock()
}

// Active reports whether anyone is listening (lets publishers skip work).
func (h *Hub) Active() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs) > 0
}

// Subscribe returns a channel of future events, the recent history, and a
// function to unsubscribe.
func (h *Hub) Subscribe(buffer int) (<-chan Event, []Event, func()) {
	ch := make(chan Event, buffer)
	h.mu.Lock()
	id := h.nextSub
	h.nextSub++
	h.subs[id] = ch
	hist := append([]Event(nil), h.history...)
	h.mu.Unlock()
	return ch, hist, func() {
		h.mu.Lock()
		delete(h.subs, id)
		h.mu.Unlock()
	}
}
