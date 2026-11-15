// Package sim provides the simulated environment the cluster runs in: a
// virtual clock that can be slowed down or paused, and a network that can
// delay, drop and partition messages.
//
// WHY simulate instead of real sockets: the interesting part of Raft is how it
// behaves under failures. With a simulated network we can inject exactly the
// failure we want (isolate the leader, drop 30% of packets, add 200ms latency)
// with one function call, reproduce it in tests, and slow time down far enough
// for a human to watch individual RPCs fly between nodes.
package sim

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// Clock is a virtual clock plus an event scheduler. Virtual time advances at
// 1/scale of real time: scale 1 is real time, scale 20 is 20x slow motion.
// All scheduled callbacks run one at a time on the goroutine that calls Run.
type Clock struct {
	mu       sync.Mutex
	scale    float64
	paused   bool
	baseReal time.Time
	baseVirt time.Duration
	queue    eventHeap
	seq      uint64
	wake     chan struct{}
}

type event struct {
	at  time.Duration
	seq uint64 // tie-breaker: FIFO among events scheduled for the same instant
	fn  func()
}

type eventHeap []event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(event)) }
func (h *eventHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

// NewClock returns a clock running at the given scale (>0).
func NewClock(scale float64) *Clock {
	if scale <= 0 {
		scale = 1
	}
	return &Clock{scale: scale, baseReal: time.Now(), wake: make(chan struct{}, 1)}
}

func (c *Clock) nowLocked() time.Duration {
	if c.paused {
		return c.baseVirt
	}
	return c.baseVirt + time.Duration(float64(time.Since(c.baseReal))/c.scale)
}

// Now returns the current virtual time.
func (c *Clock) Now() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nowLocked()
}

// rebase pins the current virtual time so that scale/pause changes do not
// make time jump.
func (c *Clock) rebaseLocked() {
	c.baseVirt = c.nowLocked()
	c.baseReal = time.Now()
}

// SetScale changes how slow virtual time runs relative to real time.
func (c *Clock) SetScale(scale float64) {
	if scale <= 0 {
		return
	}
	c.mu.Lock()
	c.rebaseLocked()
	c.scale = scale
	c.mu.Unlock()
	c.poke()
}

// Scale returns the current slow-down factor.
func (c *Clock) Scale() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scale
}

// Pause freezes virtual time: no ticks fire and no messages are delivered.
func (c *Clock) Pause() {
	c.mu.Lock()
	c.rebaseLocked()
	c.paused = true
	c.mu.Unlock()
	c.poke()
}

// Resume continues after Pause.
func (c *Clock) Resume() {
	c.mu.Lock()
	c.paused = false
	c.baseReal = time.Now()
	c.mu.Unlock()
	c.poke()
}

// Paused reports whether the clock is frozen.
func (c *Clock) Paused() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused
}

// Real converts a virtual duration into the real time it currently takes.
func (c *Clock) Real(d time.Duration) time.Duration {
	return time.Duration(float64(d) * c.Scale())
}

// After schedules fn to run once, d of virtual time from now.
func (c *Clock) After(d time.Duration, fn func()) {
	c.mu.Lock()
	c.atLocked(c.nowLocked()+d, fn)
	c.mu.Unlock()
	c.poke()
}

// At schedules fn to run at virtual time t.
func (c *Clock) At(t time.Duration, fn func()) {
	c.mu.Lock()
	c.atLocked(t, fn)
	c.mu.Unlock()
	c.poke()
}

func (c *Clock) atLocked(t time.Duration, fn func()) {
	c.seq++
	heap.Push(&c.queue, event{at: t, seq: c.seq, fn: fn})
}

// Every runs fn every d of virtual time until the returned cancel func is
// called. Used for the per-node Raft tick.
func (c *Clock) Every(d time.Duration, fn func()) (cancel func()) {
	var mu sync.Mutex
	stopped := false
	next := c.Now() + d
	var run func()
	run = func() {
		mu.Lock()
		s := stopped
		mu.Unlock()
		if s {
			return
		}
		fn()
		// WHY schedule from the previous deadline, not from "now": if the
		// process stalls, ticks catch up instead of silently stretching time.
		next += d
		c.At(next, run)
	}
	c.At(next, run)
	return func() {
		mu.Lock()
		stopped = true
		mu.Unlock()
	}
}

func (c *Clock) poke() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Run executes scheduled events until ctx is cancelled.
func (c *Clock) Run(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		c.mu.Lock()
		var wait time.Duration = time.Hour
		var fn func()
		if !c.paused && len(c.queue) > 0 {
			now := c.nowLocked()
			if head := c.queue[0]; head.at <= now {
				fn = heap.Pop(&c.queue).(event).fn
			} else {
				wait = time.Duration(float64(head.at-now) * c.scale)
			}
		}
		c.mu.Unlock()
		if fn != nil {
			fn()
			if ctx.Err() != nil {
				return
			}
			continue
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		case <-timer.C:
		}
	}
}
