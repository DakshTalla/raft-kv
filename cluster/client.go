package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DakshTalla/raft-kv/kv"
)

// Request is one client operation.
type Request struct {
	Op    string
	Key   string
	Value string
}

// Response is the result of a committed operation.
type Response struct {
	kv.Result
	Node     int `json:"node"`     // the leader that served the request
	Attempts int `json:"attempts"` // 1 = first try succeeded
}

// NotLeaderError tells the client which node to talk to instead.
type NotLeaderError struct{ Leader int }

func (e *NotLeaderError) Error() string {
	if e.Leader == 0 {
		return "not the leader (leader unknown — election in progress?)"
	}
	return fmt.Sprintf("not the leader; try node %d", e.Leader)
}

// ErrNodeDown is returned when a specific node was addressed but is dead.
var ErrNodeDown = errors.New("node is down")

// Do executes one linearizable operation.
//
// via == 0: behave like a smart client — find the leader, retry on timeouts
// and leader changes until ctx expires.
// via == N: send to node N only; if N is not the leader the caller gets a
// NotLeaderError carrying a redirect hint (the HTTP layer turns it into a 307).
//
// WHY reads also go through the log: a leader that has been partitioned away
// does not know it was replaced. If it answered reads from local state it
// could return stale data. Putting the GET in the log means it only returns
// once a majority has acknowledged this leader, which a deposed leader can
// never achieve. (Production systems avoid the log write with ReadIndex or
// leader leases — see docs/STUDY_GUIDE.md.)
func (c *Cluster) Do(ctx context.Context, via int, req Request) (Response, error) {
	var sess *clientSession
	select {
	case sess = <-c.sessions:
	case <-ctx.Done():
		return Response{}, ctx.Err()
	}
	defer func() { c.sessions <- sess }()
	sess.seq++
	// The same (client, seq) is reused for every retry of this operation, so
	// even if two attempts both end up in the log it is applied only once.
	cmd := kv.Command{Op: req.Op, Key: req.Key, Value: req.Value, Client: sess.id, Seq: sess.seq}
	data := cmd.Encode()

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return Response{}, fmt.Errorf("no result after %d attempt(s): %w", attempt-1, err)
		}
		target := via
		if target == 0 {
			target = c.Leader()
		}
		node, store, alive := c.get(target)
		if node == nil || !alive {
			if via != 0 {
				return Response{}, ErrNodeDown
			}
			c.sleep(ctx, 20*time.Millisecond)
			continue
		}
		ch := store.Wait(cmd.Client, cmd.Seq)
		if _, _, ok := node.Propose(data); !ok {
			store.Cancel(cmd.Client, cmd.Seq)
			if via != 0 {
				return Response{}, &NotLeaderError{Leader: node.Leader()}
			}
			c.sleep(ctx, 20*time.Millisecond)
			continue
		}
		timeout := time.NewTimer(max(c.Clock.Real(500*time.Millisecond), 50*time.Millisecond))
		select {
		case res := <-ch:
			timeout.Stop()
			return Response{Result: res, Node: target, Attempts: attempt}, nil
		case <-timeout.C:
			// Not committed in time: the leader may have been deposed or
			// partitioned. Retry (possibly on a new leader) with the same seq.
			store.Cancel(cmd.Client, cmd.Seq)
		case <-ctx.Done():
			timeout.Stop()
			store.Cancel(cmd.Client, cmd.Seq)
		}
	}
}

// sleep waits for d of virtual time (at least 1ms real) or until ctx is done.
func (c *Cluster) sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(max(c.Clock.Real(d), time.Millisecond))
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// Convenience wrappers used by tests and the benchmark.

func (c *Cluster) Put(ctx context.Context, key, value string) (Response, error) {
	return c.Do(ctx, 0, Request{Op: kv.OpPut, Key: key, Value: value})
}

func (c *Cluster) Get(ctx context.Context, key string) (Response, error) {
	return c.Do(ctx, 0, Request{Op: kv.OpGet, Key: key})
}

func (c *Cluster) Delete(ctx context.Context, key string) (Response, error) {
	return c.Do(ctx, 0, Request{Op: kv.OpDelete, Key: key})
}
