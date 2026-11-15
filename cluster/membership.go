package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/DakshTalla/raft-kv/events"
	"github.com/DakshTalla/raft-kv/raft"
)

// MaxNodes caps how large the demo cluster may grow.
const MaxNodes = 9

// AddNode starts a brand-new node and asks the leader to add it to the
// configuration. It returns once the change is committed (the node is a
// voter), or with an error when ctx expires first.
//
// What happens underneath (raft/membership.go): the leader first replicates
// its log — or a snapshot — to the new node as a non-voting learner, and only
// when it has caught up appends the configuration entry that makes it a voter.
func (c *Cluster) AddNode(ctx context.Context) (int, error) {
	c.mu.Lock()
	if len(c.ids) >= MaxNodes {
		c.mu.Unlock()
		return 0, fmt.Errorf("the cluster already has %d nodes (max %d)", len(c.ids), MaxNodes)
	}
	// WHY never reuse an id: a removed node's id may still appear in old log
	// entries and votes. A fresh id cannot be confused with the old node.
	c.nextID++
	id := c.nextID
	c.ids = append(c.ids, id)
	c.members[id] = &member{id: id, joined: true}
	c.mu.Unlock()

	os.RemoveAll(c.nodeDir(id)) // a fresh node starts with an empty disk
	if err := c.startNode(id); err != nil {
		c.forget(id)
		return 0, err
	}
	c.publish(events.Event{Kind: "add", Node: id})

	err := c.waitConfig(ctx, func(leader *raft.Node, st raft.Status) bool {
		if slices.Contains(st.Voters, id) {
			return st.ConfigIndex <= st.CommitIndex // promoted and committed
		}
		if !slices.Contains(st.Learners, id) {
			// Not being caught up by this leader (first attempt, or the
			// leader changed and forgot): (re)start the change.
			leader.AddMember(id)
		}
		return false
	})
	if err != nil {
		return id, fmt.Errorf("node %d started but did not become a voter: %w", id, err)
	}
	return id, nil
}

// RemoveNode takes node id out of the configuration and then shuts it down.
// Removing the leader works too: it keeps leading until the change commits,
// then steps down and the remaining nodes elect a new leader.
func (c *Cluster) RemoveNode(ctx context.Context, id int) error {
	c.mu.Lock()
	m := c.members[id]
	n := len(c.ids)
	c.mu.Unlock()
	if m == nil {
		return fmt.Errorf("no such node %d", id)
	}
	if n <= 1 {
		return errors.New("cannot remove the last node")
	}
	err := c.waitConfig(ctx, func(leader *raft.Node, st raft.Status) bool {
		if !slices.Contains(st.Voters, id) {
			return st.ConfigIndex <= st.CommitIndex
		}
		leader.RemoveMember(id) // ErrChangePending just means "try again soon"
		return false
	})
	if err != nil {
		return fmt.Errorf("could not remove node %d: %w", id, err)
	}
	c.Kill(id)
	c.forget(id)
	os.RemoveAll(c.nodeDir(id))
	c.publish(events.Event{Kind: "remove", Node: id})
	return nil
}

// waitConfig repeatedly looks at the current leader's status and calls step
// until step reports that the desired configuration is committed.
func (c *Cluster) waitConfig(ctx context.Context, step func(*raft.Node, raft.Status) bool) error {
	for {
		if l := c.Leader(); l != 0 {
			if node, _, alive := c.get(l); alive {
				if step(node, node.Status(0)) {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(max(c.Clock.Real(20*time.Millisecond), 2*time.Millisecond)):
		}
	}
}

func (c *Cluster) forget(id int) {
	c.Net.Detach(id)
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.members, id)
	c.ids = slices.DeleteFunc(c.ids, func(x int) bool { return x == id })
}

func (c *Cluster) nodeDir(id int) string {
	return filepath.Join(c.opts.DataDir, fmt.Sprintf("node-%d", id))
}
