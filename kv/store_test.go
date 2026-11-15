package kv

import (
	"testing"

	"github.com/DakshTalla/raft-kv/raft"
)

func entry(i uint64, c Command) raft.Entry { return raft.Entry{Index: i, Term: 1, Data: c.Encode()} }

func TestDuplicateRequestIsAppliedOnce(t *testing.T) {
	s := NewStore()
	s.Apply(entry(1, Command{Op: OpPut, Key: "a", Value: "1", Client: 7, Seq: 1}))
	s.Apply(entry(2, Command{Op: OpPut, Key: "a", Value: "2", Client: 7, Seq: 2}))
	// A retry of seq 1 that was committed a second time must be ignored,
	// otherwise it would overwrite the newer value.
	s.Apply(entry(3, Command{Op: OpPut, Key: "a", Value: "1", Client: 7, Seq: 1}))
	if got, _ := s.Dump(-1); len(got) != 1 || got[0].Value != "2" {
		t.Fatalf("duplicate was re-applied: %v", got)
	}
}

func TestSnapshotRestoreKeepsDataAndSessions(t *testing.T) {
	s := NewStore()
	s.Apply(entry(1, Command{Op: OpPut, Key: "a", Value: "1", Client: 7, Seq: 1}))
	s.Apply(entry(2, Command{Op: OpPut, Key: "b", Value: "2", Client: 8, Seq: 1}))
	s.Apply(entry(3, Command{Op: OpDelete, Key: "b", Client: 8, Seq: 2}))
	snap, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	r := NewStore()
	if err := r.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if got, n := r.Dump(-1); n != 1 || got[0] != (Pair{"a", "1"}) || r.Applied() != 3 {
		t.Fatalf("bad restore: %v applied=%d", got, r.Applied())
	}
	// Dedup table survived: replaying client 8 seq 1 must not resurrect "b".
	r.Apply(entry(4, Command{Op: OpPut, Key: "b", Value: "2", Client: 8, Seq: 1}))
	if _, n := r.Dump(-1); n != 1 {
		t.Fatal("duplicate applied after snapshot restore")
	}
}

func TestWaiterGetsResult(t *testing.T) {
	s := NewStore()
	ch := s.Wait(1, 1)
	s.Apply(entry(5, Command{Op: OpPut, Key: "k", Value: "v", Client: 1, Seq: 1}))
	if res := <-ch; res.Index != 5 || res.Value != "v" {
		t.Fatalf("bad result %+v", res)
	}
}
