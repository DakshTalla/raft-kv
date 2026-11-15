package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStorage(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	must(s.SaveHardState(HardState{Term: 3, VotedFor: 2}))
	must(s.Append([]Entry{{1, 1, []byte("a")}, {2, 1, []byte("b")}, {3, 2, []byte("c")}}))
	must(s.Append([]Entry{{4, 3, nil}}))
	s.Close()

	s, _ = OpenStorage(dir, false)
	hs, snap, entries, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs != (HardState{3, 2}) || snap != nil || len(entries) != 4 || string(entries[2].Data) != "c" || entries[3].Data != nil {
		t.Fatalf("bad reload: hs=%+v snap=%v entries=%+v", hs, snap, entries)
	}

	// Truncation (follower dropping conflicting entries) rewrites the file.
	must(s.RewriteLog(entries[:2]))
	must(s.Append([]Entry{{3, 4, []byte("c2")}}))
	// Snapshot at index 2 compacts everything up to it.
	must(s.SaveSnapshot(Snapshot{LastIndex: 2, LastTerm: 1, Data: []byte("{}")}, []Entry{{3, 4, []byte("c2")}}))
	s.Close()

	s, _ = OpenStorage(dir, false)
	defer s.Close()
	_, snap, entries, _ = s.Load()
	if snap == nil || snap.LastIndex != 2 || len(entries) != 1 || entries[0].Term != 4 {
		t.Fatalf("bad reload after snapshot: snap=%+v entries=%+v", snap, entries)
	}
}

func TestStorageIgnoresTornTailWrite(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenStorage(dir, false)
	must(s.Append([]Entry{{1, 1, []byte("a")}}))
	s.Close()
	// Simulate a crash half-way through writing the next entry.
	f, _ := os.OpenFile(filepath.Join(dir, "log.jsonl"), os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"index":2,"term":1,"da`)
	f.Close()

	s, _ = OpenStorage(dir, false)
	defer s.Close()
	_, _, entries, err := s.Load()
	if err != nil || len(entries) != 1 {
		t.Fatalf("want 1 intact entry, got %d (err=%v)", len(entries), err)
	}
}
