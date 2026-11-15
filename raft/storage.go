package raft

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// HardState is the part of Raft state that must survive a crash (Figure 2,
// "Persistent state on all servers"): currentTerm and votedFor. The log is
// persisted separately in an append-only file.
//
// WHY persist votedFor: without it a node could crash after voting for A,
// restart, and vote for B in the same term — two leaders in one term.
// WHY persist currentTerm: so a restarted node can't accept messages from a
// stale leader it already knew was deposed.
type HardState struct {
	Term     uint64 `json:"term"`
	VotedFor int    `json:"votedFor"` // 0 = voted for nobody this term
}

// Storage persists one node's Raft state in a directory:
//
//	state.json     currentTerm + votedFor (rewritten atomically)
//	log.jsonl      the log, one JSON entry per line (append-only WAL)
//	snapshot.json  latest snapshot (rewritten atomically)
//
// WHY an append-only log file: the common case (leader appends, follower
// appends) is a cheap sequential write of only the new entries. Only the rare
// cases — a follower truncating conflicting entries, or compaction after a
// snapshot — rewrite the whole file.
type Storage struct {
	dir     string
	fsync   bool
	logFile *os.File
	logW    *bufio.Writer
}

// OpenStorage opens (creating if needed) the storage directory. With fsync
// false, writes reach the OS page cache but are not forced to the disk: a
// process crash loses nothing, a power failure might. See README "Limits".
func OpenStorage(dir string, fsync bool) (*Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Storage{dir: dir, fsync: fsync}
	if err := s.openLogForAppend(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Storage) path(name string) string { return filepath.Join(s.dir, name) }

func (s *Storage) openLogForAppend() error {
	f, err := os.OpenFile(s.path("log.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.logFile = f
	s.logW = bufio.NewWriterSize(f, 64*1024)
	return nil
}

// Load reads everything back after a restart. Entries already covered by the
// snapshot are dropped. A torn final line (crash in the middle of a write) is
// ignored, which is safe: that entry was never acknowledged to anyone.
func (s *Storage) Load() (HardState, *Snapshot, []Entry, error) {
	var hs HardState
	if err := readJSON(s.path("state.json"), &hs); err != nil && !errors.Is(err, os.ErrNotExist) {
		return hs, nil, nil, fmt.Errorf("load hard state: %w", err)
	}
	var snap *Snapshot
	var sn Snapshot
	if err := readJSON(s.path("snapshot.json"), &sn); err == nil {
		snap = &sn
	} else if !errors.Is(err, os.ErrNotExist) {
		return hs, nil, nil, fmt.Errorf("load snapshot: %w", err)
	}
	var floor uint64
	if snap != nil {
		floor = snap.LastIndex
	}

	f, err := os.Open(s.path("log.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return hs, snap, nil, nil
	}
	if err != nil {
		return hs, snap, nil, err
	}
	defer f.Close()
	var entries []Entry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	for sc.Scan() {
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			break // torn tail write
		}
		if e.Index <= floor {
			continue
		}
		// Defensive: if the file ever contains an index that goes backwards,
		// the later line wins (it was written after a truncation).
		for len(entries) > 0 && entries[len(entries)-1].Index >= e.Index {
			entries = entries[:len(entries)-1]
		}
		entries = append(entries, e)
	}
	return hs, snap, entries, nil
}

// SaveHardState atomically replaces state.json.
func (s *Storage) SaveHardState(hs HardState) error {
	return s.writeAtomic("state.json", hs)
}

// Append adds entries to the end of the WAL.
func (s *Storage) Append(entries []Entry) error {
	for i := range entries {
		b, err := json.Marshal(&entries[i])
		if err != nil {
			return err
		}
		s.logW.Write(b)
		s.logW.WriteByte('\n')
	}
	return s.flush()
}

// RewriteLog replaces the whole WAL with entries (after a truncation or a
// compaction). Written to a temp file and renamed so a crash never leaves a
// half-written log behind.
func (s *Storage) RewriteLog(entries []Entry) error {
	if err := s.closeLog(); err != nil {
		return err
	}
	tmp := s.path("log.jsonl.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 64*1024)
	enc := json.NewEncoder(w) // Encode adds the trailing newline
	for i := range entries {
		if err := enc.Encode(&entries[i]); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if s.fsync {
		f.Sync()
	}
	f.Close()
	if err := renameRetry(tmp, s.path("log.jsonl")); err != nil {
		return err
	}
	return s.openLogForAppend()
}

// SaveSnapshot writes the snapshot first, then compacts the WAL. If we crash
// between the two steps, Load() simply skips WAL entries the snapshot covers.
func (s *Storage) SaveSnapshot(snap Snapshot, remaining []Entry) error {
	if err := s.writeAtomic("snapshot.json", snap); err != nil {
		return err
	}
	return s.RewriteLog(remaining)
}

func (s *Storage) flush() error {
	if err := s.logW.Flush(); err != nil {
		return err
	}
	if s.fsync {
		return s.logFile.Sync()
	}
	return nil
}

func (s *Storage) closeLog() error {
	if s.logFile == nil {
		return nil
	}
	s.logW.Flush()
	err := s.logFile.Close()
	s.logFile = nil
	return err
}

// Close flushes and closes the WAL.
func (s *Storage) Close() error { return s.closeLog() }

func (s *Storage) writeAtomic(name string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := s.path(name + ".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if s.fsync {
		f.Sync()
	}
	f.Close()
	// WHY rename: on both POSIX and Windows (MoveFileEx) the rename replaces
	// the old file in one step, so readers see either the old or new version.
	return renameRetry(tmp, s.path(name))
}

// renameRetry is os.Rename with a few retries.
//
// WHY: on Windows a rename fails with "Access is denied" while any other
// process (antivirus, the search indexer) has the destination file open, which
// happens for a few milliseconds now and then. Since a storage error makes
// the node crash on purpose, a transient lock must not count as one. The Go
// toolchain does the same internally (cmd/internal/robustio).
func renameRetry(from, to string) error {
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		if err = os.Rename(from, to); err == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	return err
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
