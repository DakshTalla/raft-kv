package kv

import (
	"crypto/rand"
	"encoding/binary"
)

// NewSessionID returns a random, non-zero client session id.
//
// WHY crypto/rand: the duplicate-detection table is replicated and survives
// restarts, so a session id must never repeat across process runs. Ids drawn
// from a time-seeded math/rand generator can repeat (two runs can end up with
// the same seed), and then a fresh request looks like a retry of an old one
// and gets the OLD cached answer — a stale read. 63 random bits from the
// operating system make a collision practically impossible.
func NewSessionID() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("kv: cannot read random bytes: " + err.Error())
	}
	return binary.LittleEndian.Uint64(b[:])>>1 | 1
}
