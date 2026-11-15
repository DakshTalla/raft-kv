// Package checker verifies that a recorded history of client operations is
// linearizable: that there is some order of the operations which (a) respects
// real time — if A finished before B started, A comes first — and (b) is a
// legal sequential execution of a key-value store.
//
// It is a small version of the Wing & Gong / Lowe algorithm used by tools
// like Porcupine and Knossos: a depth-first search over possible orders with
// memoization of (set of linearized ops, register state).
package checker

import (
	"math"
	"sort"
	"strings"
)

// Op is one client operation as observed from outside the system.
type Op struct {
	Call   int64 // time the client sent the request (ns)
	Return int64 // time the client got the response (ns)
	Kind   string // "put", "get" or "delete"
	Key    string
	In     string // value written (put)
	Out    string // value read (get)
	Found  bool   // get: key existed
	// Unknown marks a write whose client timed out. It may have taken effect
	// at any point after Call, or never — both must be allowed.
	Unknown bool
}

// Check reports whether the history is linearizable. On failure it returns
// the key whose sub-history has no valid ordering.
//
// WHY check each key separately: operations on different keys commute, so
// the history is linearizable iff each per-key history is (linearizability is
// "local"). That keeps the search small.
func Check(history []Op) (ok bool, badKey string) {
	byKey := map[string][]Op{}
	for _, op := range history {
		if op.Unknown {
			if op.Kind == "get" {
				continue // a read nobody saw the result of constrains nothing
			}
			op.Return = math.MaxInt64
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	for key, ops := range byKey {
		if !checkKey(ops) {
			return false, key
		}
	}
	return true, ""
}

func checkKey(ops []Op) bool {
	sort.Slice(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	n := len(ops)
	done := make([]bool, n)
	mustDo := 0 // completed ops still to place; Unknown ones are optional
	for _, op := range ops {
		if !op.Unknown {
			mustDo++
		}
	}
	seen := map[string]bool{}

	var dfs func(left int, val string, exists bool) bool
	dfs = func(left int, val string, exists bool) bool {
		if left == 0 {
			return true
		}
		var sb strings.Builder
		for _, d := range done {
			if d {
				sb.WriteByte('1')
			} else {
				sb.WriteByte('0')
			}
		}
		if exists {
			sb.WriteByte('+')
		}
		sb.WriteString(val)
		memo := sb.String()
		if seen[memo] {
			return false // already explored this exact situation
		}
		seen[memo] = true

		// An op may be linearized next only if no other pending op finished
		// before it started (otherwise that other op must come first).
		minReturn := int64(math.MaxInt64)
		for i, op := range ops {
			if !done[i] && op.Return < minReturn {
				minReturn = op.Return
			}
		}
		for i, op := range ops {
			if done[i] {
				continue
			}
			if op.Call > minReturn {
				break // sorted by Call: nothing later can go first either
			}
			nv, ne := val, exists
			switch op.Kind {
			case "get":
				if op.Found != exists || (exists && op.Out != val) {
					continue // this read could not have happened here
				}
			case "put":
				nv, ne = op.In, true
			case "delete":
				nv, ne = "", false
			}
			done[i] = true
			nl := left
			if !op.Unknown {
				nl--
			}
			if dfs(nl, nv, ne) {
				return true
			}
			done[i] = false
		}
		return false
	}
	return dfs(mustDo, "", false)
}
