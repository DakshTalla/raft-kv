package checker

import "testing"

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	h := []Op{
		{Call: 1, Return: 2, Kind: "put", Key: "x", In: "1"},
		{Call: 3, Return: 4, Kind: "get", Key: "x", Out: "1", Found: true},
		{Call: 5, Return: 6, Kind: "delete", Key: "x"},
		{Call: 7, Return: 8, Kind: "get", Key: "x", Found: false},
	}
	if ok, key := Check(h); !ok {
		t.Fatalf("expected linearizable, failed on key %q", key)
	}
}

func TestConcurrentWritesEitherOrder(t *testing.T) {
	// Two overlapping puts; a later read may see either one.
	for _, seen := range []string{"1", "2"} {
		h := []Op{
			{Call: 1, Return: 10, Kind: "put", Key: "x", In: "1"},
			{Call: 2, Return: 9, Kind: "put", Key: "x", In: "2"},
			{Call: 11, Return: 12, Kind: "get", Key: "x", Out: seen, Found: true},
		}
		if ok, _ := Check(h); !ok {
			t.Fatalf("reading %q after concurrent puts should be linearizable", seen)
		}
	}
}

func TestStaleReadIsRejected(t *testing.T) {
	// put 1, then put 2 completes, then a read returns 1: a stale read.
	h := []Op{
		{Call: 1, Return: 2, Kind: "put", Key: "x", In: "1"},
		{Call: 3, Return: 4, Kind: "put", Key: "x", In: "2"},
		{Call: 5, Return: 6, Kind: "get", Key: "x", Out: "1", Found: true},
	}
	if ok, _ := Check(h); ok {
		t.Fatal("stale read must not be linearizable")
	}
}

func TestLostWriteIsRejected(t *testing.T) {
	h := []Op{
		{Call: 1, Return: 2, Kind: "put", Key: "x", In: "1"},
		{Call: 3, Return: 4, Kind: "get", Key: "x", Found: false},
	}
	if ok, _ := Check(h); ok {
		t.Fatal("acknowledged write that disappears must not be linearizable")
	}
}

func TestUnknownWriteMayOrMayNotApply(t *testing.T) {
	base := []Op{
		{Call: 1, Return: 2, Kind: "put", Key: "x", In: "1"},
		{Call: 3, Kind: "put", Key: "x", In: "2", Unknown: true}, // timed out
	}
	for _, seen := range []string{"1", "2"} {
		h := append(append([]Op{}, base...), Op{Call: 20, Return: 21, Kind: "get", Key: "x", Out: seen, Found: true})
		if ok, _ := Check(h); !ok {
			t.Fatalf("read of %q with an in-doubt write should be allowed", seen)
		}
	}
	// ...but it cannot un-apply: 2 then 1 again is impossible.
	h := append(append([]Op{}, base...),
		Op{Call: 20, Return: 21, Kind: "get", Key: "x", Out: "2", Found: true},
		Op{Call: 22, Return: 23, Kind: "get", Key: "x", Out: "1", Found: true})
	if ok, _ := Check(h); ok {
		t.Fatal("value flipping back must not be linearizable")
	}
}
