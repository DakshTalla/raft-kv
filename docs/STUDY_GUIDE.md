# Study guide — how to explain this project in an interview

Read this top to bottom once, then open the UI (`./start.ps1`) and reproduce every scenario in section 3. If you can narrate what is happening on screen, you can defend the project.

Contents: [1. Pitch](#1-the-60-second-pitch) · [2. Raft concepts](#2-raft-in-plain-words) · [3. Scenarios](#3-scenarios-to-practise-in-the-ui) · [4. File walkthrough](#4-file-by-file-walkthrough) · [5. Interview Q&A](#5-interview-questions-and-answers) · [6. Numbers](#6-numbers-to-remember) · [7. Resume bullets](#7-resume-bullets)

---

## 1. The 60-second pitch

> "I built a distributed key-value store that stays consistent when machines crash or the network splits. Five nodes keep identical copies of the data using the Raft consensus algorithm, which I implemented from scratch in Go — leader election, log replication, persistence and snapshots — without a library. On top of it is a linearizable GET/PUT/DELETE API.
>
> To prove it works I built two things. First, a chaos test that kills nodes, partitions the network and drops packets while clients run, and then checks the history with a linearizability checker I wrote. Second, a web visualizer where you can watch every RPC, kill the leader, or draw a line through the cluster to partition it and see the system recover.
>
> It does about 16,500 linearizable writes per second with 128 clients and recovers from a leader crash in about 170 milliseconds. The same Raft code also runs as five separate processes talking over TCP."

Know which mode you are showing. **The visualizer runs the five nodes in one process over a simulated network** — deliberately, so faults can be injected with one click and time can be slowed down 60×. **`./start-cluster.ps1` runs five separate OS processes that talk Raft over real TCP** (`transport/`, `server/`, `cmd/raftnode`). The Raft package is identical in both: it only calls a `Send(message)` function and receives `Step` calls. What is still not covered is running on separate *machines* (no TLS, no service discovery).

---

## 2. Raft in plain words

### The problem

We want several machines to hold the same data so that the service survives crashes. The hard part is agreeing on the **order** of writes when messages are delayed or lost and machines die at any moment. If every machine applies the same commands in the same order, they all end up in the same state. That is called **state machine replication**, and the ordered list of commands is the **replicated log**. Raft is an algorithm for keeping that log identical on a majority of machines.

### Roles and terms

- Every node is a **follower**, a **candidate** or a **leader**. There is at most one leader at a time, and only the leader accepts writes.
- Time is divided into **terms** (1, 2, 3, …). A term starts with an election. A term has at most one leader. The term number acts as a logical clock: every message carries the sender's term, and a node that sees a higher term immediately updates its own and becomes a follower. Messages with a lower term are rejected.

### Leader election

1. The leader sends a **heartbeat** (an empty `AppendEntries`) every 50 ms.
2. Each follower runs an **election timer** set to a random value between 150 and 300 ms. A heartbeat resets it.
3. If the timer fires, the follower becomes a **candidate**: it increments its term, votes for itself and sends `RequestVote` to everyone.
4. A node grants its vote if (a) it has not voted for someone else in this term, and (b) the candidate's log is **at least as up-to-date** as its own (compare the last entry's term, then the log length).
5. A candidate with votes from a **majority** (3 of 5) becomes leader and starts sending heartbeats.

Why random timeouts? If all followers timed out together they would split the vote forever. Randomness makes one of them go first almost always.

Why a majority? Any two majorities of 5 share at least one node. That overlap is the reason two leaders cannot be elected in one term, and the reason a new leader always knows about every committed entry.

### Log replication

1. The client sends a command to the leader. The leader appends it to its log as an **entry** `(index, term, command)`.
2. The leader sends `AppendEntries` with the new entries, plus the index and term of the entry just before them (`prevLogIndex`, `prevLogTerm`).
3. A follower accepts only if it has that previous entry. Otherwise it rejects, and the leader steps back and retries with older entries until the logs meet. The follower then deletes anything that conflicts and appends the leader's entries. **The leader never changes its own log; followers are made to match it.**
4. Once an entry is stored on a majority, it is **committed**. The leader applies it to the KV store and answers the client. Followers learn the commit index from the next `AppendEntries` and apply too.

### The five safety properties

| Property | Meaning | Where it is enforced in this code |
|---|---|---|
| **Election Safety** | At most one leader per term | One vote per term, persisted (`handleVote`); majority needed (`handleVoteResp`) |
| **Leader Append-Only** | A leader never overwrites or deletes its log | Only followers call `truncateFrom` |
| **Log Matching** | If two logs have an entry with the same index and term, the logs are identical up to that index | The `prevLogIndex/prevLogTerm` check in `handleAppend` |
| **Leader Completeness** | A committed entry is in the log of every future leader | The "up-to-date" vote check in `handleVote` |
| **State Machine Safety** | No two nodes apply different commands at the same index | Follows from the above; plus "only commit current-term entries by counting" in `maybeCommit` |

### Three details that interviewers love

**a) Why must term and vote be on disk?** If a node votes for A, crashes, restarts and forgets, it could vote for B in the same term → two leaders. So `votedFor` and `currentTerm` are written to disk before the vote reply is sent. The log must be on disk because acknowledging an entry is a promise that it will still be there.

**b) Why does a new leader append a no-op? (Figure 8)** A leader may find entries from an old term that are on a majority but were never marked committed. It is *not* allowed to commit them just by counting copies — the paper's Figure 8 shows a sequence where such an entry gets overwritten later. The rule: a leader only commits entries **from its own term** by counting; older entries become committed as a side effect. So a fresh leader immediately appends an empty entry in its own term; once that commits, everything before it is committed too.

**c) Why can't a partitioned old leader cause damage?** It can still *accept* writes into its log, but it cannot reach a majority, so they never commit and the client never gets a success. When the partition heals, it sees a higher term, steps down, and the new leader's `AppendEntries` makes it delete those entries. In this project even reads go through the log, so the old leader cannot serve stale reads either.

### Snapshots

The log cannot grow forever. After 40 applied entries (configurable) a node serializes its KV store into a **snapshot**, records the index and term of the last entry it covers, and deletes the log up to there. If a follower is so far behind that the leader no longer has the entries it needs, the leader sends the snapshot (`InstallSnapshot`) instead.

---

## 3. Scenarios to practise in the UI

Run `./start.ps1`. The default speed is 20× slow motion. The ring around each node is its election timer draining; a heartbeat refills it.

| # | Do this | What you should see and say |
|---|---|---|
| 1 | Just watch | Small green dots leave the leader every heartbeat, blue acks come back. Follower timers refill each time. |
| 2 | **Kill** the leader | Followers' timers drain. The first to hit zero turns yellow (candidate), term +1, yellow `RequestVote` dots fly out, votes come back, it turns green. Timeline: "started an election", "voted for", "became leader", "appended index N (no-op)". |
| 3 | **Send** a `PUT` | A larger green dot with "1" goes to each follower. The entry appears dashed (uncommitted) in the leader's log row, then solid once 3 nodes have it. |
| 4 | Set *Send to* a follower, **Send** | Response shows "N2 ↪ 307 redirect → served by N4". |
| 5 | **Split 2 \| 3** (leader lands in the minority) | The majority side elects a new leader with a higher term. Now two nodes are green — but in different terms. Send a write to the old leader: it appears dashed and the request times out (504). Send one to the new leader: it commits. |
| 6 | **Heal network** | The old leader sees the higher term, steps down; timeline shows "discarded entries from N — conflicted with the leader's log". All five log rows become identical. |
| 7 | Kill 3 nodes | No leader can be elected; writes fail. Restart one → progress resumes. "5 nodes tolerate 2 failures." |
| 8 | Kill a follower, press **Burst** 3×, restart it | Log cells show `snap`. The restarted node gets a pink square (`InstallSnapshot`); timeline: "installed a snapshot". |
| 9 | Restart any node | Click it: its term and log came back from disk; `commitIndex` starts low and catches up from the leader. |
| 10 | Packet loss 30% | Red crosses on dropped messages; the next heartbeat retransmits. Still correct, just slower. |
| 11 | Latency 200 ms | Round trips approach the election timeout → spurious elections and rising terms. Good moment to explain why `election timeout ≫ network round trip`. |

---

## 4. File-by-file walkthrough

### `raft/` — the algorithm (the part you must know cold)

**`raft/types.go`** — Data types only.
- `Role`: follower / candidate / leader.
- `Entry{Index, Term, Data}`: one log slot. `Data == nil` is the leader's no-op.
- `Message`: one struct for all six message kinds (`RequestVote`, `AppendEntries`, `InstallSnapshot` and their responses). Requests and responses are separate messages; nothing blocks.
- `StateMachine` interface: `Apply`, `Snapshot`, `Restore`. Raft does not know it is replicating a KV store.
- `Event`: what the node reports to observers (role change, commit, …).

**`raft/raft.go`** — The node. Read it in this order:
1. `Node` struct: persistent state (`term`, `votedFor`, `log`), volatile state (`role`, `commitIndex`, `lastApplied`), leader state (`next`, `match`, `inflight`), timers. `log[0]` is a **sentinel** holding the index/term of the last snapshotted entry so "previous entry" lookups never need a special case.
2. Three public entry points, each taking the lock and ending with `ready()`:
   - `Tick()` — leader: maybe send heartbeats; others: advance the election timer, maybe `campaign()`.
   - `Step(m)` — first the universal term rule (higher term → become follower; lower term → reject), then dispatch on the message type.
   - `Propose(data)` — leader appends a client command and replicates.
3. Elections: `campaign` → `handleVote` (the up-to-date check) → `handleVoteResp` (count) → `becomeLeader` (init `next`/`match`, append no-op).
4. Replication: `sendAppend` (build a batch from `next[peer]`, or send a snapshot if those entries are compacted) → `handleAppend` on the follower (consistency check, conflict hints, truncate, append, advance commit) → `handleAppendResp` on the leader (advance `match`/`next` or back up) → `maybeCommit` (highest index of the current term stored on a majority).
5. Snapshots: `maybeSnapshot`, `handleSnapshot`, `handleSnapshotResp`.
6. `ready()` — **persist, then apply, then send**. The single most important ordering in the file.
7. `persist()` — appends new entries to the WAL, or rewrites it if entries were truncated.

**`raft/storage.go`** — Disk format, one directory per node:
- `state.json` — `{term, votedFor}`, replaced atomically (write temp file, rename).
- `log.jsonl` — one JSON entry per line, append-only. Rewritten only on truncation/compaction.
- `snapshot.json` — the latest snapshot.
- `Load()` skips entries covered by the snapshot and ignores a half-written last line.

**`raft/storage_test.go`** — Round-trip, truncation, snapshot and torn-write tests for the storage.

### `sim/` — the fake world

**`sim/clock.go`** — A virtual clock and scheduler. Virtual time = real time ÷ scale, and can be paused. `At/After/Every` schedule callbacks in virtual time; `Run` executes them one by one on a single goroutine. Raft ticks and message deliveries are both just scheduled callbacks, which is why slow motion and pause work for everything at once.

**`sim/network.go`** — `Send` picks a random latency, may drop the message (partition or packet loss), and schedules delivery. Per-link FIFO order is preserved (like TCP). `Partition` assigns each node a group id; nodes in different groups cannot talk. Every send / delivery / drop is reported to an observer, which is where the UI's animated dots come from.

### `kv/` — the state machine

**`kv/store.go`**
- `Command{Op, Key, Value, Client, Seq}` is what goes into a log entry (JSON).
- `Apply` executes a committed command. It first checks the **session table**: if this client's `Seq` was already applied, it returns the saved result instead of executing again.
- `Wait/Cancel` let the HTTP request sleep until its command is applied.
- `Snapshot/Restore` include the session table, so duplicate detection survives compaction.

**`kv/store_test.go`** — duplicate suppression, snapshot round trip, waiter notification.

### `transport/`, `server/`, `cmd/raftnode` — the real multi-process mode

**`transport/tcp.go`** — The real-network counterpart of `sim/network.go`. One listener per node; one long-lived outgoing TCP connection per peer, owned by a `writeLoop` goroutine that dials on demand and reconnects after errors. Messages are encoded with `encoding/gob`. `Send` never blocks: each peer has a bounded queue and a full queue drops the message (Raft retransmits on the next heartbeat). TCP keeps one sender's messages in order, the same per-link FIFO the simulator imitates.

**`server/server.go`** — One node as a standalone process: storage + KV store + Raft node + TCP transport + a wall-clock ticker (10 ms per tick). `Do` proposes a command on this node only and returns `NotLeaderError` with a leader hint otherwise. A small pool of client sessions gives exactly-once semantics to requests that do not bring their own id.

**`server/server_test.go`** — Five nodes on real localhost sockets: replicate 100 writes, stop the leader, stop a second node, bring the first back so it has to catch up via a snapshot over the wire, check all replicas converge.

**`cmd/raftnode/main.go`** — The binary for one node: flags for its id and every node's Raft and API addresses; serves `/kv/{key}` (307 to the leader's address when it is a follower) and `/status`. `start-cluster.ps1` launches five of them in separate windows.

### `cluster/` — gluing it together

**`cluster/cluster.go`** — Creates the clock, the network, and N × (storage + store + node). `startNode` builds a node from whatever is on disk (so it is both "first start" and "restart after crash"). Chaos controls: `Kill`, `Restart`, `Partition`, `Heal`, `SetNetwork`, `SetScale`, `Pause`. Converts node and network events into the public event stream.

**`cluster/client.go`** — `Do()` is the client. It takes a session from a pool, increments the sequence number, and loops: find leader → register a waiter → `Propose` → wait for the result or time out and retry **with the same sequence number**. With `via = N` it talks to one node only and returns `NotLeaderError` (→ HTTP 307).

**`cluster/state.go`** — Builds the JSON snapshot of the whole cluster that is pushed to the UI ten times per second.

**`cluster/cluster_test.go`** — The test suite (see Q&A "How did you test it?").

### `checker/`

**`checker/linearizability.go`** — Takes a history of operations with call and return timestamps. Per key, it searches for an ordering that respects real time and is a valid sequence for a register. Depth-first search with memoization on (set of operations already placed, current value). Timed-out writes are allowed to take effect at any later time or never.

### `events/`, `api/`, `cmd/`

**`events/hub.go`** — Pub/sub with non-blocking sends: a slow browser loses events rather than slowing consensus. Keeps the last 200 events to replay to new connections.

**`api/server.go`** — Routes. `kv` handler: no `?node` → smart routing; `?node=N` → 307 redirect if N is a follower, 503 if no leader is known, 504 if not committed in time. `ws` handler: subscribes to the hub and writes batched JSON arrays.

**`cmd/raftkv/main.go`** — Flags, starts the cluster and the HTTP server.
**`cmd/bench/main.go`** — Closed-loop load generator and failover timer; writes `docs/BENCHMARK.md`.

### `web/` — the visualizer

- **`lib/useCluster.ts`** — The WebSocket hook. State and timeline go into React state; RPC messages go into a mutable `Map` that is read every animation frame (hundreds of messages per second must not trigger React renders). Also keeps a local estimate of the server's virtual clock.
- **`lib/api.ts`** — `fetch` wrappers for the KV and chaos endpoints. **`lib/types.ts`** mirrors the Go JSON. **`lib/theme.ts`** holds colours.
- **`components/ClusterRing.tsx`** — The SVG ring. `NodeView` draws a node; `LiveLayer` redraws election timers and message dots on every frame; pointer handlers implement "draw a cut" (which side of the line each node is on = sign of a cross product).
- **`components/LogPanel.tsx`** — Logs aligned by index; solid = committed, dashed = not yet; colour = term.
- **`components/ChaosPanel.tsx`**, **`KVPanel.tsx`**, **`Timeline.tsx`**, **`Inspector.tsx`**, **`Header.tsx`** — controls, client, event list, per-node Raft variables, speed/pause.
- **`app/page.tsx`** — Layout and shared UI state (selected node, draw mode).

---

## 5. Interview questions and answers

**Q1. What is consensus and why is it hard?**
Getting several machines to agree on one value (here: the next log entry) even though machines crash and messages are delayed, lost or reordered. It is hard because a node cannot distinguish "the other node is dead" from "the network is slow", so any decision based on a timeout may be wrong, and the algorithm must stay safe anyway.

**Q2. Why Raft and not Paxos?**
They give the same guarantees. Raft was designed to be understandable: a strong leader, logs that only flow leader → follower, and a clear decomposition into election, replication and safety. Multi-Paxos leaves many of those engineering details open.

**Q3. What happens when the leader crashes?**
Heartbeats stop. After 150–300 ms a follower's election timer fires, it becomes candidate in term+1, wins a majority and becomes leader. It appends a no-op to commit earlier entries. Clients that were talking to the old leader time out and retry on the new one; because requests carry a session id and sequence number, the retry is not applied twice. Measured: ~170 ms median to a new leader.

**Q4. Can there be two leaders at once?**
Two nodes can *believe* they are leader during a partition, but in different terms, and only the one with a majority can commit. There can never be two leaders in the same term, because each node votes once per term and two majorities overlap.

**Q5. What is split brain and how does Raft prevent it?**
Split brain is two sides of a partition both accepting writes and diverging. Raft requires a majority to commit, and only one side of any partition can have a majority. The minority side stalls instead of diverging. (Scenario 5 shows exactly this.)

**Q6. Explain the election restriction.**
A node refuses to vote for a candidate whose log is less up-to-date than its own (higher last term wins; equal terms → longer log wins). A committed entry is on a majority; a candidate needs a majority of votes; the two majorities overlap in at least one node, and that node will refuse a candidate lacking the entry. So every elected leader already has all committed entries, and Raft never needs to copy entries *to* a leader.

**Q7. When exactly is an entry committed?**
When the leader of the current term has stored it on a majority. The leader tracks `matchIndex` per follower and commits the highest index N such that a majority has `matchIndex ≥ N` **and** `log[N].term == currentTerm`.

**Q8. Why the `term == currentTerm` condition? (Figure 8)**
Without it a leader could "commit" an old-term entry that is on a majority, then crash, and a different node with a newer-term entry at that index could win the next election and overwrite it. Committing only current-term entries by counting rules that out; older entries are committed indirectly. That is also why a new leader appends a no-op right away.

**Q9. How do followers' logs get repaired?**
`AppendEntries` carries `prevLogIndex` and `prevLogTerm`. If the follower does not have a matching entry it rejects. The leader lowers `nextIndex` for that follower and retries until they match, then the follower deletes everything after the match point and appends the leader's entries. I implemented the fast version: the follower returns the conflicting term and the first index of that term, so the leader skips a whole term per round trip.

**Q10. What do you persist, and when?**
`currentTerm`, `votedFor` and the log, before sending any message that depends on them. In the code every entry point ends with `ready()`, which calls `persist()` before it flushes the outbox. `commitIndex` and `lastApplied` are not persisted; after a restart the node learns the commit index from the leader and rebuilds the KV store from the snapshot plus the log.

**Q11. What if the node crashes in the middle of a disk write?**
The hard state and snapshot are written to a temp file and renamed, so they are either old or new. The log is append-only; a half-written last line fails to parse and is ignored — safe, because the entry was never acknowledged. With `fsync` off (the default) a power failure could lose acknowledged writes; I say so in the README.

**Q12. Is your store linearizable? What does that mean?**
Linearizable means every operation appears to take effect at a single instant between its call and its return, consistent with real-time order: once a write returns, every later read sees it. Yes: all operations, including reads, go through the log and are answered only after commit, and client retries are deduplicated. I verify it with a checker on histories recorded under chaos.

**Q13. Why do reads go through the log? Isn't that slow?**
A leader that has been partitioned away does not know it has been replaced; if it answered reads from local state it could return stale data. Committing the read proves it is still the leader. It costs a log write per read. The standard optimizations are **ReadIndex** (leader records the commit index, confirms leadership with one heartbeat round, then reads locally) and **leader leases** (rely on bounded clock drift to skip the round). I did not implement those.

**Q14. A client's request times out. Did it happen?**
Unknown — it may or may not have been committed. So the client retries with the same `(client id, sequence number)`. The state machine keeps the last sequence number and result per client; a duplicate returns the stored result without executing again. The table is part of the snapshot so this still works after compaction.

**Q15. How do snapshots work, and what can go wrong?**
A node serializes the state machine at `lastApplied`, saves it with that index and term, and drops the log prefix. A follower that needs dropped entries gets the whole snapshot via `InstallSnapshot`, replaces its state, and continues from there. Care points: write the snapshot before truncating the log; keep the last included term for the next consistency check (the sentinel `log[0]`); ignore a snapshot older than what you have already committed.

**Q16. How did you test it?**
- Unit tests for storage, the KV state machine and the checker itself (it must reject a stale read and a lost write).
- Scenario tests on a real 5-node cluster: initial election, re-election, losing quorum, partition with a stranded leader, full restart from disk, snapshot install.
- A chaos test: 4 clients do random operations on 3 keys for 6 seconds while a "monkey" crashes leaders and random nodes, restarts them, partitions the network and 5% of packets are dropped. Afterwards I assert election safety (one leader per term, from the event stream), log matching and identical replicas, and linearizability of the ~200-operation history.
- The node also panics if it ever receives `AppendEntries` from another leader in its own term or is asked to truncate a committed entry, so the tests would crash loudly on a safety bug.

**Q17. How does the linearizability checker work?**
Per key, it does a depth-first search for a valid order. At each step it may pick any pending operation that was called before the earliest return among pending operations (otherwise real-time order would be violated), checks it against the current value (a read must match), and recurses. It memoizes on (which operations are placed, current value) to avoid exploring the same state twice. Timed-out writes are optional and unbounded in time. It is the Wing & Gong algorithm; tools like Porcupine and Jepsen's Knossos do the same thing at scale.

**Q18. Where is the bottleneck? How does it get 15k ops/s?**
Batching. The leader keeps at most one `AppendEntries` in flight per follower; commands that arrive meanwhile are sent together when the ack returns. So throughput scales with the number of concurrent clients while latency stays around one round trip (~6 ms with 1–3 ms simulated one-way latency). With a single client it is ~200 ops/s — purely latency bound. Next limits would be the single scheduler goroutine and JSON encoding of the WAL.

**Q19. Why is failover ~170 ms and how would you reduce it?**
It is the election timeout: followers wait 150–300 ms after the last heartbeat. Lowering the timeout speeds failover but risks false elections when the network or a GC pause is slower than the timeout. The rule is `broadcast time ≪ election timeout ≪ mean time between failures`.

**Q20. What does CAP say about your system?**
It is CP. During a partition the minority side refuses writes and reads (unavailable) rather than return inconsistent data. The majority side stays available.

**Q21. How many failures can it tolerate?**
A cluster of 2f+1 nodes tolerates f crash failures: 5 nodes → 2. It does not tolerate Byzantine (lying) nodes; that needs a different algorithm such as PBFT.

**Q22. Why an odd number of nodes?**
6 nodes need a majority of 4 and still only tolerate 2 failures — an extra node adds cost and no fault tolerance.

**Q23. What is missing compared with etcd or Consul?**
Membership changes (joint consensus), ReadIndex/leases, chunked snapshots, fsync batching, session expiry, TLS between nodes. I list these under "Limits" in the README. PreVote, CheckQuorum and a real TCP transport *are* implemented.

**Q24. What are PreVote and CheckQuorum?**
*PreVote*: before incrementing its term, a would-be candidate asks whether it could win. A node that was partitioned away (and kept timing out) would otherwise come back with a huge term and force the healthy leader to step down. *CheckQuorum*: a leader that has not heard from a majority for an election timeout steps down by itself instead of continuing to believe it leads, so its clients fail fast and look for the real leader.

Both are implemented (`startPreVote`, `handlePreVote`, `checkQuorum` in `raft/raft.go`) and you can watch them: press *Split 2 | 3*. The old leader turns into a follower after about 300 ms ("lost contact with a majority"), and the two cut-off nodes show **PRE-VOTE** and stay in their old term no matter how long you wait. Heal the network and the majority's leader keeps its job in the same term. Two details worth mentioning: a pre-vote is not a promise, so nothing is written to disk and the voter's timer is not reset; and a node refuses a pre-vote while it has heard from a leader recently, which is what stops a returning node from disrupting a healthy cluster. Tests: `TestPreVotePreventsDisruption`, `TestCheckQuorumLeaderStepsDown`.

**Q25. Is it really distributed, or just a simulation?**
Both modes exist. The visualizer uses a simulated network because the goal there is to study and demonstrate failure behaviour: inject an exact fault with one call, slow time down 60×, pause, and write repeatable tests. `./start-cluster.ps1` runs five separate processes over real TCP sockets, and `server/server_test.go` tests that mode (crash the leader, catch up via snapshot over the wire). The Raft package is the same in both — it calls a `Send` function and receives `Step` calls — which is the point of the message-passing design. Swapping gob-over-TCP for gRPC would only touch `transport/`.

**Q26. What was the hardest bug / most subtle part?**
Good honest candidates: (1) ordering — making sure nothing is sent before it is persisted (solved structurally with `ready()`); (2) the duplicate-detection table had to be inside the snapshot, and client session ids had to be random, because the table survives restarts and reused ids would make new requests look like old duplicates; (3) snapshot edge cases around the sentinel entry (`prevLogIndex` falling inside a follower's snapshot).

**Q27. Why Go?**
Cheap goroutines and channels for the server side, a strong standard library (HTTP server, JSON, `container/heap`), a single static binary to deploy, and it is the language of the systems this imitates (etcd, Consul, CockroachDB).

**Q28. How does the UI stay smooth at hundreds of messages per second?**
RPC events bypass React state: they are stored in a mutable map and drawn inside one `requestAnimationFrame` loop. Only the 10 Hz cluster snapshot and the timeline use React state. The server batches events into one WebSocket frame, and the hub drops events for a slow client instead of blocking the cluster.

---

## 6. Numbers to remember

| | |
|---|---|
| Cluster | 5 nodes, tolerates 2 failures, majority = 3 |
| Election timeout / heartbeat | 150–300 ms random / 50 ms |
| Throughput | ~16,500 linearizable writes/s (128 clients), ~5,300 (32 clients), ~200 (1 client) |
| Latency | p50 6.3 ms, p99 31 ms at 128 clients (1–3 ms simulated one-way network latency) |
| Failover | 170 ms median, 239 ms worst of 15 trials to elect; 180 ms median to first committed write |
| Tests | 10 simulated-cluster scenarios (incl. PreVote, CheckQuorum) + chaos run of ~200 operations checked for linearizability + a 5-node cluster over real TCP; race detector in CI |
| Size | ~3,700 lines of Go (heavily commented), ~1,600 lines of TypeScript, ~900 lines of tests |

Re-measure on your own machine before quoting: `go run ./cmd/bench -out docs/BENCHMARK.md`.

---

## 7. Resume bullets

- Built a fault-tolerant distributed key-value store in **Go**, implementing the **Raft consensus algorithm from scratch** (leader election, log replication, write-ahead log persistence, snapshot-based log compaction); a 5-node cluster sustains **~16,500 linearizable writes/s at 6.3 ms median latency** (128 concurrent clients) through request batching; runs as 5 processes over TCP or in a fault-injecting simulator.
- Achieved automatic leader **failover in ~170 ms (median of 15 trials)** with zero lost acknowledged writes, plus PreVote and CheckQuorum so partitions cannot disrupt a healthy leader; verified correctness with a chaos test suite that injects node crashes, network partitions and 5% packet loss, and validates client histories with a self-written **linearizability checker**.
- Designed a real-time cluster visualizer in **Next.js, TypeScript and WebSockets** that animates every RPC, election timer and per-node log at 60 fps, with interactive fault injection (kill/restart nodes, draw network partitions, latency, packet loss, 60× slow motion).

Shorter one-liner for a projects section:
*Raft KV — distributed key-value store on a from-scratch Raft implementation in Go; ~16.5k linearizable writes/s, ~170 ms failover, chaos-tested with a linearizability checker; live Next.js visualizer.*
