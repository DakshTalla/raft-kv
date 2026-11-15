// Types mirroring the JSON produced by the Go server (cluster/state.go and
// events/hub.go). Keep the two in sync.

export type Role = "follower" | "candidate" | "leader";

export interface LogEntry {
  index: number;
  term: number;
  cmd: string;
}

export interface NodeState {
  id: number;
  alive: boolean;
  group: number;
  role: Role;
  term: number;
  votedFor: number;
  leader: number;
  commitIndex: number;
  lastApplied: number;
  lastIndex: number;
  lastTerm: number;
  snapshotIndex: number;
  snapshotTerm: number;
  electionElapsed: number;
  electionTimeout: number;
  heartbeatElapsed: number;
  heartbeatTicks: number;
  votes?: Record<string, boolean>;
  nextIndex?: Record<string, number>;
  matchIndex?: Record<string, number>;
  log: LogEntry[];
  keys: number;
}

export interface ClusterState {
  now: number; // virtual ms
  scale: number; // slow-motion factor (1 = real time)
  paused: boolean;
  tickMs: number;
  leader: number;
  nodes: NodeState[];
  network: { latencyMinMs: number; latencyMaxMs: number; dropRate: number; partitioned: boolean };
  kv: { key: string; value: string }[] | null;
  kvTotal: number;
}

export type MsgType =
  | "RequestVote"
  | "RequestVoteResp"
  | "AppendEntries"
  | "AppendEntriesResp"
  | "InstallSnapshot"
  | "InstallSnapshotResp";

export interface Msg {
  id: number;
  type: MsgType;
  from: number;
  to: number;
  term: number;
  sentAt: number;
  deliverAt: number;
  entries: number;
  heartbeat?: boolean;
  success: boolean;
  reason?: string;
}

export interface ClusterEvent {
  seq: number;
  t: number;
  kind: string;
  node?: number;
  term?: number;
  role?: Role;
  index?: number;
  peer?: number;
  detail?: string;
  msg?: Msg;
  state?: ClusterState;
}

// A message being animated on the ring.
export interface Flight extends Msg {
  status: "flying" | "arrived" | "dropped" | "blocked";
  endedAt?: number; // performance.now() when it arrived / was dropped
  startedAt: number; // performance.now() when we first saw it
  frozenAt?: number; // progress 0..1 where a dropped message stopped
}

export interface ClientResult {
  id: number;
  op: "put" | "get" | "delete";
  key: string;
  value?: string;
  ok: boolean;
  found?: boolean;
  node?: number;
  index?: number;
  latencyMs?: number;
  redirectedFrom?: number;
  error?: string;
  status: number;
}
