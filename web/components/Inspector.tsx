"use client";

import { chaos } from "@/lib/api";
import type { ClusterState } from "@/lib/types";
import { ROLE_COLOR, roleOf } from "@/lib/theme";

function Row({ k, v, hint }: { k: string; v: React.ReactNode; hint?: string }) {
  return (
    <div className="flex items-baseline justify-between gap-3 py-[3px]" title={hint}>
      <span className="text-[11px] text-muted">{k}</span>
      <span className="font-mono text-[11px] text-ink">{v}</span>
    </div>
  );
}

// Inspector shows the Raft state of one node — the same variables as
// Figure 2 of the Raft paper.
export default function Inspector({ state, id, onClose }: { state: ClusterState; id: number; onClose: () => void }) {
  const n = state.nodes.find((x) => x.id === id);
  if (!n) return null;
  const role = roleOf(n);
  const tick = state.tickMs;
  return (
    <div className="panel p-3">
      <div className="mb-2 flex items-center justify-between">
        <h2 className="flex items-center gap-2 text-sm font-semibold">
          <i className="h-2.5 w-2.5 rounded-full" style={{ background: ROLE_COLOR[role] }} />
          Node {n.id}
          <span className="text-xs font-normal capitalize text-muted">{role === "dead" ? "down" : role}</span>
        </h2>
        <div className="flex items-center gap-1.5">
          {n.alive ? (
            <button className="btn btn-danger h-6 px-2 text-[11px]" onClick={() => chaos.kill(n.id)}>
              Kill
            </button>
          ) : (
            <button className="btn h-6 border-leader/40 px-2 text-[11px] text-leader" onClick={() => chaos.restart(n.id)}>
              Restart
            </button>
          )}
          <button className="btn h-6 w-6 px-0" onClick={onClose} aria-label="Close inspector">
            ×
          </button>
        </div>
      </div>

      <p className="panel-title mb-0.5 mt-2 !text-[10px]">Persistent (on disk)</p>
      <Row k="currentTerm" v={n.term} hint="Latest term this node has seen" />
      <Row k="votedFor" v={n.votedFor ? `N${n.votedFor}` : "—"} hint="Who got this node's vote in the current term" />
      <Row k="log" v={`${n.lastIndex} entries · last term ${n.lastTerm}`} />
      <Row k="snapshot" v={n.snapshotIndex ? `≤ index ${n.snapshotIndex}` : "none"} hint="Entries up to here were compacted" />

      <p className="panel-title mb-0.5 mt-2 !text-[10px]">Volatile (in memory)</p>
      <Row k="commitIndex" v={n.commitIndex} hint="Highest entry known to be stored on a majority" />
      <Row k="lastApplied" v={n.lastApplied} hint="Highest entry applied to the KV store" />
      <Row k="known leader" v={n.leader ? `N${n.leader}` : "—"} />
      {n.alive && n.role !== "leader" && (
        <Row
          k="election timer"
          v={`${Math.round(n.electionElapsed * tick)} / ${Math.round(n.electionTimeout * tick)} ms`}
          hint="Randomized between 150 and 300 ms; reset by every heartbeat"
        />
      )}
      {n.alive && n.role === "candidate" && n.votes && (
        <Row k="votes" v={Object.entries(n.votes).map(([p, ok]) => `N${p}${ok ? "✓" : "✗"}`).join(" ")} />
      )}

      {n.alive && n.role === "leader" && n.nextIndex && n.matchIndex && (
        <>
          <p className="panel-title mb-1 mt-2 !text-[10px]">Leader view of followers</p>
          <table className="w-full font-mono text-[11px]">
            <thead>
              <tr className="text-left text-faint">
                <th className="font-normal">peer</th>
                <th className="font-normal">nextIndex</th>
                <th className="font-normal">matchIndex</th>
              </tr>
            </thead>
            <tbody>
              {Object.keys(n.nextIndex).map((p) => (
                <tr key={p}>
                  <td>N{p}</td>
                  <td>{n.nextIndex![p]}</td>
                  <td className={n.matchIndex![p] >= n.commitIndex ? "text-leader" : "text-candidate"}>{n.matchIndex![p]}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
