"use client";

import { useMemo, useState } from "react";
import type { ClusterEvent } from "@/lib/types";
import { fmtTime } from "@/lib/theme";

type Category = "election" | "log" | "chaos";

const CATEGORY: Record<string, Category> = {
  role: "election",
  vote: "election",
  prevote: "election",
  append: "log",
  commit: "log",
  truncate: "log",
  snapshot: "log",
  restore: "log",
  kill: "chaos",
  add: "chaos",
  remove: "chaos",
  learner: "log",
  config: "log",
  restart: "chaos",
  partition: "chaos",
  heal: "chaos",
  network: "chaos",
  speed: "chaos",
  pause: "chaos",
  resume: "chaos",
};

const COLOR: Record<Category, string> = { election: "#fbbf24", log: "#34d399", chaos: "#f87171" };

function describe(e: ClusterEvent): { title: string; sub?: string; color?: string } {
  const n = `N${e.node}`;
  switch (e.kind) {
    case "role":
      if (e.role === "leader") return { title: `${n} became leader`, sub: `won a majority in term ${e.term}`, color: "#34d399" };
      if (e.role === "candidate") return { title: `${n} started an election`, sub: `a majority pre-voted yes → term ${e.term}` };
      return { title: `${n} stepped down to follower`, sub: e.detail || `saw term ${e.term}`, color: "#60a5fa" };
    case "prevote":
      return { title: `${n} lost its leader, asks for pre-votes`, sub: `stays in term ${e.term} unless a majority agrees` };
    case "vote":
      return { title: `${n} voted for N${e.peer}`, sub: `term ${e.term}` };
    case "append":
      return { title: `${n} appended index ${e.index}`, sub: e.detail === "no-op" ? "no-op entry for the new term" : `term ${e.term}, not yet committed` };
    case "commit":
      return { title: `${n} advanced commit to ${e.index}`, sub: "stored on a majority" };
    case "truncate":
      return { title: `${n} discarded entries from ${e.index}`, sub: "conflicted with the leader's log", color: "#f87171" };
    case "snapshot":
      return { title: `${n} took a snapshot at ${e.index}`, sub: "log compacted", color: "#e879f9" };
    case "restore":
      return { title: `${n} installed a snapshot (≤ ${e.index})`, sub: "too far behind for log replay", color: "#e879f9" };
    case "add":
      return { title: `${n} started`, sub: "joining the cluster", color: "#a78bfa" };
    case "learner":
      return { title: `N${e.peer} added as a learner`, sub: "catching up before it may vote", color: "#a78bfa" };
    case "config":
      return { title: `New configuration committed`, sub: `voters: ${(e.detail ?? "").replace("CONFIG ", "")}`, color: "#a78bfa" };
    case "remove":
      return { title: `${n} removed from the cluster`, sub: "configuration change committed, node shut down", color: "#a78bfa" };
    case "kill":
      return { title: `${n} crashed`, sub: "in-memory state lost" };
    case "restart":
      return { title: `${n} restarted`, sub: "recovered term, vote and log from disk", color: "#34d399" };
    case "partition":
      return { title: "Network partitioned", sub: e.detail };
    case "heal":
      return { title: "Network healed", color: "#34d399" };
    case "network":
      return { title: "Network conditions changed", sub: e.detail };
    case "speed":
      return { title: "Simulation speed changed", sub: e.detail, color: "#a78bfa" };
    case "pause":
      return { title: "Simulation paused", color: "#a78bfa" };
    case "resume":
      return { title: "Simulation resumed", color: "#a78bfa" };
  }
  return { title: e.kind, sub: e.detail };
}

export default function Timeline({ events, onClear }: { events: ClusterEvent[]; onClear: () => void }) {
  const [show, setShow] = useState<Record<Category, boolean>>({ election: true, log: true, chaos: true });
  const visible = useMemo(() => events.filter((e) => show[CATEGORY[e.kind] ?? "chaos"]).slice(0, 80), [events, show]);

  return (
    <div className="panel flex min-h-0 flex-1 flex-col p-3">
      <div className="mb-2 flex items-center justify-between">
        <h2 className="panel-title">Event timeline</h2>
        <button className="text-[11px] text-faint hover:text-ink" onClick={onClear}>
          Clear
        </button>
      </div>
      <div className="mb-2 flex gap-1.5">
        {(Object.keys(show) as Category[]).map((c) => (
          <button
            key={c}
            onClick={() => setShow((s) => ({ ...s, [c]: !s[c] }))}
            aria-pressed={show[c]}
            className={`flex h-6 items-center gap-1.5 rounded-full border px-2.5 text-[11px] capitalize transition-colors ${show[c] ? "border-line-strong bg-raised text-ink" : "border-line text-faint"}`}
          >
            <i className="h-1.5 w-1.5 rounded-full" style={{ background: show[c] ? COLOR[c] : "#4b5060" }} />
            {c === "log" ? "Replication" : c === "election" ? "Elections" : "Chaos"}
          </button>
        ))}
      </div>
      <ol className="flex min-h-0 flex-1 flex-col gap-0.5 overflow-y-auto pr-1">
        {visible.length === 0 && <p className="text-xs text-faint">Nothing yet. Kill the leader or send a write to see events.</p>}
          {visible.map((e) => {
            const d = describe(e);
            const color = d.color ?? COLOR[CATEGORY[e.kind] ?? "chaos"];
            return (
              <li
                key={e.seq}
                className="slide-in flex gap-2.5 rounded-md px-1.5 py-1 hover:bg-white/[0.03]"
              >
                <span className="mt-[5px] h-2 w-2 shrink-0 rounded-full" style={{ background: color }} />
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-xs text-ink">{d.title}</span>
                  {d.sub && <span className="block truncate text-[11px] text-muted">{d.sub}</span>}
                </span>
                <span className="shrink-0 font-mono text-[10px] text-faint">{fmtTime(e.t)}</span>
              </li>
            );
          })}
      </ol>
    </div>
  );
}
