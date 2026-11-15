"use client";

import { motion } from "framer-motion";
import type { ClusterState } from "@/lib/types";
import { ROLE_COLOR, roleOf, termColor } from "@/lib/theme";

const COLS = 12;

// LogPanel shows every node's log aligned by index, so the Log Matching
// property is visible at a glance: a column is "safe" once the same entry
// appears, solid (committed), on a majority of rows.
export default function LogPanel({ state, selected, onSelect }: { state: ClusterState; selected: number | null; onSelect: (id: number) => void }) {
  const maxLast = Math.max(1, ...state.nodes.map((n) => n.lastIndex));
  const from = Math.max(1, maxLast - COLS + 1);
  const indices = Array.from({ length: COLS }, (_, i) => from + i);
  const leader = state.nodes.find((n) => n.id === state.leader);
  const grid = { gridTemplateColumns: `78px repeat(${COLS}, minmax(0, 1fr))` };

  return (
    <div className="panel flex min-h-0 flex-col p-3">
      <div className="mb-2 flex items-center justify-between gap-3">
        <h2 className="panel-title">Replicated logs</h2>
        <div className="flex items-center gap-3 text-[11px] text-muted">
          <span className="flex items-center gap-1.5">
            <i className="inline-block h-2.5 w-4 rounded-sm bg-follower/80" /> committed
          </span>
          <span className="flex items-center gap-1.5">
            <i className="inline-block h-2.5 w-4 rounded-sm border border-dashed border-follower/80" /> not yet committed
          </span>
          <span className="hidden items-center gap-1.5 sm:flex">colour = term</span>
        </div>
      </div>

      <div className="grid gap-x-1 gap-y-1 font-mono text-[10px]" style={grid}>
        <div className="text-faint">index</div>
        {indices.map((i) => (
          <div key={i} className={`text-center ${leader && i === leader.commitIndex ? "text-leader" : "text-faint"}`}>
            {i}
            {leader && i === leader.commitIndex ? " ✓" : ""}
          </div>
        ))}
      </div>

      <div className="mt-1 flex flex-col gap-1">
        {state.nodes.map((n) => {
          const role = roleOf(n);
          const byIndex = new Map(n.log.map((e) => [e.index, e]));
          return (
            <div key={n.id} className="grid items-stretch gap-x-1" style={grid}>
              <button
                onClick={() => onSelect(n.id)}
                className={`flex items-center gap-1.5 rounded-md px-1.5 text-left font-mono text-[11px] ${selected === n.id ? "bg-accent/15" : "hover:bg-white/5"}`}
              >
                <i className="h-2 w-2 shrink-0 rounded-full" style={{ background: ROLE_COLOR[role] }} />
                <span className={role === "dead" ? "text-faint" : "text-ink"}>N{n.id}</span>
                <span className="text-faint">✓{n.commitIndex}</span>
              </button>
              {indices.map((i) => {
                const e = byIndex.get(i);
                if (!e) {
                  const compacted = i <= n.snapshotIndex;
                  return (
                    <div
                      key={i}
                      className={`flex h-7 items-center justify-center rounded-md border text-[9px] ${compacted ? "border-line bg-white/[0.03] text-faint" : "border-dashed border-line/60 text-transparent"}`}
                      title={compacted ? `index ${i}: compacted into snapshot (≤ ${n.snapshotIndex})` : `index ${i}: empty`}
                    >
                      {compacted ? "snap" : "·"}
                    </div>
                  );
                }
                const committed = i <= n.commitIndex;
                const c = termColor(e.term);
                return (
                    <motion.div
                      key={`${i}-${e.term}`}
                      initial={{ opacity: 0, scale: 0.6 }}
                      animate={{ opacity: role === "dead" ? 0.4 : 1, scale: 1 }}
                      transition={{ duration: 0.22 }}
                      className="flex h-7 min-w-0 items-center justify-center overflow-hidden rounded-md border px-1 font-mono text-[9.5px] leading-none"
                      style={{
                        borderColor: c,
                        borderStyle: committed ? "solid" : "dashed",
                        background: committed ? `${c}2e` : "transparent",
                        color: committed ? "#f3f4f8" : c,
                      }}
                      title={`index ${e.index} · term ${e.term} · ${e.cmd} · ${committed ? "committed" : "uncommitted"}`}
                    >
                      <span className="truncate">{e.cmd}</span>
                    </motion.div>
                );
              })}
            </div>
          );
        })}
      </div>
    </div>
  );
}
