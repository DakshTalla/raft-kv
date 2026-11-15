"use client";

import { chaos } from "@/lib/api";
import type { ClusterState } from "@/lib/types";

// The server's "scale" is a slow-down factor; show it as playback speed.
const SPEEDS = [
  { scale: 1, label: "real-time", hint: "Real time: 150–300 ms election timeouts" },
  { scale: 5, label: "5× slower", hint: "5× slow motion" },
  { scale: 20, label: "20×", hint: "20× slow motion" },
  { scale: 60, label: "60×", hint: "60× slow motion" },
];

function Stat({ label, value, color }: { label: string; value: React.ReactNode; color?: string }) {
  return (
    <div className="flex flex-col leading-tight">
      <span className="text-[10px] uppercase tracking-[0.1em] text-faint">{label}</span>
      <span className="font-mono text-sm font-medium" style={{ color }}>
        {value}
      </span>
    </div>
  );
}

export default function Header({ state, connected, msgRate }: { state: ClusterState | null; connected: boolean; msgRate: number }) {
  const leader = state?.nodes.find((n) => n.id === state.leader);
  const alive = state?.nodes.filter((n) => n.alive).length ?? 0;
  const total = state?.nodes.length ?? 0;
  const term = state ? Math.max(...state.nodes.filter((n) => n.alive).map((n) => n.term), 0) : 0;

  return (
    <header className="flex flex-wrap items-center gap-x-6 gap-y-2 border-b border-line bg-panel/80 px-4 py-2.5 backdrop-blur">
      <div className="flex items-center gap-2.5">
        <svg width="26" height="26" viewBox="0 0 26 26" aria-hidden>
          <circle cx="13" cy="13" r="12" fill="none" stroke="#2b3040" />
          {[0, 1, 2, 3, 4].map((i) => {
            const a = ((-90 + 72 * i) * Math.PI) / 180;
            return <circle key={i} cx={13 + 9 * Math.cos(a)} cy={13 + 9 * Math.sin(a)} r={i === 0 ? 3 : 2.2} fill={i === 0 ? "#34d399" : "#60a5fa"} />;
          })}
        </svg>
        <div className="leading-tight">
          <h1 className="text-sm font-semibold tracking-tight">Raft KV</h1>
          <p className="text-[11px] text-muted">consensus, live</p>
        </div>
      </div>

      <div className="flex items-center gap-5">
        <Stat label="Leader" value={leader ? `N${leader.id}` : "none"} color={leader ? "#34d399" : "#fbbf24"} />
        <Stat label="Term" value={term} />
        <Stat label="Committed" value={leader?.commitIndex ?? "—"} />
        <Stat label="Nodes up" value={`${alive}/${total}`} color={alive * 2 > total ? undefined : "#f87171"} />
        <Stat label="RPCs / s" value={msgRate} />
      </div>

      <div className="ml-auto flex items-center gap-3">
        <div className="flex items-center gap-1 rounded-lg border border-line bg-bg p-1" role="group" aria-label="Simulation speed">
          <span className="px-1.5 text-[10px] uppercase tracking-[0.1em] text-faint">Speed</span>
          {SPEEDS.map((s) => (
            <button
              key={s.scale}
              title={s.hint}
              onClick={() => chaos.speed(s.scale)}
              aria-pressed={state?.scale === s.scale}
              className={`h-6 min-w-9 rounded-md px-2 font-mono text-[11px] transition-colors ${state?.scale === s.scale ? "bg-accent/20 text-[#d6c9ff]" : "text-muted hover:text-ink"}`}
            >
              {s.label}
            </button>
          ))}
        </div>
        <button
          className={`btn w-[88px] ${state?.paused ? "btn-active" : ""}`}
          onClick={() => (state?.paused ? chaos.resume() : chaos.pause())}
          disabled={!state}
        >
          {state?.paused ? "▶ Resume" : "❚❚ Pause"}
        </button>
        <span className="flex items-center gap-1.5 text-[11px] text-muted" role="status">
          <i className={`h-2 w-2 rounded-full ${connected ? "bg-leader shadow-[0_0_8px_#34d399]" : "animate-pulse bg-danger"}`} />
          {connected ? "live" : "offline"}
        </span>
      </div>
    </header>
  );
}
