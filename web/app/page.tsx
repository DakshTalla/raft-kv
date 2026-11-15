"use client";

import { useState } from "react";
import ChaosPanel from "@/components/ChaosPanel";
import ClusterRing, { type Cut } from "@/components/ClusterRing";
import Header from "@/components/Header";
import Inspector from "@/components/Inspector";
import KVPanel from "@/components/KVPanel";
import LogPanel from "@/components/LogPanel";
import Timeline from "@/components/Timeline";
import { chaos } from "@/lib/api";
import { useCluster } from "@/lib/useCluster";

const LEGEND = [
  { color: "#fde68a", label: "PreVote", hollow: true },
  { color: "#fbbf24", label: "RequestVote" },
  { color: "#34d399", label: "AppendEntries (number = entries)" },
  { color: "#34d399", label: "heartbeat", small: true },
  { color: "#7dd3fc", label: "ack", small: true },
  { color: "#e879f9", label: "InstallSnapshot", square: true },
  { color: "#f87171", label: "rejected / dropped", hollow: true },
];

export default function Page() {
  const feed = useCluster();
  const { state } = feed;
  const [selected, setSelected] = useState<number | null>(null);
  const [drawMode, setDrawMode] = useState(false);
  const [cut, setCut] = useState<Cut | null>(null);

  const heal = () => {
    setCut(null);
    chaos.heal();
  };

  return (
    <div className="flex min-h-screen flex-col lg:h-screen">
      <Header state={state} connected={feed.connected} msgRate={feed.msgRate} />

      {!state ? (
        <div className="flex flex-1 flex-col items-center justify-center gap-3 p-8 text-center">
          <div className="h-10 w-10 animate-spin rounded-full border-2 border-line-strong border-t-accent" />
          <p className="text-sm text-ink">Connecting to the cluster…</p>
          <p className="max-w-sm text-xs text-muted">
            Start the backend with <code className="rounded bg-raised px-1.5 py-0.5 font-mono text-ink">./start.ps1</code> (or{" "}
            <code className="rounded bg-raised px-1.5 py-0.5 font-mono text-ink">go run ./cmd/raftkv</code>). This page reconnects automatically.
          </p>
        </div>
      ) : (
        <main className="grid min-h-0 flex-1 grid-cols-1 gap-3 p-3 lg:grid-cols-[290px_minmax(0,1fr)_330px]">
          {/* Left: failure injection + client */}
          <section className="flex min-h-0 flex-col gap-3 lg:overflow-y-auto">
            <ChaosPanel state={state} selected={selected} onSelect={setSelected} drawMode={drawMode} setDrawMode={setDrawMode} onHeal={heal} />
            <KVPanel state={state} />
          </section>

          {/* Center: the cluster and its logs */}
          <section className="flex min-h-0 min-w-0 flex-col gap-3">
            <div className="panel dot-grid relative flex min-h-[420px] flex-1 flex-col overflow-hidden">
              <div className="relative min-h-0 flex-1">
                <div className="absolute inset-0">
                <ClusterRing
                  state={state}
                  flights={feed.flights}
                  virtualNow={feed.virtualNow}
                  selected={selected}
                  onSelect={setSelected}
                  drawMode={drawMode}
                  cut={cut}
                  onCut={(c, groups) => {
                    setCut(c);
                    setDrawMode(false);
                    chaos.partition(groups);
                  }}
                />
                </div>
              </div>
              <div className="flex flex-wrap items-center justify-center gap-x-4 gap-y-1 border-t border-line/70 bg-panel/70 px-3 py-2 text-[11px] text-muted">
                {LEGEND.map((l) => (
                  <span key={l.label} className="flex items-center gap-1.5">
                    <i
                      className={`inline-block ${l.small ? "h-1.5 w-1.5" : "h-2.5 w-2.5"} ${l.square ? "rounded-[2px]" : "rounded-full"}`}
                      style={l.hollow ? { border: `1.5px solid ${l.color}` } : { background: l.color }}
                    />
                    {l.label}
                  </span>
                ))}
                <span className="text-faint">· ring around a node = time left before it starts an election</span>
              </div>
            </div>
            <LogPanel state={state} selected={selected} onSelect={setSelected} />
          </section>

          {/* Right: node details + what just happened */}
          <section className="flex min-h-0 flex-col gap-3">
            {selected !== null && <Inspector state={state} id={selected} onClose={() => setSelected(null)} />}
            <Timeline events={feed.timeline} onClear={feed.clearTimeline} />
          </section>
        </main>
      )}
    </div>
  );
}
