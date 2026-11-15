"use client";

import { useEffect, useState } from "react";
import { chaos } from "@/lib/api";
import type { ClusterState } from "@/lib/types";
import { ROLE_COLOR, roleOf } from "@/lib/theme";

interface Props {
  state: ClusterState;
  selected: number | null;
  onSelect: (id: number) => void;
  drawMode: boolean;
  setDrawMode: (on: boolean) => void;
  onHeal: () => void;
}

// ChaosPanel injects failures: crash/restart nodes, partition the network,
// add latency and packet loss.
export default function ChaosPanel({ state, selected, onSelect, drawMode, setDrawMode, onHeal }: Props) {
  const net = state.network;
  const [latency, setLatency] = useState(net.latencyMaxMs);
  const [drop, setDrop] = useState(Math.round(net.dropRate * 100));
  const [dirty, setDirty] = useState(false);
  const [changing, setChanging] = useState<string | null>(null); // membership change in flight
  const [memberError, setMemberError] = useState<string | null>(null);

  const change = async (label: string, run: () => Promise<string | null>) => {
    setChanging(label);
    setMemberError(null);
    const err = await run();
    setChanging(null);
    if (err) setMemberError(err);
  };

  // Follow server-side changes unless the user is mid-drag.
  useEffect(() => {
    if (!dirty) {
      setLatency(net.latencyMaxMs);
      setDrop(Math.round(net.dropRate * 100));
    }
  }, [net.latencyMaxMs, net.dropRate, dirty]);

  const apply = (lat: number, dr: number) => {
    chaos.conditions(Math.max(1, Math.round(lat * 0.4)), lat, dr / 100).finally(() => setDirty(false));
  };

  const leader = state.leader;
  const ids = state.nodes.map((n) => n.id);
  const isolateLeader = () => leader && chaos.partition([[leader], ids.filter((i) => i !== leader)]);
  const splitMinority = () => {
    // Put the leader on the minority side: the interesting case.
    const buddy = ids.find((i) => i !== leader)!;
    const minority = leader ? [leader, buddy] : ids.slice(0, 2);
    chaos.partition([minority, ids.filter((i) => !minority.includes(i))]);
  };

  return (
    <div className="panel p-3">
      <div className="mb-2.5 flex items-center justify-between">
        <h2 className="panel-title">Nodes</h2>
        <button
          className="btn h-6 px-2 text-[11px]"
          disabled={!!changing || state.nodes.length >= 9}
          onClick={() => change("Adding a node…", chaos.addNode)}
          title="Start a new node: it catches up as a learner, then becomes a voter"
        >
          + Add node
        </button>
      </div>

      <div className="flex flex-col gap-1">
        {state.nodes.map((n) => {
          const role = roleOf(n);
          return (
            <div
              key={n.id}
              className={`flex items-center gap-2 rounded-lg border px-2 py-1 ${selected === n.id ? "border-accent/60 bg-accent/10" : "border-transparent hover:bg-white/[0.03]"}`}
            >
              <button className="flex min-w-0 flex-1 items-center gap-2 text-left" onClick={() => onSelect(n.id)}>
                <i className="h-2.5 w-2.5 shrink-0 rounded-full" style={{ background: ROLE_COLOR[role], boxShadow: role === "leader" ? `0 0 10px ${ROLE_COLOR.leader}` : undefined }} />
                <span className="font-mono text-xs">N{n.id}</span>
                <span className="truncate text-[11px] text-muted">
                  {role === "dead" ? "down" : n.membership && n.membership !== "voter" ? n.membership : role} · term {n.term}
                </span>
              </button>
              {n.alive ? (
                <button className="btn btn-danger h-6 px-2 text-[11px]" onClick={() => chaos.kill(n.id)}>
                  Kill
                </button>
              ) : (
                <button className="btn h-6 border-leader/40 px-2 text-[11px] text-leader" onClick={() => chaos.restart(n.id)}>
                  Restart
                </button>
              )}
              <button
                className="btn btn-danger h-6 w-6 px-0 text-[13px] leading-none"
                disabled={!!changing || state.nodes.length <= 1}
                onClick={() => change(`Removing N${n.id}…`, () => chaos.removeNode(n.id))}
                title={`Remove N${n.id} from the cluster (membership change)`}
                aria-label={`Remove node ${n.id}`}
              >
                −
              </button>
            </div>
          );
        })}
      </div>

      <p className="mt-1.5 text-[11px] text-muted">
        {changing ? (
          <span className="animate-pulse text-accent">{changing} waiting for the configuration change to commit</span>
        ) : memberError ? (
          <span className="text-danger">{memberError}</span>
        ) : (
          <>
            {state.voters?.length ?? state.nodes.length} voters · majority = {Math.floor((state.voters?.length ?? state.nodes.length) / 2) + 1}
          </>
        )}
      </p>

      <h3 className="panel-title mb-2 mt-4">Network partition</h3>
      <div className="grid grid-cols-2 gap-1.5">
        <button className={`btn ${drawMode ? "btn-active" : ""}`} onClick={() => setDrawMode(!drawMode)} title="Drag a line across the cluster to cut it in two">
          ✂ {drawMode ? "Drawing…" : "Draw a cut"}
        </button>
        <button className="btn" onClick={isolateLeader} disabled={!leader}>
          Isolate leader
        </button>
        <button className="btn" onClick={splitMinority}>
          Split 2 | 3
        </button>
        <button className={`btn ${net.partitioned ? "border-leader/50 text-leader" : ""}`} onClick={onHeal} disabled={!net.partitioned}>
          Heal network
        </button>
      </div>
      {drawMode && <p className="mt-1.5 text-[11px] text-accent">Drag a line across the ring. Nodes on each side form a group.</p>}

      <h3 className="panel-title mb-1.5 mt-4">Network conditions</h3>
      <label className="flex items-center justify-between text-[11px] text-muted">
        <span>Latency (one way)</span>
        <span className="font-mono text-ink">
          {Math.max(1, Math.round(latency * 0.4))}–{Math.round(latency)} ms
        </span>
      </label>
      <input
        type="range"
        min={2}
        max={200}
        value={latency}
        className="w-full"
        onChange={(e) => {
          setDirty(true);
          setLatency(Number(e.target.value));
        }}
        onPointerUp={() => apply(latency, drop)}
        onKeyUp={() => apply(latency, drop)}
        aria-label="Network latency"
      />
      <label className="mt-1 flex items-center justify-between text-[11px] text-muted">
        <span>Packet loss</span>
        <span className="font-mono text-ink">{drop}%</span>
      </label>
      <input
        type="range"
        min={0}
        max={60}
        value={drop}
        className="w-full"
        onChange={(e) => {
          setDirty(true);
          setDrop(Number(e.target.value));
        }}
        onPointerUp={() => apply(latency, drop)}
        onKeyUp={() => apply(latency, drop)}
        aria-label="Packet loss"
      />
      {latency > 60 && (
        <p className="mt-1 text-[11px] text-candidate">
          Round trips now approach the 150–300 ms election timeout — expect spurious elections.
        </p>
      )}
    </div>
  );
}
