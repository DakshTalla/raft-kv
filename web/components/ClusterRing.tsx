"use client";

import { motion } from "framer-motion";
import { useEffect, useRef, useState } from "react";
import type { ClusterState, Flight, NodeState } from "@/lib/types";
import { GROUP_COLORS, ROLE_COLOR, roleOf } from "@/lib/theme";

const W = 760;
const H = 600;
const CX = W / 2;
const CY = H / 2 + 6;
const RING_R = 208;
const NODE_R = 44;

export interface Cut {
  a: { x: number; y: number };
  b: { x: number; y: number };
}

interface Props {
  state: ClusterState;
  flights: React.RefObject<Map<number, Flight>>;
  virtualNow: () => number;
  selected: number | null;
  onSelect: (id: number | null) => void;
  drawMode: boolean;
  cut: Cut | null;
  onCut: (cut: Cut, groups: number[][]) => void;
}

function nodePos(i: number, n: number) {
  const a = (-90 + (360 / n) * i) * (Math.PI / 180);
  return { x: CX + RING_R * Math.cos(a), y: CY + RING_R * Math.sin(a) };
}

export default function ClusterRing({ state, flights, virtualNow, selected, onSelect, drawMode, cut, onCut }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [draft, setDraft] = useState<Cut | null>(null);
  const n = state.nodes.length;
  const pos = new Map(state.nodes.map((nd, i) => [nd.id, nodePos(i, n)]));

  // Screen pixels -> SVG coordinates (the SVG scales with its container).
  const toSvg = (e: React.PointerEvent) => {
    const svg = svgRef.current!;
    const pt = svg.createSVGPoint();
    pt.x = e.clientX;
    pt.y = e.clientY;
    const p = pt.matrixTransform(svg.getScreenCTM()!.inverse());
    return { x: p.x, y: p.y };
  };

  const finishCut = () => {
    if (!draft) return;
    const { a, b } = draft;
    setDraft(null);
    if (Math.hypot(b.x - a.x, b.y - a.y) < 40) return;
    // Which side of the drawn line is each node on? (sign of the 2D cross product)
    const left: number[] = [];
    const right: number[] = [];
    for (const nd of state.nodes) {
      const p = pos.get(nd.id)!;
      const cross = (b.x - a.x) * (p.y - a.y) - (b.y - a.y) * (p.x - a.x);
      (cross > 0 ? left : right).push(nd.id);
    }
    if (left.length && right.length) onCut(draft, [left, right]);
  };

  const blocked = (x: NodeState, y: NodeState) => !x.alive || !y.alive || x.group !== y.group;
  const shownCut = draft ?? (state.network.partitioned ? cut : null);

  return (
    <svg
      ref={svgRef}
      viewBox={`0 0 ${W} ${H}`}
      className="h-full w-full select-none"
      style={{ cursor: drawMode ? "crosshair" : "default", touchAction: drawMode ? "none" : "auto" }}
      onPointerDown={(e) => {
        if (!drawMode) return;
        (e.target as Element).setPointerCapture?.(e.pointerId);
        const p = toSvg(e);
        setDraft({ a: p, b: p });
      }}
      onPointerMove={(e) => draft && setDraft({ a: draft.a, b: toSvg(e) })}
      onPointerUp={finishCut}
      onClick={(e) => {
        if (!drawMode && e.target === svgRef.current) onSelect(null);
      }}
      role="img"
      aria-label="Raft cluster"
    >
      <defs>
        <radialGradient id="core-glow" cx="50%" cy="50%" r="50%">
          <stop offset="0%" stopColor="#a78bfa" stopOpacity="0.10" />
          <stop offset="100%" stopColor="#a78bfa" stopOpacity="0" />
        </radialGradient>
        <filter id="soft-glow" x="-80%" y="-80%" width="260%" height="260%">
          <feGaussianBlur stdDeviation="7" result="b" />
          <feMerge>
            <feMergeNode in="b" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      <circle cx={CX} cy={CY} r={RING_R + 40} fill="url(#core-glow)" pointerEvents="none" />
      <circle cx={CX} cy={CY} r={RING_R} fill="none" stroke="#1e222d" strokeDasharray="2 7" pointerEvents="none" />

      {/* Links between every pair of nodes */}
      <g pointerEvents="none">
        {state.nodes.flatMap((x, i) =>
          state.nodes.slice(i + 1).map((y) => {
            const p = pos.get(x.id)!;
            const q = pos.get(y.id)!;
            const cutOff = blocked(x, y);
            return (
              <line
                key={`${x.id}-${y.id}`}
                x1={p.x}
                y1={p.y}
                x2={q.x}
                y2={q.y}
                stroke={cutOff ? "#f87171" : "#2b3040"}
                strokeOpacity={cutOff ? 0.22 : 0.75}
                strokeWidth={1}
                strokeDasharray={cutOff ? "3 6" : undefined}
              />
            );
          }),
        )}
      </g>

      {/* Center readout */}
      <g pointerEvents="none" textAnchor="middle">
        <text x={CX} y={CY - 8} className="fill-faint" fontSize={11} letterSpacing="0.14em">
          {state.leader ? "LEADER" : "NO LEADER"}
        </text>
        <text
          x={CX}
          y={CY + 22}
          fontSize={30}
          fontWeight={600}
          fontFamily="var(--font-geist-mono)"
          fill={state.leader ? ROLE_COLOR.leader : ROLE_COLOR.candidate}
        >
          {state.leader ? `N${state.leader}` : "···"}
        </text>
        {state.paused && (
          <text x={CX} y={CY + 46} fontSize={11} letterSpacing="0.14em" className="fill-candidate">
            PAUSED
          </text>
        )}
      </g>

      <LiveLayer state={state} flights={flights} virtualNow={virtualNow} pos={pos} />

      {state.nodes.map((nd) => (
        <NodeView
          key={nd.id}
          node={nd}
          p={pos.get(nd.id)!}
          selected={selected === nd.id}
          quorum={Math.floor(n / 2) + 1}
          onClick={() => !drawMode && onSelect(selected === nd.id ? null : nd.id)}
        />
      ))}

      {shownCut && (
        <g pointerEvents="none">
          <line
            x1={shownCut.a.x}
            y1={shownCut.a.y}
            x2={shownCut.b.x}
            y2={shownCut.b.y}
            stroke="#f87171"
            strokeWidth={2.5}
            strokeDasharray="10 7"
            strokeLinecap="round"
            filter="url(#soft-glow)"
          />
          <text
            x={shownCut.a.x + (shownCut.b.x - shownCut.a.x) * 0.08 + 12}
            y={shownCut.a.y + (shownCut.b.y - shownCut.a.y) * 0.08}
            textAnchor="start"
            fontSize={10}
            letterSpacing="0.14em"
            fill="#fca5a5"
          >
            {draft ? "RELEASE TO CUT" : "NETWORK PARTITION"}
          </text>
        </g>
      )}
    </svg>
  );
}

// ---------------------------------------------------------------------------

function NodeView({
  node,
  p,
  selected,
  quorum,
  onClick,
}: {
  node: NodeState;
  p: { x: number; y: number };
  selected: boolean;
  quorum: number;
  onClick: () => void;
}) {
  const role = roleOf(node);
  const color = ROLE_COLOR[role];
  const granted = node.votes ? Object.values(node.votes).filter(Boolean).length : 0;
  return (
    <g
      transform={`translate(${p.x} ${p.y})`}
      onClick={(e) => {
        e.stopPropagation();
        onClick();
      }}
      style={{ cursor: "pointer" }}
      tabIndex={0}
      role="button"
      aria-label={`Node ${node.id}, ${role}, term ${node.term}`}
      onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && onClick()}
    >
      {node.group > 0 && node.alive && (
        <circle r={NODE_R + 17} fill="none" stroke={GROUP_COLORS[(node.group - 1) % GROUP_COLORS.length]} strokeOpacity={0.55} strokeDasharray="2 5" />
      )}
      {selected && <circle r={NODE_R + 23} fill="none" stroke="#a78bfa" strokeOpacity={0.9} strokeWidth={1.5} />}
      {role === "leader" && (
        <motion.circle
          r={NODE_R}
          fill="none"
          stroke={color}
          strokeWidth={2}
          initial={{ scale: 1, opacity: 0.6 }}
          animate={{ scale: 1.55, opacity: 0 }}
          transition={{ duration: 1.8, repeat: Infinity, ease: "easeOut" }}
        />
      )}
      <motion.circle
        r={NODE_R}
        fill="#0f121a"
        animate={{ stroke: color, opacity: role === "dead" ? 0.55 : 1 }}
        transition={{ duration: 0.35 }}
        strokeWidth={role === "leader" ? 3 : 2}
        strokeDasharray={role === "dead" ? "5 5" : undefined}
        filter={role === "leader" || role === "candidate" ? "url(#soft-glow)" : undefined}
      />
      <motion.circle r={NODE_R - 5} animate={{ fill: color }} fillOpacity={role === "dead" ? 0.04 : 0.11} transition={{ duration: 0.35 }} />

      {role === "leader" && (
        <path d="M-11 -25 l4 -9 l7 6 l7 -6 l4 9 z" fill={color} transform="translate(0 -4)" />
      )}
      <text textAnchor="middle" y={role === "leader" ? 5 : 1} fontSize={21} fontWeight={600} fontFamily="var(--font-geist-mono)" fill={role === "dead" ? "#6b7183" : "#f3f4f8"}>
        N{node.id}
      </text>
      <text textAnchor="middle" y={role === "leader" ? 21 : 18} fontSize={9} fontWeight={600} letterSpacing="0.12em" fill={color}>
        {role === "dead" ? "DOWN" : node.membership && node.membership !== "voter" ? node.membership.toUpperCase() : node.preVoting ? "PRE-VOTE" : role.toUpperCase()}
      </text>

      {/* Term badge */}
      <g transform={`translate(${NODE_R * 0.74} ${-NODE_R * 0.74})`}>
        <rect x={-15} y={-10} width={30} height={20} rx={10} fill="#141720" stroke="#2b3040" />
        <text textAnchor="middle" y={4} fontSize={10} fontFamily="var(--font-geist-mono)" fill="#c7cbd8">
          T{node.term}
        </text>
      </g>

      {/* Candidate: one pip per vote needed for a majority */}
      {role === "candidate" && (
        <g transform={`translate(${-(quorum - 1) * 6} ${NODE_R - 12})`}>
          {Array.from({ length: quorum }, (_, i) => (
            <circle key={i} cx={i * 12} r={3.5} fill={i < granted ? color : "none"} stroke={color} strokeWidth={1.2} />
          ))}
        </g>
      )}

      <text textAnchor="middle" y={NODE_R + 40} fontSize={10} fontFamily="var(--font-geist-mono)" fill="#8b91a3">
        log {node.lastIndex} · commit {node.commitIndex}
      </text>
    </g>
  );
}

// ---------------------------------------------------------------------------

interface Style {
  color: string;
  r: number;
  hollow?: boolean;
  ring?: boolean; // outlined in its own colour (pre-vote: only a question, not a vote)
  square?: boolean;
  label?: string;
}

function styleOf(f: Flight): Style {
  switch (f.type) {
    case "RequestVote":
      return { color: "#fbbf24", r: 6 };
    case "RequestVoteResp":
      return f.success ? { color: "#fbbf24", r: 4.5 } : { color: "#f87171", r: 4.5, hollow: true };
    case "AppendEntries":
      return f.heartbeat ? { color: "#34d399", r: 3.2 } : { color: "#34d399", r: 7, label: String(f.entries) };
    case "AppendEntriesResp":
      return f.success ? { color: "#7dd3fc", r: 3.2 } : { color: "#f87171", r: 4.5, hollow: true };
    case "PreVote":
      return { color: "#fde68a", r: 4.5, ring: true };
    case "PreVoteResp":
      return f.success ? { color: "#fde68a", r: 3.2 } : { color: "#f87171", r: 4.5, hollow: true };
    case "InstallSnapshot":
      return { color: "#e879f9", r: 7, square: true };
    default:
      return { color: "#e879f9", r: 3.5 };
  }
}

// LiveLayer draws everything that moves every frame: the election timers
// around each node and the RPC messages in flight. It re-renders itself with
// requestAnimationFrame; the rest of the ring only re-renders when the
// cluster state changes.
function LiveLayer({
  state,
  flights,
  virtualNow,
  pos,
}: {
  state: ClusterState;
  flights: React.RefObject<Map<number, Flight>>;
  virtualNow: () => number;
  pos: Map<number, { x: number; y: number }>;
}) {
  const [, setFrame] = useState(0);
  useEffect(() => {
    let raf = 0;
    const loop = () => {
      setFrame((f) => (f + 1) % 1_000_000);
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, []);

  const vnow = virtualNow();
  const real = performance.now();
  const sinceState = Math.max(0, vnow - state.now) / state.tickMs; // ticks since the last update

  const dots: React.ReactNode[] = [];
  const map = flights.current;
  for (const [id, f] of map) {
    const a = pos.get(f.from);
    const b = pos.get(f.to);
    if (!a || !b) {
      map.delete(id);
      continue;
    }
    const dx = b.x - a.x;
    const dy = b.y - a.y;
    const len = Math.hypot(dx, dy);
    const ux = dx / len;
    const uy = dy / len;
    // Requests and responses use parallel lanes so they do not overlap.
    const ox = -uy * 7;
    const oy = ux * 7;
    const start = NODE_R + 6;
    const travel = len - 2 * start;

    let progress = 0;
    let opacity = 1;
    let cross = false;
    let ripple = 0;

    if (f.status === "flying") {
      const span = Math.max(f.deliverAt - f.sentAt, 0.001);
      progress = Math.min(1, Math.max(0, (vnow - f.sentAt) / span));
      if (vnow > f.deliverAt + 1500) {
        map.delete(id); // we never saw the delivery event
        continue;
      }
    } else if (f.status === "arrived") {
      const age = real - (f.endedAt ?? real);
      if (age > 260) {
        map.delete(id);
        continue;
      }
      progress = 1;
      ripple = age / 260;
      opacity = 1 - ripple;
    } else if (f.status === "dropped") {
      const age = real - (f.endedAt ?? real);
      if (age > 700) {
        map.delete(id);
        continue;
      }
      progress = f.frozenAt ?? 0.5;
      opacity = 1 - age / 700;
      cross = true;
    } else {
      // blocked: leaves the sender, hits the wall, fades.
      const age = real - f.startedAt;
      if (age > 750) {
        map.delete(id);
        continue;
      }
      progress = Math.min(0.26, (age / 260) * 0.26);
      opacity = age < 260 ? 1 : 1 - (age - 260) / 490;
      cross = age >= 200;
    }

    const x = a.x + ux * (start + travel * progress) + ox;
    const y = a.y + uy * (start + travel * progress) + oy;
    const s = styleOf(f);
    dots.push(
      <g key={id} transform={`translate(${x} ${y})`} opacity={opacity}>
        {ripple > 0 && <circle r={s.r + ripple * 12} fill="none" stroke={s.color} strokeOpacity={0.5} />}
        {s.square ? (
          <rect x={-s.r} y={-s.r} width={s.r * 2} height={s.r * 2} rx={2} fill={s.color} />
        ) : (
          <circle r={s.r} fill={s.hollow || s.ring ? "#07080c" : s.color} stroke={s.color} strokeWidth={s.hollow || s.ring ? 1.6 : 0} />
        )}
        {s.label && (
          <text textAnchor="middle" y={3} fontSize={8} fontWeight={700} fill="#052e1f">
            {s.label}
          </text>
        )}
        {cross && <path d="M-5 -5 L5 5 M5 -5 L-5 5" stroke="#f87171" strokeWidth={2} strokeLinecap="round" />}
      </g>,
    );
  }

  return (
    <g pointerEvents="none">
      {state.nodes.map((nd) => {
        if (!nd.alive) return null;
        const p = pos.get(nd.id)!;
        const r = NODE_R + 8;
        const circ = 2 * Math.PI * r;
        const live = state.paused ? 0 : sinceState;
        if (nd.role === "leader") {
          // Leader: a short arc that fills up until the next heartbeat goes out.
          const frac = Math.min(1, (nd.heartbeatElapsed + live) / nd.heartbeatTicks) % 1;
          return (
            <circle key={nd.id} cx={p.x} cy={p.y} r={r} fill="none" stroke="#34d399" strokeOpacity={0.5} strokeWidth={2}
              strokeDasharray={`${circ * frac} ${circ}`} transform={`rotate(-90 ${p.x} ${p.y})`} strokeLinecap="round" />
          );
        }
        // Follower / candidate: the ring drains; when it is empty the node
        // starts an election. A heartbeat from the leader refills it.
        const left = Math.max(0, 1 - (nd.electionElapsed + live) / nd.electionTimeout);
        const urgent = left < 0.3;
        return (
          <g key={nd.id}>
            <circle cx={p.x} cy={p.y} r={r} fill="none" stroke="#1e222d" strokeWidth={3} />
            <circle cx={p.x} cy={p.y} r={r} fill="none" stroke={urgent ? "#fbbf24" : nd.role === "candidate" ? "#fbbf24" : "#60a5fa"}
              strokeOpacity={0.9} strokeWidth={3} strokeLinecap="round"
              strokeDasharray={`${circ * left} ${circ}`} transform={`rotate(-90 ${p.x} ${p.y})`} />
          </g>
        );
      })}
      {dots}
    </g>
  );
}
