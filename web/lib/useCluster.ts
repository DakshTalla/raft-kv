"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { wsUrl } from "./api";
import type { ClusterEvent, ClusterState, Flight } from "./types";

const MAX_TIMELINE = 250;
const MAX_FLIGHTS = 400;

export interface ClusterFeed {
  connected: boolean;
  state: ClusterState | null;
  timeline: ClusterEvent[]; // newest first; RPC messages are not included
  /** In-flight messages. Mutated in place; read it inside requestAnimationFrame. */
  flights: React.RefObject<Map<number, Flight>>;
  /** Current virtual time (ms), extrapolated between server updates. */
  virtualNow: () => number;
  msgRate: number; // RPC messages per real second
  clearTimeline: () => void;
}

// useCluster owns the WebSocket connection to the Go server.
//
// WHY two kinds of storage: cluster state and the timeline change a few times
// per second, so they live in React state. RPC messages arrive hundreds of
// times per second at full speed, so they go into a mutable Map that the ring
// reads directly on every animation frame — no React re-render per message.
export function useCluster(): ClusterFeed {
  const [connected, setConnected] = useState(false);
  const [state, setState] = useState<ClusterState | null>(null);
  const [timeline, setTimeline] = useState<ClusterEvent[]>([]);
  const [msgRate, setMsgRate] = useState(0);

  const flights = useRef<Map<number, Flight>>(new Map());
  // Clock sync: the last (virtual time, real time) pair we got from the server.
  const clock = useRef({ now: 0, at: 0, scale: 1, paused: false });
  const msgCount = useRef(0);
  const leader = useRef(0);
  const lastKind = useRef<Map<number, string>>(new Map()); // last timeline event per node

  const virtualNow = useCallback(() => {
    const c = clock.current;
    if (c.paused) return c.now;
    return c.now + (performance.now() - c.at) / c.scale;
  }, []);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let closed = false;
    let retry: ReturnType<typeof setTimeout>;

    const handle = (batch: ClusterEvent[]) => {
      const fresh: ClusterEvent[] = [];
      let latest: ClusterState | null = null;
      const map = flights.current;
      const real = performance.now();

      for (const e of batch) {
        if (e.kind === "state" && e.state) {
          latest = e.state;
          continue;
        }
        const m = e.msg;
        if (!m) {
          if (e.kind === "role" && e.role === "leader") leader.current = e.node ?? 0;
          // Every node reports its own commit index; only the leader's is
          // news — the followers' would repeat it four more times.
          if (e.kind === "commit" && e.node !== leader.current) continue;
          // An isolated node retries its pre-vote on every election timeout;
          // show the first attempt only, until something else happens to it.
          if (e.node) {
            if (e.kind === "prevote" && lastKind.current.get(e.node) === "prevote") continue;
            lastKind.current.set(e.node, e.kind);
          }
          fresh.push(e);
          continue;
        }
        if (e.kind === "msg_send") {
          msgCount.current++;
          if (map.size < MAX_FLIGHTS) map.set(m.id, { ...m, status: "flying", startedAt: real });
        } else if (e.kind === "msg_recv") {
          const f = map.get(m.id);
          if (f) {
            f.status = "arrived";
            f.endedAt = real;
          }
        } else if (e.kind === "msg_drop") {
          const f = map.get(m.id);
          if (f) {
            // Lost in flight (node died or a partition appeared mid-way).
            const span = Math.max(f.deliverAt - f.sentAt, 0.001);
            f.frozenAt = Math.min(1, Math.max(0, (virtualNow() - f.sentAt) / span));
            f.status = "dropped";
            f.endedAt = real;
            f.reason = m.reason;
          } else if (map.size < MAX_FLIGHTS) {
            // Never left: blocked by a partition or dropped by packet loss.
            map.set(m.id, { ...m, status: "blocked", startedAt: real });
          }
        }
      }

      if (latest) {
        clock.current = { now: latest.now, at: real, scale: latest.scale, paused: latest.paused };
        if (latest.leader) leader.current = latest.leader;
        setState(latest);
      }
      if (fresh.length) {
        setTimeline((prev) => [...fresh.reverse(), ...prev].slice(0, MAX_TIMELINE));
      }
    };

    const connect = () => {
      ws = new WebSocket(wsUrl());
      ws.onopen = () => {
        flights.current.clear();
        setTimeline([]);
        setConnected(true);
      };
      ws.onmessage = (ev) => {
        try {
          handle(JSON.parse(ev.data));
        } catch {
          /* ignore malformed frame */
        }
      };
      ws.onclose = () => {
        setConnected(false);
        if (!closed) retry = setTimeout(connect, 1000);
      };
      ws.onerror = () => ws?.close();
    };
    connect();

    const rate = setInterval(() => {
      setMsgRate(msgCount.current);
      msgCount.current = 0;
    }, 1000);

    return () => {
      closed = true;
      clearTimeout(retry);
      clearInterval(rate);
      ws?.close();
    };
  }, [virtualNow]);

  const clearTimeline = useCallback(() => setTimeline([]), []);

  return { connected, state, timeline, flights, virtualNow, msgRate, clearTimeline };
}
