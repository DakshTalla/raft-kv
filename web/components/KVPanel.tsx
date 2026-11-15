"use client";

import { AnimatePresence, motion } from "framer-motion";
import { useState } from "react";
import { kvRequest } from "@/lib/api";
import type { ClientResult, ClusterState } from "@/lib/types";

type Op = "put" | "get" | "delete";

// KVPanel is a client for the store. Sending to "Leader (auto)" behaves like
// a smart client; picking a specific node shows what happens when a client
// talks to a follower: it gets redirected (HTTP 307) to the leader.
export default function KVPanel({ state }: { state: ClusterState }) {
  const [op, setOp] = useState<Op>("put");
  const [key, setKey] = useState("user:1");
  const [value, setValue] = useState("alice");
  const [node, setNode] = useState(0);
  const [busy, setBusy] = useState(0);
  const [results, setResults] = useState<ClientResult[]>([]);

  const push = (r: ClientResult) => setResults((prev) => [r, ...prev].slice(0, 30));

  const send = async () => {
    if (!key.trim()) return;
    setBusy((b) => b + 1);
    push(await kvRequest(op, key.trim(), value, node));
    setBusy((b) => b - 1);
  };

  // Fire 20 concurrent writes — a quick way to watch batching and commits.
  const burst = async () => {
    setBusy((b) => b + 1);
    const base = Math.floor(Math.random() * 900) + 100;
    const all = await Promise.all(
      Array.from({ length: 20 }, (_, i) => kvRequest("put", `k${(base + i) % 12}`, `v${base + i}`, 0)),
    );
    const ok = all.filter((r) => r.ok);
    const avg = ok.reduce((s, r) => s + (r.latencyMs ?? 0), 0) / Math.max(ok.length, 1);
    push({ id: all[0].id, op: "put", key: `burst ×20`, value: `${ok.length} ok`, ok: ok.length === 20, node: ok[0]?.node, latencyMs: avg, status: 200, error: ok.length < 20 ? `${20 - ok.length} failed` : undefined });
    setBusy((b) => b - 1);
  };

  return (
    <div className="panel flex min-h-0 flex-1 flex-col p-3">
      <h2 className="panel-title mb-2.5">Client</h2>

      <div className="mb-2 grid grid-cols-3 gap-1 rounded-lg border border-line bg-bg p-1">
        {(["put", "get", "delete"] as Op[]).map((o) => (
          <button
            key={o}
            onClick={() => setOp(o)}
            className={`h-6 rounded-md text-[11px] font-semibold uppercase tracking-wider transition-colors ${op === o ? "bg-accent/20 text-[#d6c9ff]" : "text-muted hover:text-ink"}`}
          >
            {o === "delete" ? "del" : o}
          </button>
        ))}
      </div>

      <form
        className="flex flex-col gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          send();
        }}
      >
        <div className="flex gap-2">
          <input className="field" placeholder="key" value={key} onChange={(e) => setKey(e.target.value)} aria-label="Key" />
          {op === "put" && <input className="field" placeholder="value" value={value} onChange={(e) => setValue(e.target.value)} aria-label="Value" />}
        </div>
        <div className="flex gap-2">
          <select className="field" value={node} onChange={(e) => setNode(Number(e.target.value))} aria-label="Send to">
            <option value={0}>Send to: leader (auto)</option>
            {state.nodes.map((n) => (
              <option key={n.id} value={n.id}>
                Send to: N{n.id} ({n.alive ? n.role : "down"})
              </option>
            ))}
          </select>
          <button type="submit" className="btn btn-primary shrink-0">
            Send
          </button>
          <button type="button" className="btn shrink-0" onClick={burst} title="20 concurrent PUTs">
            Burst
          </button>
        </div>
      </form>

      <div className="mt-3 flex items-center justify-between">
        <h3 className="panel-title">Responses</h3>
        {busy > 0 && <span className="animate-pulse text-[11px] text-candidate">waiting for commit…</span>}
      </div>
      <div className="mt-1.5 flex min-h-[72px] flex-1 flex-col gap-1 overflow-y-auto pr-1">
        {results.length === 0 && <p className="text-xs text-faint">Send a request. A write returns only after a majority of nodes has stored it.</p>}
        <AnimatePresence initial={false}>
          {results.map((r) => (
            <motion.div
              key={r.id}
              layout
              initial={{ opacity: 0, y: -6 }}
              animate={{ opacity: 1, y: 0 }}
              className={`rounded-md border px-2 py-1.5 font-mono text-[11px] ${r.ok ? "border-line bg-white/[0.02]" : "border-danger/30 bg-danger/[0.06]"}`}
            >
              <div className="flex items-baseline justify-between gap-2">
                <span className="truncate">
                  <span className={r.ok ? "text-leader" : "text-danger"}>{r.op.toUpperCase()}</span> {r.key}
                  {r.ok && r.op === "get" && (r.found ? <span className="text-ink"> → “{r.value}”</span> : <span className="text-faint"> → not found</span>)}
                  {r.ok && r.op === "put" && r.value !== undefined && <span className="text-muted"> = {r.value}</span>}
                </span>
                {r.latencyMs !== undefined && <span className="shrink-0 text-faint">{r.latencyMs.toFixed(0)} ms</span>}
              </div>
              <div className="mt-0.5 text-[10px] text-muted">
                {r.redirectedFrom && r.node && r.redirectedFrom !== r.node && (
                  <span className="text-candidate">N{r.redirectedFrom} ↪ 307 redirect → </span>
                )}
                {r.ok ? (
                  <>
                    served by N{r.node}
                    {r.index ? ` · log index ${r.index}` : ""}
                  </>
                ) : (
                  <span className="text-danger">
                    {r.status || "network"}: {r.error ?? "failed"}
                  </span>
                )}
              </div>
            </motion.div>
          ))}
        </AnimatePresence>
      </div>

      <div className="mt-3 flex items-center justify-between">
        <h3 className="panel-title">Committed data</h3>
        <span className="font-mono text-[11px] text-faint">{state.kvTotal} keys</span>
      </div>
      <div className="mt-1.5 max-h-36 overflow-y-auto rounded-lg border border-line bg-bg">
        {(state.kv ?? []).length === 0 ? (
          <p className="px-2.5 py-2 text-xs text-faint">Empty store.</p>
        ) : (
          <table className="w-full font-mono text-[11px]">
            <tbody>
              {(state.kv ?? []).map((p) => (
                <tr key={p.key} className="border-b border-line/60 last:border-0">
                  <td className="max-w-[110px] truncate px-2.5 py-1 text-follower">{p.key}</td>
                  <td className="max-w-[140px] truncate px-2.5 py-1 text-ink">{p.value}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
