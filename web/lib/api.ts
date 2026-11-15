import type { ClientResult } from "./types";

// Where the Go server lives. In production the Go binary serves this UI, so
// the API is same-origin. Under `next dev` (port 3000) it is on :8080.
// NEXT_PUBLIC_API_URL overrides both (e.g. UI on Vercel, API on Fly.io).
export function apiBase(): string {
  if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL.replace(/\/$/, "");
  if (typeof window !== "undefined" && window.location.port === "3000") {
    return `${window.location.protocol}//${window.location.hostname}:8080`;
  }
  return "";
}

export function wsUrl(): string {
  const base = apiBase() || window.location.origin;
  return base.replace(/^http/, "ws") + "/ws";
}

async function post(path: string, body?: unknown): Promise<void> {
  await fetch(apiBase() + path, {
    method: "POST",
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
}

async function membership(method: string, path: string): Promise<string | null> {
  try {
    const res = await fetch(apiBase() + path, { method });
    const body = await res.json().catch(() => ({}));
    return res.ok ? null : (body.error ?? `HTTP ${res.status}`);
  } catch (e) {
    return String(e);
  }
}

export const chaos = {
  kill: (id: number) => post(`/api/nodes/${id}/kill`),
  restart: (id: number) => post(`/api/nodes/${id}/restart`),
  // Membership changes wait until the change is committed, so they report errors.
  addNode: () => membership("POST", "/api/nodes"),
  removeNode: (id: number) => membership("DELETE", `/api/nodes/${id}`),
  partition: (groups: number[][]) => post("/api/network/partition", { groups }),
  heal: () => post("/api/network/heal"),
  conditions: (latencyMinMs: number, latencyMaxMs: number, dropRate: number) =>
    post("/api/network/conditions", { latencyMinMs, latencyMaxMs, dropRate }),
  speed: (scale: number) => post("/api/sim/speed", { scale }),
  pause: () => post("/api/sim/pause"),
  resume: () => post("/api/sim/resume"),
};

let nextResultId = 1;

// kvRequest sends one client operation. `node` = 0 lets the server route to
// the leader; otherwise the request goes to that node, which answers with a
// 307 redirect if it is a follower. fetch() follows the redirect for us and
// `res.redirected` tells us it happened.
export async function kvRequest(
  op: "put" | "get" | "delete",
  key: string,
  value: string,
  node: number,
): Promise<ClientResult> {
  const id = nextResultId++;
  const url = `${apiBase()}/api/kv/${encodeURIComponent(key)}${node ? `?node=${node}` : ""}`;
  const started = performance.now();
  try {
    const res = await fetch(url, {
      method: op === "put" ? "PUT" : op === "get" ? "GET" : "DELETE",
      body: op === "put" ? value : undefined,
    });
    const body = await res.json().catch(() => ({}));
    return {
      id,
      op,
      key,
      value: body.value ?? (op === "put" ? value : undefined),
      ok: res.ok && body.ok,
      found: body.found,
      node: body.node,
      index: body.index,
      latencyMs: performance.now() - started,
      redirectedFrom: res.redirected && node ? node : undefined,
      error: body.error,
      status: res.status,
    };
  } catch (e) {
    return { id, op, key, ok: false, error: String(e), status: 0 };
  }
}
