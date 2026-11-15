import type { NodeState } from "./types";

export type VisualRole = "leader" | "candidate" | "follower" | "dead";

export const ROLE_COLOR: Record<VisualRole, string> = {
  leader: "#34d399",
  candidate: "#fbbf24",
  follower: "#60a5fa",
  dead: "#4b5060",
};

export function roleOf(n: NodeState): VisualRole {
  return n.alive ? n.role : "dead";
}

// One colour per partition group.
export const GROUP_COLORS = ["#f472b6", "#22d3ee", "#facc15", "#a3e635", "#fb923c"];

// Log entries are tinted by the term they were created in, so a glance at the
// log panel shows which leader wrote what.
const TERM_COLORS = ["#60a5fa", "#34d399", "#fbbf24", "#f472b6", "#a78bfa", "#22d3ee", "#fb923c", "#a3e635"];

export function termColor(term: number): string {
  return TERM_COLORS[(term - 1 + TERM_COLORS.length * 1000) % TERM_COLORS.length];
}

export function fmtTime(virtualMs: number): string {
  return (virtualMs / 1000).toFixed(2) + "s";
}
