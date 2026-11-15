import type { NextConfig } from "next";

// WHY a static export: the UI is a pure client-side app that talks to the Go
// server over HTTP + WebSocket. Exporting it to plain files (web/out) lets the
// Go binary serve it, so the whole project runs from one process on one port.
const nextConfig: NextConfig = {
  output: "export",
  images: { unoptimized: true },
};

export default nextConfig;
