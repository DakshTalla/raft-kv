# Deploying Raft KV

The server is a single Go binary that also serves the web UI, so the simplest deployment is **one container on Fly.io**. Everything here uses free tiers. You need your own accounts; nothing is deployed for you.

## 0. Put the code on GitHub

```powershell
# Create an empty repo named raft-kv on github.com first (no README), then:
git remote add origin https://github.com/<your-username>/raft-kv.git
git push -u origin main
```

## Option A — Fly.io (recommended: UI + API together)

1. Install the CLI and log in:
   ```powershell
   winget install Fly-io.flyctl
   fly auth signup      # or: fly auth login
   ```
2. Open `fly.toml` and change `app = "raft-kv-demo"` to a unique name, e.g. `raft-kv-<yourname>`.
3. From the project folder:
   ```powershell
   fly launch --copy-config --no-deploy   # answer "no" to databases
   fly deploy
   ```
   Fly builds the `Dockerfile` on its own builders — you do not need Docker locally.
4. Open `https://<app-name>.fly.dev`.

Notes:
- The machine stops when idle and starts on the first request (a few seconds of cold start).
- The container starts with `-fresh`, so each boot is an empty cluster. That is intended for a demo.
- Everyone who opens the page shares the same cluster and chaos controls.

## Option B — UI on Vercel, API on Fly.io

Only worth it if you specifically want a `vercel.app` URL.

1. Deploy the API with Option A.
2. On vercel.com: *Add New → Project →* import the GitHub repo.
   - **Root Directory:** `web`
   - **Framework preset:** Next.js
   - **Environment variable:** `NEXT_PUBLIC_API_URL` = `https://<app-name>.fly.dev`
3. Deploy. The UI connects to the Fly.io API over HTTPS and WSS (CORS is already enabled on the server).

## Option C — Render.com

*New → Web Service →* connect the repo → **Runtime: Docker**. Leave everything else at the defaults; the server listens on port 8080 (set the `ADDR` environment variable to `:10000` if Render asks for its default port).

## Checking a deployment

```bash
curl https://<app-name>.fly.dev/healthz        # -> ok
curl -X PUT -d hello https://<app-name>.fly.dev/api/kv/greeting
```
