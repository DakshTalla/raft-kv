<#
.SYNOPSIS
  One-command start for Raft KV: builds the web UI (first run only), builds the
  Go server and runs a 5-node cluster with the visualizer on http://localhost:8080

.EXAMPLE
  ./start.ps1                 # start (keeps existing data)
  ./start.ps1 -Fresh          # wipe the data directory first
  ./start.ps1 -Scale 1        # real-time instead of 20x slow motion
  ./start.ps1 -RebuildWeb     # force a rebuild of the frontend
#>
param(
  [switch]$Fresh,
  [switch]$RebuildWeb,
  [double]$Scale = 20,
  [int]$Port = 8080
)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# A freshly installed Go may not be on this shell's PATH yet.
$env:Path = [Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [Environment]::GetEnvironmentVariable("Path", "User")

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
  Write-Host "Go is not installed. Run:  winget install GoLang.Go" -ForegroundColor Red
  exit 1
}
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
  Write-Host "Node.js is not installed. Run:  winget install OpenJS.NodeJS.LTS" -ForegroundColor Red
  exit 1
}

if ($RebuildWeb -or -not (Test-Path "web/out/index.html")) {
  Write-Host "==> Building the web UI (first run takes a minute)..." -ForegroundColor Cyan
  Push-Location web
  if (-not (Test-Path "node_modules")) { npm install --no-fund --no-audit }
  npm run build
  Pop-Location
  if ($LASTEXITCODE -ne 0) { Write-Host "Web build failed." -ForegroundColor Red; exit 1 }
}

# An earlier instance would keep the port, the data files and the .exe locked.
$old = Get-Process raftkv -ErrorAction SilentlyContinue
if ($old) {
  Write-Host "==> Stopping the raft-kv server that is already running..." -ForegroundColor Yellow
  $old | Stop-Process -Force
  Start-Sleep -Milliseconds 500
}

Write-Host "==> Building the server..." -ForegroundColor Cyan
go build -o bin/raftkv.exe ./cmd/raftkv
if ($LASTEXITCODE -ne 0) { Write-Host "Go build failed." -ForegroundColor Red; exit 1 }

$serverArgs = @("-addr", ":$Port", "-scale", "$Scale")
if ($Fresh) { $serverArgs += "-fresh" }

Write-Host "==> Raft KV is starting on http://localhost:$Port  (Ctrl+C to stop)" -ForegroundColor Green
Start-Job -ScriptBlock { param($p) Start-Sleep -Milliseconds 1200; Start-Process "http://localhost:$p" } -ArgumentList $Port | Out-Null
& ./bin/raftkv.exe @serverArgs
