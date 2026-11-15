<#
.SYNOPSIS
  Starts a REAL 5-node cluster: five separate OS processes that talk Raft over
  TCP on localhost (no simulator, no visualizer). Each node gets its own
  console window; close a window (or Ctrl+C in it) to crash that node.

  Raft ports: 9001-9005     Client API: http://127.0.0.1:8001 ... 8005

.EXAMPLE
  ./start-cluster.ps1            # start (keeps data in data-real/)
  ./start-cluster.ps1 -Fresh     # wipe data-real/ first
  ./start-cluster.ps1 -Stop      # stop every node

  curl.exe -L -X PUT -d hello http://127.0.0.1:8001/kv/greeting   # -L follows the 307 to the leader
  curl.exe -L http://127.0.0.1:8003/kv/greeting
  curl.exe http://127.0.0.1:8002/status
#>
param([switch]$Fresh, [switch]$Stop, [int]$Nodes = 5)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$env:Path = [Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [Environment]::GetEnvironmentVariable("Path", "User")

Get-Process raftnode -ErrorAction SilentlyContinue | Stop-Process -Force
if ($Stop) { Write-Host "All nodes stopped."; exit 0 }

Write-Host "==> Building raftnode..." -ForegroundColor Cyan
go build -o bin/raftnode.exe ./cmd/raftnode
if ($LASTEXITCODE -ne 0) { Write-Host "Go build failed." -ForegroundColor Red; exit 1 }
if ($Fresh -and (Test-Path "data-real")) { Remove-Item -Recurse -Force "data-real" }

$raft = (1..$Nodes | ForEach-Object { "$_=127.0.0.1:$(9000 + $_)" }) -join ","
$api = (1..$Nodes | ForEach-Object { "$_=127.0.0.1:$(8000 + $_)" }) -join ","
foreach ($id in 1..$Nodes) {
  Start-Process -FilePath "$PSScriptRoot/bin/raftnode.exe" -WorkingDirectory $PSScriptRoot `
    -ArgumentList @("-id", "$id", "-raft", $raft, "-api", $api)
}

Start-Sleep -Seconds 2
Write-Host "==> $Nodes nodes running as separate processes." -ForegroundColor Green
foreach ($id in 1..$Nodes) {
  try {
    $s = Invoke-RestMethod "http://127.0.0.1:$(8000 + $id)/status" -TimeoutSec 2
    Write-Host ("    node {0}: {1,-9} term {2}   http://127.0.0.1:{3}" -f $id, $s.role, $s.term, (8000 + $id))
  } catch { Write-Host "    node ${id}: not responding" -ForegroundColor Yellow }
}
Write-Host ""
Write-Host "Try:  curl.exe -L -X PUT -d hello http://127.0.0.1:8001/kv/greeting"
Write-Host "      curl.exe -L http://127.0.0.1:8003/kv/greeting"
Write-Host "Close a node's window to crash it, then read again from another node."
Write-Host "Stop everything:  ./start-cluster.ps1 -Stop"
