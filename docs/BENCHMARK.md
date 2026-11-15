# Benchmark results

- Date: 2026-11-15
- Machine: windows/amd64, 14 logical CPUs, go1.27.4
- Cluster: 5 nodes in one process, real-time clock, election timeout 150–300 ms, heartbeat 50 ms
- Simulated network: 1–3 ms one-way latency per message, no loss
- Persistence: term/vote/log written to disk before replying (fsync=false)
- Every operation is a linearizable PUT that goes through the Raft log and is acknowledged after a majority commit

## Throughput and latency (PUT, 5s per run)

| Concurrent clients | Throughput (ops/s) | p50 latency | p95 latency | p99 latency |
|---:|---:|---:|---:|---:|
| 1 | 206 | 4.9 ms | 5.9 ms | 6.4 ms |
| 8 | 1369 | 5.8 ms | 6.6 ms | 8.2 ms |
| 32 | 5262 | 5.9 ms | 6.8 ms | 15.2 ms |
| 128 | 16539 | 6.3 ms | 21.2 ms | 30.9 ms |

## Leader failover (15 trials: kill the leader, measure recovery)

| Metric | median | mean | max |
|---|---:|---:|---:|
| Leader crash → new leader elected | 169.8 ms | 175.5 ms | 239.2 ms |
| Leader crash → first successful client write | 179.9 ms | 184.8 ms | 249.2 ms |

Failover is dominated by the election timeout: followers wait 150–300 ms without a heartbeat before starting an election.
