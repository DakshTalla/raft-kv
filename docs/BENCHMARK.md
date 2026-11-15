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
| 1 | 207 | 4.8 ms | 5.9 ms | 6.4 ms |
| 8 | 1329 | 5.8 ms | 6.9 ms | 12.5 ms |
| 32 | 5259 | 5.9 ms | 6.8 ms | 16.2 ms |
| 128 | 14838 | 6.5 ms | 24.8 ms | 38.2 ms |

## Leader failover (15 trials: kill the leader, measure recovery)

| Metric | median | mean | max |
|---|---:|---:|---:|
| Leader crash → new leader elected | 170.3 ms | 178.7 ms | 261.4 ms |
| Leader crash → first successful client write | 178.3 ms | 187.8 ms | 270.8 ms |

Failover is dominated by the election timeout: followers wait 150–300 ms without a heartbeat before starting an election.
