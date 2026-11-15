// Command bench measures throughput, latency and failover time of a 5-node
// in-process cluster running in real time (scale 1) and prints a Markdown
// report. Run: go run ./cmd/bench -out docs/BENCHMARK.md
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DakshTalla/raft-kv/cluster"
)

func main() {
	dur := flag.Duration("duration", 5*time.Second, "duration of each load run")
	trials := flag.Int("trials", 15, "number of leader-failover trials")
	out := flag.String("out", "", "also write the report to this file")
	fsync := flag.Bool("fsync", false, "fsync every write")
	flag.Parse()

	dir, _ := os.MkdirTemp("", "raftkv-bench-")
	defer os.RemoveAll(dir)

	var b strings.Builder
	fmt.Fprintf(&b, "# Benchmark results\n\n")
	fmt.Fprintf(&b, "- Date: %s\n- Machine: %s/%s, %d logical CPUs, %s\n", time.Now().Format("2006-01-02"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Fprintf(&b, "- Cluster: 5 nodes in one process, real-time clock, election timeout 150–300 ms, heartbeat 50 ms\n")
	fmt.Fprintf(&b, "- Simulated network: 1–3 ms one-way latency per message, no loss\n")
	fmt.Fprintf(&b, "- Persistence: term/vote/log written to disk before replying (fsync=%v)\n", *fsync)
	fmt.Fprintf(&b, "- Every operation is a linearizable PUT that goes through the Raft log and is acknowledged after a majority commit\n\n")

	// --- Throughput / latency -------------------------------------------------
	fmt.Fprintf(&b, "## Throughput and latency (PUT, %v per run)\n\n", *dur)
	fmt.Fprintf(&b, "| Concurrent clients | Throughput (ops/s) | p50 latency | p95 latency | p99 latency |\n|---:|---:|---:|---:|---:|\n")
	for i, clients := range []int{1, 8, 32, 128} {
		c := newCluster(fmt.Sprintf("%s/load-%d", dir, i), *fsync)
		ops, lat := load(c, clients, *dur)
		c.Close()
		row := fmt.Sprintf("| %d | %.0f | %s | %s | %s |\n", clients, float64(ops)/dur.Seconds(), pct(lat, 50), pct(lat, 95), pct(lat, 99))
		fmt.Print(row)
		b.WriteString(row)
	}

	// --- Failover -------------------------------------------------------------
	c := newCluster(dir+"/failover", *fsync)
	var elect, write []time.Duration
	for i := 0; i < *trials; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if _, err := c.Put(ctx, "warm", fmt.Sprint(i)); err != nil {
			panic(err)
		}
		old := c.Leader()
		start := time.Now()
		c.Kill(old)
		for l := c.Leader(); l == 0 || l == old; l = c.Leader() {
			time.Sleep(200 * time.Microsecond)
		}
		elect = append(elect, time.Since(start))
		if _, err := c.Put(ctx, "after", fmt.Sprint(i)); err != nil {
			panic(err)
		}
		write = append(write, time.Since(start))
		cancel()
		c.Restart(old)
		time.Sleep(400 * time.Millisecond) // let it rejoin before the next trial
	}
	c.Close()
	fmt.Fprintf(&b, "\n## Leader failover (%d trials: kill the leader, measure recovery)\n\n", *trials)
	fmt.Fprintf(&b, "| Metric | median | mean | max |\n|---|---:|---:|---:|\n")
	fmt.Fprintf(&b, "| Leader crash → new leader elected | %s | %s | %s |\n", pct(elect, 50), mean(elect), pct(elect, 100))
	fmt.Fprintf(&b, "| Leader crash → first successful client write | %s | %s | %s |\n", pct(write, 50), mean(write), pct(write, 100))
	fmt.Fprintf(&b, "\nFailover is dominated by the election timeout: followers wait 150–300 ms without a heartbeat before starting an election.\n")

	fmt.Println()
	fmt.Print(b.String())
	if *out != "" {
		os.MkdirAll(filepath.Dir(*out), 0o755)
		if err := os.WriteFile(*out, []byte(b.String()), 0o644); err != nil {
			panic(err)
		}
	}
}

func newCluster(dir string, fsync bool) *cluster.Cluster {
	c, err := cluster.New(cluster.Options{
		DataDir: dir, Fsync: fsync, Scale: 1, SnapshotThreshold: 2000, Sessions: 512,
		LatencyMin: time.Millisecond, LatencyMax: 3 * time.Millisecond,
	})
	if err != nil {
		panic(err)
	}
	if c.WaitLeader(5*time.Second) == 0 {
		panic("no leader")
	}
	return c
}

// load runs `clients` closed-loop writers for d and returns the number of
// successful operations and their latencies.
func load(c *cluster.Cluster, clients int, d time.Duration) (int64, []time.Duration) {
	var ops atomic.Int64
	var mu sync.Mutex
	var all []time.Duration
	var wg sync.WaitGroup
	deadline := time.Now().Add(d)
	for w := 0; w < clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var lat []time.Duration
			for i := 0; time.Now().Before(deadline); i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				start := time.Now()
				_, err := c.Put(ctx, fmt.Sprintf("key-%d-%d", w, i%100), "value-0123456789")
				cancel()
				if err == nil {
					lat = append(lat, time.Since(start))
					ops.Add(1)
				}
			}
			mu.Lock()
			all = append(all, lat...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	return ops.Load(), all
}

func pct(d []time.Duration, p int) string {
	if len(d) == 0 {
		return "n/a"
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := len(s) * p / 100
	if i >= len(s) {
		i = len(s) - 1
	}
	return fmtDur(s[i])
}

func mean(d []time.Duration) string {
	var sum time.Duration
	for _, x := range d {
		sum += x
	}
	return fmtDur(sum / time.Duration(len(d)))
}

func fmtDur(d time.Duration) string { return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000) }
