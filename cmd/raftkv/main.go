// Command raftkv runs a Raft cluster in one process and serves the HTTP API,
// the WebSocket event stream and (if built) the web visualizer.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/DakshTalla/raft-kv/api"
	"github.com/DakshTalla/raft-kv/cluster"
)

func main() {
	addr := flag.String("addr", envOr("ADDR", ":8080"), "HTTP listen address")
	data := flag.String("data", "data", "directory for persistent Raft state")
	nodes := flag.Int("nodes", 5, "number of Raft nodes")
	scale := flag.Float64("scale", 20, "slow-motion factor: 1 = real time, 20 = 20x slower (nice to watch)")
	snap := flag.Uint64("snapshot", 40, "snapshot after this many applied entries (0 = never)")
	fsync := flag.Bool("fsync", false, "fsync every disk write")
	fresh := flag.Bool("fresh", false, "delete existing data before starting")
	web := flag.String("web", "web/out", "directory with the built web UI")
	flag.Parse()

	if *fresh {
		if err := os.RemoveAll(*data); err != nil {
			log.Fatal(err)
		}
	}
	c, err := cluster.New(cluster.Options{
		N: *nodes, DataDir: *data, Fsync: *fsync, Scale: *scale, SnapshotThreshold: *snap,
		// Demo-friendly network: slow enough that messages are visible in flight.
		LatencyMin: 10 * time.Millisecond, LatencyMax: 25 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Addr: *addr, Handler: api.New(c, *web).Handler()}
	go func() {
		log.Printf("raft-kv: %d nodes, %gx slow motion, data in %q", *nodes, *scale, *data)
		log.Printf("raft-kv: open http://localhost%s", *addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	log.Print("shutting down")
	srv.Close()
	c.Close()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
