// Command raftnode runs ONE Raft node as its own OS process, talking to its
// peers over TCP. Start one per node (see start-cluster.ps1):
//
//	raftnode -id 1 -raft 1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003 \
//	         -api 1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003
//
// Each node serves the key-value API on its own -api address:
//
//	PUT/GET/DELETE /kv/{key}   307-redirects to the leader's address if this node is a follower
//	GET /status                this node's Raft state
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/DakshTalla/raft-kv/kv"
	"github.com/DakshTalla/raft-kv/server"
)

func main() {
	id := flag.Int("id", 0, "this node's id")
	raftFlag := flag.String("raft", "", "raft addresses of all nodes: 1=host:port,2=host:port,...")
	apiFlag := flag.String("api", "", "client API addresses of all nodes: 1=host:port,2=host:port,...")
	data := flag.String("data", "", "data directory (default data-real/node-<id>)")
	fsync := flag.Bool("fsync", false, "fsync every disk write")
	snap := flag.Uint64("snapshot", 1000, "snapshot after this many applied entries")
	flag.Parse()

	raftAddrs, err1 := parseAddrs(*raftFlag)
	apiAddrs, err2 := parseAddrs(*apiFlag)
	if err := errors.Join(err1, err2); err != nil || raftAddrs[*id] == "" || apiAddrs[*id] == "" {
		log.Fatalf("usage: raftnode -id N -raft 1=host:port,... -api 1=host:port,...  (%v)", err)
	}
	if *data == "" {
		*data = fmt.Sprintf("data-real/node-%d", *id)
	}
	srv, err := server.Start(server.Config{ID: *id, Addrs: raftAddrs, DataDir: *data, Fsync: *fsync, SnapshotThreshold: *snap})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, srv.Status())
	})
	handleKV := func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		op, value := kv.OpGet, ""
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(io.LimitReader(r.Body, 64*1024))
			op, value = kv.OpPut, string(body)
		case http.MethodDelete:
			op = kv.OpDelete
		}
		// Optional: a client that wants exactly-once retries sends its own
		// session id and sequence number and reuses them when it retries.
		client, _ := strconv.ParseUint(r.URL.Query().Get("client"), 10, 64)
		seq, _ := strconv.ParseUint(r.URL.Query().Get("seq"), 10, 64)
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		res, err := srv.Do(ctx, op, key, value, client, seq)
		var nl *server.NotLeaderError
		switch {
		case err == nil:
			writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key": key, "value": res.Value, "found": res.Found, "index": res.Index, "term": res.Term, "node": *id})
		case errors.As(err, &nl) && nl.Leader != 0:
			w.Header().Set("Location", "http://"+apiAddrs[nl.Leader]+r.URL.RequestURI())
			writeJSON(w, http.StatusTemporaryRedirect, map[string]any{"ok": false, "error": err.Error(), "leader": nl.Leader})
		case errors.As(err, &nl):
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": err.Error()})
		default:
			writeJSON(w, http.StatusGatewayTimeout, map[string]any{"ok": false, "error": err.Error()})
		}
	}
	mux.HandleFunc("GET /kv/{key}", handleKV)
	mux.HandleFunc("PUT /kv/{key}", handleKV)
	mux.HandleFunc("DELETE /kv/{key}", handleKV)

	httpSrv := &http.Server{Addr: apiAddrs[*id], Handler: mux}
	go func() {
		log.Printf("node %d: raft on %s, API on http://%s, data in %s", *id, raftAddrs[*id], apiAddrs[*id], *data)
		if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	httpSrv.Close()
	srv.Close()
}

func parseAddrs(s string) (map[int]string, error) {
	out := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		id, err := strconv.Atoi(k)
		if !ok || err != nil || v == "" {
			return nil, fmt.Errorf("bad address list %q", s)
		}
		out[id] = v
	}
	return out, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
