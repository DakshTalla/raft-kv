// Package api exposes the cluster over HTTP: a JSON key-value API with
// leader redirects, chaos controls, and a WebSocket stream of events.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/websocket"

	"github.com/DakshTalla/raft-kv/cluster"
	"github.com/DakshTalla/raft-kv/events"
	"github.com/DakshTalla/raft-kv/kv"
)

// Server is the HTTP front end of a cluster.
type Server struct {
	c      *cluster.Cluster
	webDir string
}

// New creates a server. webDir, if it exists, is served as the static UI.
func New(c *cluster.Cluster, webDir string) *Server { return &Server{c: c, webDir: webDir} }

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", s.state)
	mux.HandleFunc("GET /api/kv/{key}", s.kv)
	mux.HandleFunc("PUT /api/kv/{key}", s.kv)
	mux.HandleFunc("DELETE /api/kv/{key}", s.kv)
	mux.HandleFunc("POST /api/nodes/{id}/kill", s.nodeAction)
	mux.HandleFunc("POST /api/nodes/{id}/restart", s.nodeAction)
	mux.HandleFunc("POST /api/network/partition", s.partition)
	mux.HandleFunc("POST /api/network/heal", s.heal)
	mux.HandleFunc("POST /api/network/conditions", s.conditions)
	mux.HandleFunc("POST /api/sim/speed", s.speed)
	mux.HandleFunc("POST /api/sim/pause", s.pause)
	mux.HandleFunc("POST /api/sim/resume", s.pause)
	mux.HandleFunc("GET /ws", s.ws)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	if st, err := os.Stat(s.webDir); err == nil && st.IsDir() {
		mux.Handle("/", http.FileServer(http.Dir(s.webDir)))
	}
	return cors(mux)
}

// cors lets the Next.js dev server (another origin) call the API.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Expose-Headers", "X-Raft-Leader")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"ok": false, "error": msg})
}

func (s *Server) state(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.c.State())
}

// kv handles GET/PUT/DELETE /api/kv/{key}[?node=N].
//
// Without ?node the server acts as a smart client and routes to the leader.
// With ?node=N the request is addressed to that specific node, like a client
// that only knows one cluster member: a follower answers 307 Temporary
// Redirect pointing at the leader.
// WHY 307 and not 302: 307 guarantees the client repeats the same method and
// body, so a redirected PUT stays a PUT.
func (s *Server) kv(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	req := cluster.Request{Key: key}
	switch r.Method {
	case http.MethodGet:
		req.Op = kv.OpGet
	case http.MethodPut:
		body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
		if err != nil {
			fail(w, http.StatusBadRequest, "cannot read body")
			return
		}
		req.Op, req.Value = kv.OpPut, string(body)
	case http.MethodDelete:
		req.Op = kv.OpDelete
	}
	via := 0
	if q := r.URL.Query().Get("node"); q != "" {
		n, err := strconv.Atoi(q)
		if err != nil || n < 1 {
			fail(w, http.StatusBadRequest, "bad node id")
			return
		}
		via = n
	}
	// Allow more real time when the simulation runs in slow motion.
	budget := min(3*time.Second+s.c.Clock.Real(time.Second), 30*time.Second)
	ctx, cancel := context.WithTimeout(r.Context(), budget)
	defer cancel()

	start := time.Now()
	res, err := s.c.Do(ctx, via, req)
	var notLeader *cluster.NotLeaderError
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "op": req.Op, "key": key, "value": res.Value, "found": res.Found,
			"index": res.Index, "term": res.Term, "node": res.Node, "attempts": res.Attempts,
			"latencyMs": float64(time.Since(start).Microseconds()) / 1000,
		})
	case errors.As(err, &notLeader):
		if notLeader.Leader == 0 {
			w.Header().Set("Retry-After", "1")
			fail(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		w.Header().Set("X-Raft-Leader", strconv.Itoa(notLeader.Leader))
		w.Header().Set("Location", fmt.Sprintf("/api/kv/%s?node=%d", key, notLeader.Leader))
		writeJSON(w, http.StatusTemporaryRedirect, map[string]any{"ok": false, "error": err.Error(), "leader": notLeader.Leader})
	case errors.Is(err, cluster.ErrNodeDown):
		fail(w, http.StatusBadGateway, fmt.Sprintf("node %d is down", via))
	default:
		fail(w, http.StatusGatewayTimeout, "request was not committed in time (no reachable majority?): "+err.Error())
	}
}

func (s *Server) nodeAction(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		fail(w, http.StatusBadRequest, "bad node id")
		return
	}
	if r.URL.Path[len(r.URL.Path)-4:] == "kill" {
		err = s.c.Kill(id)
	} else {
		err = s.c.Restart(id)
	}
	if err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) partition(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Groups [][]int `json:"groups"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Groups) == 0 {
		fail(w, http.StatusBadRequest, `expected {"groups": [[1,2],[3,4,5]]}`)
		return
	}
	s.c.Partition(body.Groups)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) heal(w http.ResponseWriter, _ *http.Request) {
	s.c.Heal()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) conditions(w http.ResponseWriter, r *http.Request) {
	var body struct {
		LatencyMinMs float64 `json:"latencyMinMs"`
		LatencyMaxMs float64 `json:"latencyMaxMs"`
		DropRate     float64 `json:"dropRate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.LatencyMinMs < 0 || body.LatencyMaxMs > 5000 {
		fail(w, http.StatusBadRequest, "bad network conditions")
		return
	}
	toDur := func(ms float64) time.Duration { return time.Duration(ms * float64(time.Millisecond)) }
	s.c.SetNetwork(toDur(body.LatencyMinMs), toDur(body.LatencyMaxMs), body.DropRate)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) speed(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Scale float64 `json:"scale"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Scale < 0.5 || body.Scale > 200 {
		fail(w, http.StatusBadRequest, "scale must be between 0.5 and 200")
		return
	}
	s.c.SetScale(body.Scale)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) pause(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/sim/pause" {
		s.c.Pause()
	} else {
		s.c.Resume()
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

var upgrader = websocket.Upgrader{
	// The API is a local/dev tool with no cookies or auth, so any origin may
	// connect (needed for the Next.js dev server on another port).
	CheckOrigin: func(*http.Request) bool { return true },
}

// ws streams events to one browser. Events are batched into JSON arrays:
// everything that accumulated since the last frame goes out together.
func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	ch, history, cancel := s.c.Hub.Subscribe(8192)
	defer cancel()

	// Reader: we expect no messages, but reading is how we notice the close.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	st := s.c.State()
	first := append(history, events.Event{Kind: "state", T: st.Now, State: st})
	if err := conn.WriteJSON(first); err != nil {
		return
	}
	batch := make([]events.Event, 0, 256)
	for {
		select {
		case <-closed:
			return
		case e := <-ch:
			batch = append(batch[:0], e)
		drain:
			for len(batch) < 512 {
				select {
				case e := <-ch:
					batch = append(batch, e)
				default:
					break drain
				}
			}
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteJSON(batch); err != nil {
				return
			}
		}
	}
}
