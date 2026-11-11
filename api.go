package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type createResourceRequest struct {
	Kind string `json:"kind"` // "compute" | "network" | "storage"
	Spec string `json:"spec"` // free-form JSON, e.g. {"cpu":4,"memory_gb":16}
}

const (
	// forwardedHeader marks a write a follower relayed to the leader. A
	// node that receives a forwarded write but is no longer leader answers
	// 421 instead of forwarding again, so requests can never loop.
	forwardedHeader = "X-Orbitcp-Forwarded"
	// resourceIDHeader carries the resource ID chosen by the node that
	// first received a create, so retries and forwards reuse it.
	resourceIDHeader = "X-Orbitcp-Resource-Id"
	// writeRetryWindow is how long a node keeps trying to land a write
	// while there is no reachable leader (i.e. during a failover). It is
	// several times the worst-case election time plus a stuck leader's
	// check-quorum step-down, so a client sees one slow response instead
	// of an error.
	writeRetryWindow = 5 * time.Second
	writeRetryDelay  = 20 * time.Millisecond
	maxBodyBytes     = 1 << 20
)

var forwardClient = &http.Client{Timeout: proposeTimeout + 500*time.Millisecond, Transport: sharedHTTPTransport}

// RegisterAPIHandlers wires the client-facing control-plane API.
//
// Writes can be sent to ANY node. The leader commits them through Raft;
// a follower transparently forwards them to the leader, and if there is
// no leader right now (mid-failover) it retries until one is elected.
// Writes are only acknowledged after quorum commit. Reads are served from
// local state for availability (may lag the leader by up to one heartbeat
// interval on a follower).
func RegisterAPIHandlers(mux *http.ServeMux, r *Raft, store *ResourceStore, rm *ResourceManager) {
	mux.HandleFunc("POST /resources", func(w http.ResponseWriter, req *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(req.Body, maxBodyBytes))
		var body createResourceRequest
		if err != nil || json.Unmarshal(raw, &body) != nil || body.Kind == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": `bad request body, expected {"kind": "compute|network|storage", "spec": "..."}`})
			return
		}
		id := req.Header.Get(resourceIDHeader)
		if id == "" || len(id) > 64 {
			id = newResourceID(body.Kind)
		}
		req.Header.Set(resourceIDHeader, id)
		serveWrite(w, req, r, raw, func() (Resource, error) {
			return rm.CreateWithID(id, body.Kind, body.Spec)
		})
	})

	mux.HandleFunc("GET /resources", func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, http.StatusOK, store.List())
	})

	mux.HandleFunc("GET /resources/{id}", func(w http.ResponseWriter, req *http.Request) {
		id := req.PathValue("id")
		res, ok := store.Get(id)
		if !ok {
			http.Error(w, "resource not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})

	mux.HandleFunc("POST /resources/{id}/terminate", func(w http.ResponseWriter, req *http.Request) {
		id := req.PathValue("id")
		serveWrite(w, req, r, nil, func() (Resource, error) {
			return rm.Terminate(id)
		})
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, req *http.Request) {
		writeJSON(w, http.StatusOK, r.GetStatus())
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
}

// serveWrite runs op locally if this node is leader; otherwise it forwards
// the original request to the leader. Retryable failures (no leader yet,
// leader unreachable, leadership changed mid-write) are retried until
// writeRetryWindow runs out. Every write op is idempotent (creates carry a
// fixed ID, terminate on a TERMINATING resource is a no-op), so retrying a
// write whose outcome was unknown cannot apply it twice.
func serveWrite(w http.ResponseWriter, req *http.Request, r *Raft, body []byte, op func() (Resource, error)) {
	forwarded := req.Header.Get(forwardedHeader) != ""
	deadline := time.Now().Add(writeRetryWindow)
	for {
		res, err := op()
		if err == nil {
			writeJSON(w, http.StatusOK, res)
			return
		}

		leader := ""
		switch e := err.(type) {
		case *NotLeaderError:
			if forwarded {
				// We were the leader when the follower picked us, but not
				// anymore. Let the original node retry with fresh info.
				writeJSON(w, http.StatusMisdirectedRequest, map[string]string{"error": "not leader", "leader_id": e.LeaderID})
				return
			}
			leader = e.LeaderID
		case *LeadershipLostError, *TimeoutError:
			if forwarded {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
				return
			}
		default: // validation errors: retrying won't help
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		if leader != "" && leader != r.id {
			if status, respBody, ok := forwardToLeader(leader, req, body); ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				w.Write(respBody)
				return
			}
		}
		if time.Now().After(deadline) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader available, retry later"})
			return
		}
		time.Sleep(writeRetryDelay)
	}
}

// forwardToLeader relays a write to the leader. ok=false means "try again"
// (leader unreachable, or it answered 421/503 because leadership moved).
func forwardToLeader(leader string, req *http.Request, body []byte) (status int, respBody []byte, ok bool) {
	fr, err := http.NewRequest(req.Method, "http://"+leader+req.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		return 0, nil, false
	}
	fr.Header.Set(forwardedHeader, "1")
	fr.Header.Set("Content-Type", "application/json")
	if id := req.Header.Get(resourceIDHeader); id != "" {
		fr.Header.Set(resourceIDHeader, id)
	}
	resp, err := forwardClient.Do(fr)
	if err != nil {
		return 0, nil, false
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, false
	}
	if resp.StatusCode == http.StatusMisdirectedRequest || resp.StatusCode == http.StatusServiceUnavailable {
		return 0, nil, false
	}
	return resp.StatusCode, b, true
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
