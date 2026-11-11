package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Config is sourced entirely from environment variables so the same
// binary and container image runs unmodified across all pods -- k8s
// injects the only thing that differs (NODE_ID = pod name) via the
// Downward API.
//
//	NODE_ID  - this node's identity, e.g. "raft-0" (Downward API: metadata.name)
//	CLUSTER  - comma-separated addr:port list of ALL nodes, e.g.
//	           "raft-0.raft-headless:8080,raft-1.raft-headless:8080,..."
//	PORT     - HTTP port to listen on (default 8080)
//	PG_DSN   - Postgres connection string for durable persistence, e.g.
//	           "postgres://user:pass@host:5432/orbitcp?sslmode=disable".
//	           Optional: if unset, runs in-memory only (fine for local
//	           dev/tests, NOT crash-safe -- see README).
//
// `orbitcp loadgen ...` runs the built-in load generator instead of a
// node (see loadgen.go) -- shipping it in the same image means it can run
// as a pod inside the cluster, avoiding the kubectl port-forward
// bottleneck that caps client-side load tests from a laptop.
func main() {
	if len(os.Args) > 1 && os.Args[1] == "loadgen" {
		runLoadgen(os.Args[2:])
		return
	}

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		h, _ := os.Hostname()
		nodeID = h
	}

	clusterEnv := os.Getenv("CLUSTER")
	if clusterEnv == "" {
		log.Fatal("CLUSTER env var required, e.g. CLUSTER=raft-0.raft-headless:8080,raft-1.raft-headless:8080,raft-2.raft-headless:8080")
	}
	allAddrs := strings.Split(clusterEnv, ",")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	selfAddr, peers := resolveSelf(nodeID, allAddrs)
	log.Printf("[%s] self=%s peers=%v", nodeID, selfAddr, peers)

	var storage Storage
	if dsn := os.Getenv("PG_DSN"); dsn != "" {
		pgStorage, err := NewPostgresStorage(dsn, selfAddr)
		if err != nil {
			log.Fatalf("[%s] failed to connect to Postgres persistence: %v", nodeID, err)
		}
		storage = pgStorage
		log.Printf("[%s] durable persistence ENABLED (Postgres)", nodeID)
	} else {
		log.Printf("[%s] WARNING: PG_DSN not set -- running IN-MEMORY ONLY, state will not survive a restart", nodeID)
	}

	store := NewResourceStore()
	raftNode := NewRaft(selfAddr, peers, store, storage)

	restoreStart := time.Now()
	if err := raftNode.RestoreFromStorage(); err != nil {
		log.Fatalf("[%s] failed to restore from persisted storage: %v", nodeID, err)
	}
	if storage != nil {
		st := raftNode.GetStatus()
		log.Printf("[%s] restored from Postgres in %dms: term=%d log_entries=%d commit_index=%d resources=%d",
			nodeID, time.Since(restoreStart).Milliseconds(), st.Term, st.LogLength-1, st.CommitIndex, len(store.List()))
	}

	metrics := NewMetrics(raftNode, store)
	raftNode.SetMetrics(metrics)

	resourceManager := NewResourceManager(raftNode, store)
	resourceManager.SetMetrics(metrics)
	reconciler := NewReconciler(raftNode, store)
	reconciler.SetMetrics(metrics)

	mux := http.NewServeMux()
	raftNode.transport.RegisterHandlers(mux)
	RegisterAPIHandlers(mux, raftNode, store, resourceManager)
	mux.HandleFunc("GET /metrics", metrics.ServeHTTP)

	// Bind the listener BEFORE starting the election timer. Otherwise a
	// restarted node can time out and start a disruptive election while
	// the leader's heartbeats are still bouncing off a closed port.
	addr := ":" + port
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[%s] failed to listen on %s: %v", nodeID, addr, err)
	}
	raftNode.Run()
	go reconciler.Run()

	log.Printf("[%s] listening on %s", nodeID, addr)
	log.Fatal(http.Serve(ln, mux))
}

// resolveSelf finds this node's own address within the full cluster list
// (matched by NODE_ID prefix, e.g. "raft-0" matches "raft-0.raft-headless:8080")
// and returns it along with the list of every OTHER node (the peers).
func resolveSelf(nodeID string, allAddrs []string) (string, []string) {
	var self string
	var peers []string
	for _, a := range allAddrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		host := a
		if i := strings.LastIndex(a, ":"); i != -1 {
			host = a[:i]
		}
		// Matches either a bare pod name ("raft-0" == "raft-0") or a full
		// k8s DNS name ("raft-0" is a prefix of "raft-0.raft-headless...").
		if host == nodeID || strings.HasPrefix(host, nodeID+".") || a == nodeID {
			self = a
		} else {
			peers = append(peers, a)
		}
	}
	if self == "" {
		// Fallback: NODE_ID didn't match anything in CLUSTER (e.g. local
		// dev run) -- treat NODE_ID itself as the self address.
		self = nodeID
	}
	return self, peers
}
