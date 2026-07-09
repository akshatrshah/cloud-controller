package main

import (
	"log"
	"net/http"
	"os"
	"strings"
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
func main() {
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

	if err := raftNode.RestoreFromStorage(); err != nil {
		log.Fatalf("[%s] failed to restore from persisted storage: %v", nodeID, err)
	}
	raftNode.Run()

	resourceManager := NewResourceManager(raftNode, store)
	reconciler := NewReconciler(raftNode, store)
	go reconciler.Run()

	mux := http.NewServeMux()
	raftNode.transport.RegisterHandlers(mux)
	RegisterAPIHandlers(mux, raftNode, store, resourceManager)

	addr := ":" + port
	log.Printf("[%s] listening on %s", nodeID, addr)
	log.Fatal(http.ListenAndServe(addr, mux))
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
