package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

// Config is sourced entirely from environment variables so the same
// binary and container image runs unmodified across all pods -- k8s
// injects the only thing that differs (NODE_ID = pod name) via the
// Downward API.
//
//	NODE_ID  - this node's identity, e.g. "raft-0" (Downward API: metadata.name)
//	CLUSTER  - comma-separated addr:port list of ALL nodes' HTTP addresses,
//	           e.g. "raft-0.raft-headless:8080,raft-1.raft-headless:8080,..."
//	           (the actual Raft consensus TCP transport runs on HTTP-port+10000
//	           on each node -- derived automatically, not separately configured)
//	PORT     - HTTP port to listen on (default 8080)
//	PG_DSN   - Postgres connection string for durable persistence. Optional:
//	           if unset, runs in-memory only (fine for local dev/tests, NOT
//	           crash-safe).
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

	httpPortStr := os.Getenv("PORT")
	if httpPortStr == "" {
		httpPortStr = "8080"
	}
	httpPort, err := strconv.Atoi(httpPortStr)
	if err != nil {
		log.Fatalf("invalid PORT %q: %v", httpPortStr, err)
	}

	selfHTTPAddr, peerHTTPAddrs := resolveSelf(nodeID, allAddrs)
	selfRaftAddr := raftAddrFromHTTPAddr(selfHTTPAddr)
	log.Printf("[%s] self=%s (raft transport on %s) peers=%v", nodeID, selfHTTPAddr, selfRaftAddr, peerHTTPAddrs)

	// --- Durable storage: Postgres if configured, in-memory otherwise ---
	var logStore raft.LogStore
	var stableStore raft.StableStore
	persistent := false
	if dsn := os.Getenv("PG_DSN"); dsn != "" {
		pgStore, err := NewPostgresStore(dsn, selfHTTPAddr)
		if err != nil {
			log.Fatalf("[%s] failed to connect to Postgres persistence: %v", nodeID, err)
		}
		logStore = pgStore
		stableStore = pgStore
		persistent = true
		log.Printf("[%s] durable persistence ENABLED (Postgres)", nodeID)
	} else {
		mem := raft.NewInmemStore()
		logStore = mem
		stableStore = mem
		log.Printf("[%s] WARNING: PG_DSN not set -- running IN-MEMORY ONLY, state will not survive a restart", nodeID)
	}

	// Snapshots: local disk, one directory per node (unique by port, so
	// multiple nodes can run on one machine for local dev without
	// colliding). Snapshots are NOT Postgres-backed in this project --
	// only the log/term/vote are (see README's "Known limitations").
	snapDir := filepath.Join(os.TempDir(), fmt.Sprintf("orbitcp-snapshots-%d", httpPort))
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		log.Fatalf("[%s] failed to create snapshot dir: %v", nodeID, err)
	}
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 2, os.Stderr)
	if err != nil {
		log.Fatalf("[%s] failed to create snapshot store: %v", nodeID, err)
	}

	// --- Transport: raft's own TCP-based RPC, not our HTTP API ---
	// A StatefulSet pod's own headless-service DNS record isn't guaranteed
	// to be published by CoreDNS the instant the pod starts -- there's a
	// small propagation delay after the pod gets its IP and the endpoint
	// controller registers it. A one-shot lookup here can lose that race
	// and crash-loop the pod indefinitely (log.Fatalf -> restart -> same
	// race -> repeat). Retry with backoff instead of failing on the first
	// miss. Found by watching a real StatefulSet rolling update actually
	// hit this on a freshly-recreated pod, not by code review.
	var raftBindAddr *net.TCPAddr
	resolveDeadline := time.Now().Add(30 * time.Second)
	for {
		raftBindAddr, err = net.ResolveTCPAddr("tcp", selfRaftAddr)
		if err == nil {
			break
		}
		if time.Now().After(resolveDeadline) {
			log.Fatalf("[%s] failed to resolve raft transport address %q after 30s: %v", nodeID, selfRaftAddr, err)
		}
		log.Printf("[%s] waiting for DNS to resolve %q (StatefulSet pod DNS can lag briefly on startup): %v", nodeID, selfRaftAddr, err)
		time.Sleep(1 * time.Second)
	}
	transport, err := raft.NewTCPTransport(selfRaftAddr, raftBindAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("[%s] failed to create raft transport: %v", nodeID, err)
	}

	store := NewResourceStore()

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(selfHTTPAddr)
	// Election timeout deliberately wide (default is ~1s) so it
	// comfortably exceeds real pod-to-pod network latency inside
	// Kubernetes and avoids split-vote storms during demos.
	config.HeartbeatTimeout = 1500 * time.Millisecond
	config.ElectionTimeout = 1500 * time.Millisecond

	raftNode, err := raft.NewRaft(config, store, logStore, stableStore, snapStore, transport)
	if err != nil {
		log.Fatalf("[%s] failed to start raft: %v", nodeID, err)
	}

	// Bootstrap the cluster ONCE, from a fixed static configuration listing
	// every known node. Only takes effect if this node has no existing
	// state (fresh start); on a restart, hashicorp/raft loads its prior
	// configuration from the log/stable store instead, so calling this
	// again is intentionally a safe, ignorable no-op.
	hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
	if err != nil {
		log.Fatalf("[%s] failed to check existing state: %v", nodeID, err)
	}
	if !hasState {
		servers := []raft.Server{{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(selfHTTPAddr),
			Address:  raft.ServerAddress(selfRaftAddr),
		}}
		for _, p := range peerHTTPAddrs {
			servers = append(servers, raft.Server{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(p),
				Address:  raft.ServerAddress(raftAddrFromHTTPAddr(p)),
			})
		}
		bootErr := raftNode.BootstrapCluster(raft.Configuration{Servers: servers}).Error()
		if bootErr != nil {
			log.Printf("[%s] bootstrap: %v (safe to ignore if cluster was already bootstrapped by a peer)", nodeID, bootErr)
		} else {
			log.Printf("[%s] bootstrap succeeded (%d servers)", nodeID, len(servers))
		}
	}

	metrics := NewMetrics(raftNode, store)
	resourceManager := NewResourceManager(raftNode, store)
	resourceManager.SetMetrics(metrics)
	reconciler := NewReconciler(raftNode, store)
	reconciler.SetMetrics(metrics)
	go reconciler.Run()

	mux := http.NewServeMux()
	RegisterAPIHandlers(mux, raftNode, selfHTTPAddr, persistent, store, resourceManager)
	mux.HandleFunc("GET /metrics", metrics.ServeHTTP)

	addr := ":" + httpPortStr
	log.Printf("[%s] listening on %s", nodeID, addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

// resolveSelf finds this node's own HTTP address within the full cluster
// list (matched by NODE_ID prefix, e.g. "raft-0" matches
// "raft-0.raft-headless:8080") and returns it along with every OTHER
// node's HTTP address (the peers).
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
		if host == nodeID || strings.HasPrefix(host, nodeID+".") || a == nodeID {
			self = a
		} else {
			peers = append(peers, a)
		}
	}
	if self == "" {
		self = nodeID
	}
	return self, peers
}

// raftAddrFromHTTPAddr derives the Raft consensus transport's TCP address
// from a node's HTTP address by adding 10000 to the port -- e.g.
// "raft-0.raft-headless:8080" -> "raft-0.raft-headless:18080". Keeps the
// externally-configured CLUSTER env var in familiar HTTP-address form
// while giving hashicorp/raft's TCP transport its own separate port (it
// speaks a binary protocol, not HTTP, so it can't share port 8080).
//
// The offset is deliberately large (10000, not +1): local multi-node
// testing uses sequential HTTP ports on localhost (9701, 9702, 9703...),
// and a +1 offset means node 9701's raft port (9702) collides directly
// with node 9702's own HTTP port -- a real bug hit while testing this
// locally, not a hypothetical.
func raftAddrFromHTTPAddr(httpAddr string) string {
	i := strings.LastIndex(httpAddr, ":")
	if i == -1 {
		return httpAddr
	}
	host := httpAddr[:i]
	port, err := strconv.Atoi(httpAddr[i+1:])
	if err != nil {
		return httpAddr
	}
	return fmt.Sprintf("%s:%d", host, port+10000)
}

// httpAddrFromRaftAddr is the inverse of raftAddrFromHTTPAddr. raft.Raft's
// own Leader() method returns the leader's TRANSPORT address (since that's
// what the library itself uses internally) -- but an HTTP client following
// a leader_id from a 421 response needs the HTTP address, not the binary
// Raft protocol port. Without this conversion, clients would be pointed at
// the wrong port entirely. Found by actually curling the leader_id from a
// live cluster and getting a connection reset, not by code review.
func httpAddrFromRaftAddr(raftAddr string) string {
	i := strings.LastIndex(raftAddr, ":")
	if i == -1 {
		return raftAddr
	}
	host := raftAddr[:i]
	port, err := strconv.Atoi(raftAddr[i+1:])
	if err != nil || port < 10000 {
		return raftAddr
	}
	return fmt.Sprintf("%s:%d", host, port-10000)
}
