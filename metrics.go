package main

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Metrics holds every counter/gauge this node exposes. Prometheus's scrape
// protocol is just plain text over HTTP (the "exposition format") -- there's
// no wire-protocol magic requiring a client library, so this is written by
// hand rather than pulling in prometheus/client_golang and its dependency
// tree. Same reasoning as building Raft from the paper instead of importing
// hashicorp/raft: understanding what's actually being exposed, not just
// calling a library that does it.
type Metrics struct {
	mu sync.Mutex

	// Counters (monotonically increasing; Prometheus computes rates from these)
	electionsStarted    int64
	electionsWon        int64
	proposalsSucceeded  int64
	proposalsFailed     int64
	resourcesCreated    int64
	resourcesTerminated int64

	// Gauges (current point-in-time value)
	currentTerm int64
	isLeader    int64 // 0 or 1
	commitIndex int64
	logLength   int64

	// resourcesByState is rebuilt on each scrape from the live store
	// rather than tracked incrementally, so it can never drift from the
	// actual state machine.
	store *ResourceStore
	raft  *Raft
}

func NewMetrics(raft *Raft, store *ResourceStore) *Metrics {
	return &Metrics{raft: raft, store: store}
}

func (m *Metrics) IncElectionsStarted()    { atomic.AddInt64(&m.electionsStarted, 1) }
func (m *Metrics) IncElectionsWon()        { atomic.AddInt64(&m.electionsWon, 1) }
func (m *Metrics) IncProposalsSucceeded()  { atomic.AddInt64(&m.proposalsSucceeded, 1) }
func (m *Metrics) IncProposalsFailed()     { atomic.AddInt64(&m.proposalsFailed, 1) }
func (m *Metrics) IncResourcesCreated()    { atomic.AddInt64(&m.resourcesCreated, 1) }
func (m *Metrics) IncResourcesTerminated() { atomic.AddInt64(&m.resourcesTerminated, 1) }

// ServeHTTP renders current metric values in Prometheus's plain-text
// exposition format: `# HELP`/`# TYPE` metadata lines, then
// `metric_name{label="value"} number` lines. Any HTTP client (curl,
// Prometheus's scraper, a browser) can read it identically -- it's just text.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	status := m.raft.GetStatus()

	isLeader := int64(0)
	if status.State == "LEADER" {
		isLeader = 1
	}

	var b strings.Builder

	writeGauge(&b, "orbitcp_raft_term", "Current Raft term observed by this node", float64(status.Term))
	writeGauge(&b, "orbitcp_raft_is_leader", "1 if this node currently believes it is the leader, else 0", float64(isLeader))
	writeGauge(&b, "orbitcp_raft_commit_index", "Highest log index known to be committed", float64(status.CommitIndex))
	writeGauge(&b, "orbitcp_raft_log_length", "Total number of entries in this node's replicated log", float64(status.LogLength))
	writeGauge(&b, "orbitcp_raft_quorum_size", "Number of nodes required for a majority in this cluster", float64(status.Quorum))
	writeGauge(&b, "orbitcp_raft_peer_count", "Number of other nodes in the cluster", float64(status.Peers))
	writeGauge(&b, "orbitcp_raft_last_failover_ms", "On a node that took over from a failed leader: ms between its last contact with the old leader and becoming leader (0 = never)", float64(status.LastFailoverMs))

	writeCounter(&b, "orbitcp_elections_started_total", "Total elections this node has started as a candidate", float64(atomic.LoadInt64(&m.electionsStarted)))
	writeCounter(&b, "orbitcp_elections_won_total", "Total elections this node has won and become leader", float64(atomic.LoadInt64(&m.electionsWon)))
	writeCounter(&b, "orbitcp_proposals_succeeded_total", "Total Propose() calls that committed successfully", float64(atomic.LoadInt64(&m.proposalsSucceeded)))
	writeCounter(&b, "orbitcp_proposals_failed_total", "Total Propose() calls that failed (not leader, or timed out)", float64(atomic.LoadInt64(&m.proposalsFailed)))
	writeCounter(&b, "orbitcp_resources_created_total", "Total resources ever created on this node's log", float64(atomic.LoadInt64(&m.resourcesCreated)))
	writeCounter(&b, "orbitcp_resources_terminated_total", "Total resources ever fully terminated", float64(atomic.LoadInt64(&m.resourcesTerminated)))

	writeResourceStateGauges(&b, m.store)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprint(w, b.String())
}

func writeGauge(b *strings.Builder, name, help string, value float64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n", name, help, name, name, value)
}

func writeCounter(b *strings.Builder, name, help string, value float64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n%s %g\n", name, help, name, name, value)
}

// writeResourceStateGauges emits one time series per lifecycle state, e.g.
// orbitcp_resources_by_state{state="RUNNING"} 3 -- the standard Prometheus
// pattern for "count of things in each of a fixed set of categories."
func writeResourceStateGauges(b *strings.Builder, store *ResourceStore) {
	states := []string{StatePending, StateProvisioning, StateRunning, StateTerminating, StateTerminated, StateFailed}
	counts := make(map[string]int, len(states))
	for _, r := range store.List() {
		counts[r.State]++
	}

	fmt.Fprintf(b, "# HELP orbitcp_resources_by_state Current count of resources in each lifecycle state\n# TYPE orbitcp_resources_by_state gauge\n")
	sort.Strings(states) // stable output ordering
	for _, s := range states {
		fmt.Fprintf(b, "orbitcp_resources_by_state{state=%q} %d\n", s, counts[s])
	}
}
