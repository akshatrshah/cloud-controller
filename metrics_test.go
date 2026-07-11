package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetrics_ExposesValidPrometheusFormat(t *testing.T) {
	r := newLeaderRaftForTest()
	store := r.store.(*ResourceStore)
	m := NewMetrics(r, store)
	r.SetMetrics(m)

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, req)

	body := w.Body.String()

	// Every metric must have both a HELP and a TYPE line preceding its
	// value line -- that's what makes it valid Prometheus exposition
	// format, not just arbitrary text.
	requiredMetrics := []string{
		"orbitcp_raft_term",
		"orbitcp_raft_is_leader",
		"orbitcp_raft_commit_index",
		"orbitcp_elections_started_total",
		"orbitcp_proposals_succeeded_total",
		"orbitcp_resources_by_state",
	}
	for _, name := range requiredMetrics {
		if !strings.Contains(body, "# HELP "+name) {
			t.Errorf("missing HELP line for %s", name)
		}
		if !strings.Contains(body, "# TYPE "+name) {
			t.Errorf("missing TYPE line for %s", name)
		}
	}

	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("expected text/plain content type, got %q", ct)
	}
}

func TestMetrics_ReflectsLeaderState(t *testing.T) {
	r := newLeaderRaftForTest()
	store := r.store.(*ResourceStore)
	m := NewMetrics(r, store)

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, req)

	if !strings.Contains(w.Body.String(), "orbitcp_raft_is_leader 1") {
		t.Errorf("expected is_leader=1 for a node in Leader state, got:\n%s", w.Body.String())
	}
}

func TestMetrics_CountersIncrement(t *testing.T) {
	r := newLeaderRaftForTest()
	store := r.store.(*ResourceStore)
	m := NewMetrics(r, store)
	rm := NewResourceManager(r, store)
	rm.SetMetrics(m)

	if _, err := rm.Create("compute", "{}"); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, req)

	if !strings.Contains(w.Body.String(), "orbitcp_resources_created_total 1") {
		t.Errorf("expected resources_created_total=1 after one Create(), got:\n%s", w.Body.String())
	}
}

func TestMetrics_ResourceStateGaugesReflectStore(t *testing.T) {
	r := newLeaderRaftForTest()
	store := r.store.(*ResourceStore)
	m := NewMetrics(r, store)

	store.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "a", Kind: "compute", TimestampUnix: 1000})
	store.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "b", Kind: "storage", TimestampUnix: 1000})
	store.Apply(Command{Op: "TRANSITION_STATE", ResourceID: "b", State: StateProvisioning, TimestampUnix: 1001})

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, req)
	body := w.Body.String()

	if !strings.Contains(body, `orbitcp_resources_by_state{state="PENDING"} 1`) {
		t.Errorf("expected 1 PENDING resource in gauge output, got:\n%s", body)
	}
	if !strings.Contains(body, `orbitcp_resources_by_state{state="PROVISIONING"} 1`) {
		t.Errorf("expected 1 PROVISIONING resource in gauge output, got:\n%s", body)
	}
}
