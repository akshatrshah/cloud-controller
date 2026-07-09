package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
)

// testDSN returns the Postgres connection string for tests, or "" if none
// is configured -- persistence tests skip (not fail) in that case, so
// `go test ./...` still works for anyone without a local Postgres. CI
// and setup.sh both provide TEST_PG_DSN so the real thing does get
// exercised in the environments that matter.
func testDSN(t *testing.T) string {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set, skipping Postgres persistence tests (see README)")
	}
	return dsn
}

func TestPostgresStorage_SaveAndLoadMeta(t *testing.T) {
	dsn := testDSN(t)
	ps, err := NewPostgresStorage(dsn, "test-node-meta")
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer ps.Close()

	if err := ps.SaveMeta(7, "some-leader", 42); err != nil {
		t.Fatalf("SaveMeta failed: %v", err)
	}
	term, votedFor, commitIndex, found, err := ps.LoadMeta()
	if err != nil {
		t.Fatalf("LoadMeta failed: %v", err)
	}
	if !found {
		t.Fatalf("expected found=true after a save")
	}
	if term != 7 || votedFor != "some-leader" || commitIndex != 42 {
		t.Fatalf("got term=%d votedFor=%q commitIndex=%d, want 7/some-leader/42", term, votedFor, commitIndex)
	}
}

func TestPostgresStorage_LoadMetaOnFreshNodeReturnsNotFound(t *testing.T) {
	dsn := testDSN(t)
	ps, err := NewPostgresStorage(dsn, "test-node-fresh-"+randSuffix())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer ps.Close()

	_, _, _, found, err := ps.LoadMeta()
	if err != nil {
		t.Fatalf("LoadMeta failed: %v", err)
	}
	if found {
		t.Fatalf("expected found=false for a node with no persisted state yet")
	}
}

func TestPostgresStorage_AppendAndLoadLog(t *testing.T) {
	dsn := testDSN(t)
	nodeID := "test-node-log-" + randSuffix()
	ps, err := NewPostgresStorage(dsn, nodeID)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer ps.Close()

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: Command{Op: "CREATE_RESOURCE", ResourceID: "vm1", Kind: "compute", TimestampUnix: 1000}},
		{Term: 1, Index: 2, Command: Command{Op: "TRANSITION_STATE", ResourceID: "vm1", State: StateProvisioning, TimestampUnix: 1001}},
	}
	if err := ps.AppendLog(entries); err != nil {
		t.Fatalf("AppendLog failed: %v", err)
	}

	loaded, err := ps.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog failed: %v", err)
	}
	// Sentinel (index 0) + 2 real entries.
	if len(loaded) != 3 {
		t.Fatalf("expected 3 log entries (sentinel + 2), got %d", len(loaded))
	}
	if loaded[1].Command.ResourceID != "vm1" || loaded[2].Command.State != StateProvisioning {
		t.Fatalf("loaded log entries don't match what was appended: %+v", loaded)
	}
}

func TestPostgresStorage_TruncateFrom(t *testing.T) {
	dsn := testDSN(t)
	nodeID := "test-node-trunc-" + randSuffix()
	ps, err := NewPostgresStorage(dsn, nodeID)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer ps.Close()

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: Command{Op: "CREATE_RESOURCE", ResourceID: "a"}},
		{Term: 1, Index: 2, Command: Command{Op: "CREATE_RESOURCE", ResourceID: "b"}},
		{Term: 1, Index: 3, Command: Command{Op: "CREATE_RESOURCE", ResourceID: "c"}},
	}
	if err := ps.AppendLog(entries); err != nil {
		t.Fatalf("AppendLog failed: %v", err)
	}
	if err := ps.TruncateLogFrom(2); err != nil {
		t.Fatalf("TruncateLogFrom failed: %v", err)
	}

	loaded, _ := ps.LoadLog()
	if len(loaded) != 2 { // sentinel + entry at index 1 only
		t.Fatalf("expected entries at index >= 2 to be truncated, got %d entries: %+v", len(loaded), loaded)
	}
}

func TestRaft_RestoreFromStorage_RecoversAfterRestart(t *testing.T) {
	dsn := testDSN(t)
	nodeID := "test-node-recovery-" + randSuffix()

	// --- "Before crash": a node commits some entries and persists them ---
	ps1, err := NewPostgresStorage(dsn, nodeID)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	r1 := NewRaft(nodeID, nil, NewResourceStore(), ps1)
	go r1.applyLoop()
	r1.mu.Lock()
	r1.state = Leader
	r1.currentTerm = 1
	r1.mu.Unlock()

	if _, err := r1.Propose(Command{Op: "CREATE_RESOURCE", ResourceID: "vm1", Kind: "compute", TimestampUnix: 1000}); err != nil {
		t.Fatalf("propose failed: %v", err)
	}
	ps1.Close()

	// --- "After restart": a brand new Raft + store, same node id, reconnects to Postgres ---
	ps2, err := NewPostgresStorage(dsn, nodeID)
	if err != nil {
		t.Fatalf("failed to reconnect: %v", err)
	}
	defer ps2.Close()

	store2 := NewResourceStore()
	r2 := NewRaft(nodeID, nil, store2, ps2)
	if err := r2.RestoreFromStorage(); err != nil {
		t.Fatalf("RestoreFromStorage failed: %v", err)
	}

	res, ok := store2.Get("vm1")
	if !ok {
		t.Fatalf("expected resource 'vm1' to be restored into the state machine after 'restart', but it's missing -- this is the core crash-recovery guarantee")
	}
	if res.State != StatePending {
		t.Fatalf("expected restored resource in PENDING state, got %s", res.State)
	}
	if r2.currentTerm != 1 {
		t.Fatalf("expected restored currentTerm=1, got %d", r2.currentTerm)
	}
}

var suffixCounter int64

func randSuffix() string {
	n := atomic.AddInt64(&suffixCounter, 1)
	return fmt.Sprintf("%d-%d", os.Getpid(), n)
}
