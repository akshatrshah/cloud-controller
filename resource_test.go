package main

import (
	"testing"
	"time"
)

func newLeaderRaftForTest() *Raft {
	r := newTestRaft("n1", nil) // single-node cluster: self is automatically quorum
	r.mu.Lock()
	r.state = Leader
	r.currentTerm = 1
	r.mu.Unlock()
	return r
}

func TestResourceManager_CreateRejectsInvalidKind(t *testing.T) {
	r := newLeaderRaftForTest()
	rm := NewResourceManager(r, r.store.(*ResourceStore))

	if _, err := rm.Create("banana", "{}"); err == nil {
		t.Fatalf("expected an invalid kind to be rejected")
	}
}

func TestResourceManager_CreateSucceeds(t *testing.T) {
	r := newLeaderRaftForTest()
	store := r.store.(*ResourceStore)
	rm := NewResourceManager(r, store)

	res, err := rm.Create("compute", `{"cpu":2}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.State != StatePending {
		t.Fatalf("expected new resource PENDING, got %s", res.State)
	}
	if _, ok := store.Get(res.ID); !ok {
		t.Fatalf("expected resource visible in store immediately after Create returns")
	}
}

func TestResourceManager_TerminateRejectsUnknownResource(t *testing.T) {
	r := newLeaderRaftForTest()
	rm := NewResourceManager(r, r.store.(*ResourceStore))

	if _, err := rm.Terminate("does-not-exist"); err == nil {
		t.Fatalf("expected terminate on unknown resource to fail")
	}
}

func TestResourceManager_TerminateRejectsAlreadyTerminated(t *testing.T) {
	r := newLeaderRaftForTest()
	store := r.store.(*ResourceStore)
	rm := NewResourceManager(r, store)

	res, _ := rm.Create("compute", "{}")
	// Manually drive it to TERMINATED (bypassing the reconciler for test speed).
	store.Apply(Command{Op: "TRANSITION_STATE", ResourceID: res.ID, State: StateProvisioning, TimestampUnix: 1})
	store.Apply(Command{Op: "TRANSITION_STATE", ResourceID: res.ID, State: StateRunning, TimestampUnix: 2})
	store.Apply(Command{Op: "TRANSITION_STATE", ResourceID: res.ID, State: StateTerminating, TimestampUnix: 3})
	store.Apply(Command{Op: "TRANSITION_STATE", ResourceID: res.ID, State: StateTerminated, TimestampUnix: 4})

	if _, err := rm.Terminate(res.ID); err == nil {
		t.Fatalf("expected terminate on an already-TERMINATED resource to be rejected")
	}
}

func TestReconciler_DrivesPendingToProvisioning(t *testing.T) {
	r := newLeaderRaftForTest()
	store := r.store.(*ResourceStore)
	rm := NewResourceManager(r, store)
	rc := NewReconciler(r, store)

	res, _ := rm.Create("compute", "{}")
	rc.reconcileOnce()

	got, _ := store.Get(res.ID)
	if got.State != StateProvisioning {
		t.Fatalf("expected reconciler to move PENDING -> PROVISIONING, got %s", got.State)
	}
}

func TestReconciler_DoesNotAdvanceProvisioningBeforeSimulatedDelay(t *testing.T) {
	r := newLeaderRaftForTest()
	store := r.store.(*ResourceStore)
	rm := NewResourceManager(r, store)
	rc := NewReconciler(r, store)

	res, _ := rm.Create("compute", "{}")
	rc.reconcileOnce() // PENDING -> PROVISIONING
	rc.reconcileOnce() // should NOT yet advance to RUNNING (simulated delay hasn't elapsed)

	got, _ := store.Get(res.ID)
	if got.State != StateProvisioning {
		t.Fatalf("expected resource to remain PROVISIONING immediately after entering it, got %s", got.State)
	}
}

func TestReconciler_AdvancesProvisioningToRunningAfterDelay(t *testing.T) {
	r := newLeaderRaftForTest()
	store := r.store.(*ResourceStore)

	// Seed a resource already PROVISIONING with an UpdatedAtUnix far enough
	// in the past that the reconciler should consider it "done".
	store.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "vm1", Kind: "compute", TimestampUnix: 1000})
	store.Apply(Command{Op: "TRANSITION_STATE", ResourceID: "vm1", State: StateProvisioning,
		TimestampUnix: time.Now().Unix() - 999})

	rc := NewReconciler(r, store)
	rc.reconcileOnce()

	got, _ := store.Get("vm1")
	if got.State != StateRunning {
		t.Fatalf("expected reconciler to advance a long-PROVISIONING resource to RUNNING, got %s", got.State)
	}
}

func TestReconciler_NoopWhenNotLeader(t *testing.T) {
	r := newTestRaft("n1", []string{"n2"}) // stays Follower: never elected in this test
	store := r.store.(*ResourceStore)
	store.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "vm1", Kind: "compute", TimestampUnix: 1000})

	rc := NewReconciler(r, store)
	// Calling reconcileOnce() directly (bypassing Run()'s leader-check gate)
	// still must not change state: every underlying propose() call hits
	// Raft's own NotLeaderError rejection, so nothing gets applied.
	rc.reconcileOnce()

	got, _ := store.Get("vm1")
	if got.State != StatePending {
		t.Fatalf("expected state to remain PENDING when not leader, got %s", got.State)
	}
}
