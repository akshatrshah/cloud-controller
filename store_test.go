package main

import "testing"

func TestResourceStore_CreateAndGet(t *testing.T) {
	s := NewResourceStore()
	s.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "compute-1", Kind: "compute", Spec: `{"cpu":4}`, TimestampUnix: 1000})

	r, ok := s.Get("compute-1")
	if !ok {
		t.Fatalf("expected resource to exist")
	}
	if r.State != StatePending {
		t.Fatalf("expected new resource to start PENDING, got %s", r.State)
	}
	if r.CreatedAtUnix != 1000 || r.UpdatedAtUnix != 1000 {
		t.Fatalf("expected timestamps set from command, got created=%d updated=%d", r.CreatedAtUnix, r.UpdatedAtUnix)
	}
}

func TestResourceStore_CreateIsIdempotent(t *testing.T) {
	s := NewResourceStore()
	s.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "x", Kind: "compute", Spec: "original", TimestampUnix: 1000})
	s.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "x", Kind: "compute", Spec: "replayed", TimestampUnix: 2000})

	r, _ := s.Get("x")
	if r.Spec != "original" {
		t.Fatalf("expected a replayed CREATE for an existing ID to be a no-op, got spec=%q", r.Spec)
	}
}

func TestResourceStore_ValidTransitionSequence(t *testing.T) {
	s := NewResourceStore()
	s.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "vm1", Kind: "compute", TimestampUnix: 1000})

	sequence := []string{StateProvisioning, StateRunning, StateTerminating, StateTerminated}
	for i, target := range sequence {
		s.Apply(Command{Op: "TRANSITION_STATE", ResourceID: "vm1", State: target, TimestampUnix: int64(1000 + i)})
		r, _ := s.Get("vm1")
		if r.State != target {
			t.Fatalf("step %d: expected state %s, got %s", i, target, r.State)
		}
	}
}

func TestResourceStore_RejectsInvalidTransition(t *testing.T) {
	s := NewResourceStore()
	s.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "vm1", Kind: "compute", TimestampUnix: 1000})

	// PENDING -> RUNNING is not a legal direct transition (must go through
	// PROVISIONING first). The state machine must reject it deterministically.
	s.Apply(Command{Op: "TRANSITION_STATE", ResourceID: "vm1", State: StateRunning, TimestampUnix: 2000})

	r, _ := s.Get("vm1")
	if r.State != StatePending {
		t.Fatalf("expected invalid transition to be a no-op, but state changed to %s", r.State)
	}
}

func TestResourceStore_RejectsTransitionFromTerminalState(t *testing.T) {
	s := NewResourceStore()
	s.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "vm1", Kind: "compute", TimestampUnix: 1000})
	s.Apply(Command{Op: "TRANSITION_STATE", ResourceID: "vm1", State: StateFailed, TimestampUnix: 1001})

	// FAILED is terminal -- nothing should move it anywhere else.
	s.Apply(Command{Op: "TRANSITION_STATE", ResourceID: "vm1", State: StateProvisioning, TimestampUnix: 1002})

	r, _ := s.Get("vm1")
	if r.State != StateFailed {
		t.Fatalf("expected FAILED to remain terminal, got %s", r.State)
	}
}

func TestResourceStore_TransitionOnUnknownResourceIsNoop(t *testing.T) {
	s := NewResourceStore()
	s.Apply(Command{Op: "TRANSITION_STATE", ResourceID: "does-not-exist", State: StateRunning, TimestampUnix: 1000})
	if _, ok := s.Get("does-not-exist"); ok {
		t.Fatalf("expected transition on a nonexistent resource to create nothing")
	}
}

func TestResourceStore_ListByState(t *testing.T) {
	s := NewResourceStore()
	s.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "a", Kind: "compute", TimestampUnix: 1000})
	s.Apply(Command{Op: "CREATE_RESOURCE", ResourceID: "b", Kind: "storage", TimestampUnix: 1000})
	s.Apply(Command{Op: "TRANSITION_STATE", ResourceID: "b", State: StateProvisioning, TimestampUnix: 1001})

	pending := s.ListByState(StatePending)
	provisioning := s.ListByState(StateProvisioning)
	if len(pending) != 1 || pending[0].ID != "a" {
		t.Fatalf("expected exactly resource 'a' in PENDING, got %+v", pending)
	}
	if len(provisioning) != 1 || provisioning[0].ID != "b" {
		t.Fatalf("expected exactly resource 'b' in PROVISIONING, got %+v", provisioning)
	}
}

func TestIsValidTransition_Table(t *testing.T) {
	cases := []struct {
		from, to string
		want     bool
	}{
		{StatePending, StateProvisioning, true},
		{StatePending, StateFailed, true},
		{StatePending, StateRunning, false}, // must go through PROVISIONING
		{StateProvisioning, StateRunning, true},
		{StateRunning, StateTerminating, true},
		{StateRunning, StatePending, false}, // no going backwards
		{StateTerminating, StateTerminated, true},
		{StateTerminated, StateProvisioning, false}, // terminal
		{StateFailed, StateRunning, false},          // terminal
	}
	for _, tc := range cases {
		if got := isValidTransition(tc.from, tc.to); got != tc.want {
			t.Errorf("isValidTransition(%s, %s) = %v, want %v", tc.from, tc.to, got, tc.want)
		}
	}
}
