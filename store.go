package main

import "sync"

// Lifecycle states for a provisioned resource. Mirrors the state machine
// every real IaaS control plane implements for compute/network/storage
// objects (AWS EC2 instance states, GCP Compute VM lifecycle, Kubernetes
// Pod phases -- same shape, different names).
const (
	StatePending      = "PENDING"      // accepted, not yet being worked on
	StateProvisioning = "PROVISIONING" // control plane is actively creating it
	StateRunning      = "RUNNING"      // healthy and serving
	StateTerminating  = "TERMINATING"  // teardown in progress
	StateTerminated   = "TERMINATED"   // fully torn down (terminal)
	StateFailed       = "FAILED"       // unrecoverable error (terminal)
)

// validTransitions is the authoritative state machine definition. It is
// consulted identically by every replica when applying a TRANSITION_STATE
// command, which is what makes it safe: an invalid transition is a no-op
// on every node, not just the one that happened to catch it first.
var validTransitions = map[string]map[string]bool{
	StatePending:      {StateProvisioning: true, StateTerminating: true, StateFailed: true}, // TERMINATING here = "cancel before provisioning starts"
	StateProvisioning: {StateRunning: true, StateFailed: true},
	StateRunning:      {StateTerminating: true, StateFailed: true},
	StateTerminating:  {StateTerminated: true, StateFailed: true},
	StateTerminated:   {}, // terminal
	StateFailed:       {}, // terminal
}

func isValidTransition(from, to string) bool {
	next, ok := validTransitions[from]
	if !ok {
		return false
	}
	return next[to]
}

// Resource is a single provisioned (or provisioning) IaaS object.
type Resource struct {
	ID            string `json:"id"`
	Kind          string `json:"kind"` // "compute" | "network" | "storage"
	Spec          string `json:"spec"`
	State         string `json:"state"`
	Reason        string `json:"reason,omitempty"`
	CreatedAtUnix int64  `json:"created_at_unix"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

// ResourceStore is the replicated state machine: every node applies the
// exact same sequence of committed Commands, in the exact same order,
// converging on identical resource state cluster-wide.
type ResourceStore struct {
	mu        sync.RWMutex
	resources map[string]*Resource
}

func NewResourceStore() *ResourceStore {
	return &ResourceStore{resources: make(map[string]*Resource)}
}

func (s *ResourceStore) Apply(cmd Command) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case "CREATE_RESOURCE":
		if _, exists := s.resources[cmd.ResourceID]; exists {
			return // idempotent: a retried/replayed create is a no-op
		}
		s.resources[cmd.ResourceID] = &Resource{
			ID:            cmd.ResourceID,
			Kind:          cmd.Kind,
			Spec:          cmd.Spec,
			State:         StatePending,
			CreatedAtUnix: cmd.TimestampUnix,
			UpdatedAtUnix: cmd.TimestampUnix,
		}

	case "TRANSITION_STATE":
		r, ok := s.resources[cmd.ResourceID]
		if !ok {
			return
		}
		if !isValidTransition(r.State, cmd.State) {
			return // invalid transition: deterministic no-op on every replica
		}
		r.State = cmd.State
		r.Reason = cmd.Reason
		r.UpdatedAtUnix = cmd.TimestampUnix
	}
}

func (s *ResourceStore) Get(id string) (Resource, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.resources[id]
	if !ok {
		return Resource{}, false
	}
	return *r, true
}

func (s *ResourceStore) List() []Resource {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Resource, 0, len(s.resources))
	for _, r := range s.resources {
		out = append(out, *r)
	}
	return out
}

// ListByState returns all resources currently in a given state -- used by
// the reconciler loop to find work (e.g. "which resources are PENDING and
// need to start provisioning").
func (s *ResourceStore) ListByState(state string) []Resource {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Resource
	for _, r := range s.resources {
		if r.State == state {
			out = append(out, *r)
		}
	}
	return out
}
