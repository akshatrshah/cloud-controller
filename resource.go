package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"time"
)

// ResourceManager exposes the operations the HTTP API calls to mutate
// resource state. Every mutation goes through Raft's Propose(), so it's
// only accepted on the leader and only takes effect once replicated to a
// quorum -- the control plane can never have two nodes disagreeing about
// whether a VM is RUNNING or TERMINATED.
type ResourceManager struct {
	raft    *Raft
	store   *ResourceStore
	metrics *Metrics // nil = metrics disabled
}

func NewResourceManager(r *Raft, s *ResourceStore) *ResourceManager {
	return &ResourceManager{raft: r, store: s}
}

// SetMetrics attaches a metrics recorder after construction.
func (rm *ResourceManager) SetMetrics(m *Metrics) {
	rm.metrics = m
}

// ErrInvalidKind is returned for a kind other than compute/network/storage.
var ErrInvalidKind = fmt.Errorf("kind must be one of: compute, network, storage")

func newResourceID(kind string) string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("%s-%s", kind, hex.EncodeToString(b))
}

// Create provisions a new resource, starting in PENDING. The reconciler
// loop (see below) will pick it up and drive it forward automatically --
// callers don't manually walk the state machine.
func (rm *ResourceManager) Create(kind, spec string) (Resource, error) {
	return rm.CreateWithID(newResourceID(kind), kind, spec)
}

// CreateWithID is Create with a caller-chosen ID. The API layer picks the
// ID once, at the node that first received the request, and reuses it on
// every retry or forward. CREATE_RESOURCE is idempotent per ID in the
// state machine, so retrying across a leader failover can never produce
// two resources for one client request.
func (rm *ResourceManager) CreateWithID(id, kind, spec string) (Resource, error) {
	if kind != "compute" && kind != "network" && kind != "storage" {
		return Resource{}, ErrInvalidKind
	}
	_, err := rm.raft.Propose(Command{
		Op:            "CREATE_RESOURCE",
		ResourceID:    id,
		Kind:          kind,
		Spec:          spec,
		TimestampUnix: time.Now().Unix(),
	})
	if err != nil {
		return Resource{}, err
	}
	if rm.metrics != nil {
		rm.metrics.IncResourcesCreated()
	}
	r, _ := rm.store.Get(id)
	return r, nil
}

// Terminate requests teardown of a resource (RUNNING/PROVISIONING/PENDING
// -> TERMINATING; the reconciler finishes the job asynchronously).
func (rm *ResourceManager) Terminate(id string) (Resource, error) {
	current, ok := rm.store.Get(id)
	if !ok {
		return Resource{}, fmt.Errorf("resource %q not found", id)
	}
	if current.State == StateTerminating {
		return current, nil // already requested (e.g. a retried request): idempotent success
	}
	if !isValidTransition(current.State, StateTerminating) {
		return Resource{}, fmt.Errorf("cannot terminate resource in state %s", current.State)
	}
	_, err := rm.raft.Propose(Command{
		Op:            "TRANSITION_STATE",
		ResourceID:    id,
		State:         StateTerminating,
		Reason:        "terminate requested via API",
		TimestampUnix: time.Now().Unix(),
	})
	if err != nil {
		return Resource{}, err
	}
	r, _ := rm.store.Get(id)
	return r, nil
}

// --- Reconciler: the control-plane orchestration loop ---
//
// This is the piece that makes it a *control plane* rather than just a
// replicated database: a background loop, running only on the current
// leader, that continuously watches resource state and drives it forward
// -- the same "observe, diff, act" reconcile pattern Kubernetes
// controllers use. PENDING resources get provisioned; PROVISIONING
// resources that have been "in progress" long enough transition to
// RUNNING (standing in for the real cloud-provider API call a production
// system would make here); TERMINATING resources finish tearing down.

const (
	simulatedProvisionTime = 3 * time.Second // stand-in for "call the hypervisor/cloud API"
	simulatedTeardownTime  = 2 * time.Second
	reconcileInterval      = 500 * time.Millisecond
)

type Reconciler struct {
	raft    *Raft
	store   *ResourceStore
	metrics *Metrics // nil = metrics disabled
}

func NewReconciler(r *Raft, s *ResourceStore) *Reconciler {
	return &Reconciler{raft: r, store: s}
}

// SetMetrics attaches a metrics recorder after construction.
func (rc *Reconciler) SetMetrics(m *Metrics) {
	rc.metrics = m
}

// Run should be started once in a goroutine. It's safe to run on every
// node: the reconcile loop only takes action when this node IS the
// leader (Propose() rejects everyone else), so followers just spin
// harmlessly checking status until/unless they're elected.
func (rc *Reconciler) Run() {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for range ticker.C {
		if rc.raft.GetStatus().State != "LEADER" {
			continue
		}
		rc.reconcileOnce()
	}
}

func (rc *Reconciler) reconcileOnce() {
	now := time.Now().Unix()

	for _, r := range rc.store.ListByState(StatePending) {
		rc.propose(r.ID, StateProvisioning, "reconciler: starting provisioning")
	}

	for _, r := range rc.store.ListByState(StateProvisioning) {
		if now-r.UpdatedAtUnix >= int64(simulatedProvisionTime.Seconds()) {
			rc.propose(r.ID, StateRunning, "reconciler: provisioning complete")
		}
	}

	for _, r := range rc.store.ListByState(StateTerminating) {
		if now-r.UpdatedAtUnix >= int64(simulatedTeardownTime.Seconds()) {
			rc.propose(r.ID, StateTerminated, "reconciler: teardown complete")
			if rc.metrics != nil {
				rc.metrics.IncResourcesTerminated()
			}
		}
	}
}

func (rc *Reconciler) propose(id, targetState, reason string) {
	_, err := rc.raft.Propose(Command{
		Op:            "TRANSITION_STATE",
		ResourceID:    id,
		State:         targetState,
		Reason:        reason,
		TimestampUnix: time.Now().Unix(),
	})
	if err != nil {
		// Expected occasionally: e.g. a leadership change happened between
		// the leader check and the propose call. Not worth crashing over.
		log.Printf("reconciler: propose %s -> %s failed: %v", id, targetState, err)
	}
}
