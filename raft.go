package main

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// State is the role a node currently plays in the cluster.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// Command is a single state-machine mutation applied once a log entry
// commits. It represents an IaaS control-plane operation: creating a
// compute/network/storage resource, or transitioning it through its
// lifecycle (PENDING -> PROVISIONING -> RUNNING -> TERMINATING ->
// TERMINATED, with FAILED reachable from any non-terminal state).
type Command struct {
	Op            string `json:"op"` // "CREATE_RESOURCE" or "TRANSITION_STATE"
	ResourceID    string `json:"resource_id"`
	Kind          string `json:"kind,omitempty"`   // "compute" | "network" | "storage" (CREATE only)
	Spec          string `json:"spec,omitempty"`   // free-form JSON spec, e.g. {"cpu":4,"memory_gb":16} (CREATE only)
	State         string `json:"state,omitempty"`  // target lifecycle state (TRANSITION only)
	Reason        string `json:"reason,omitempty"` // e.g. failure reason (TRANSITION only)
	TimestampUnix int64  `json:"timestamp_unix"`   // computed ONCE by the proposer; every replica applies this exact value rather than its own time.Now(), so all nodes converge on identical state
}

// LogEntry is one entry in the replicated log.
type LogEntry struct {
	Term    int     `json:"term"`
	Index   int     `json:"index"`
	Command Command `json:"command"`
}

// StateMachine is anything that can deterministically apply committed
// commands. Decoupling Raft from a concrete store keeps the consensus
// engine reusable independent of what domain it's coordinating.
type StateMachine interface {
	Apply(cmd Command)
}

// Storage is the durability layer: persists the state Raft's safety
// properties depend on surviving a crash (currentTerm, votedFor, the log
// itself, and commitIndex) so a restarted node rejoins with its history
// intact instead of as a blank slate. Optional -- a nil Storage means
// in-memory-only operation (fine for tests, not for production).
type Storage interface {
	SaveMeta(term int, votedFor string, commitIndex int) error
	LoadMeta() (term int, votedFor string, commitIndex int, found bool, err error)
	AppendLog(entries []LogEntry) error
	TruncateLogFrom(index int) error
	LoadLog() ([]LogEntry, error)
}

// Raft implements a simplified but functionally faithful subset of the
// Raft consensus algorithm: randomized-timeout leader election and
// majority-quorum log replication. See: Ongaro & Ousterhout, "In Search
// of an Understandable Consensus Algorithm" (2014).
type Raft struct {
	mu sync.Mutex

	id    string   // this node's address, e.g. raft-0.raft-headless:8080
	peers []string // addresses of all OTHER nodes in the cluster

	// Persistent state -- durably written to Storage (if configured)
	// before Raft relies on it, per the Raft paper's safety requirements.
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile state on all servers
	state             State
	commitIndex       int
	lastApplied       int
	lastHeartbeatSeen time.Time
	leaderID          string

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[string]int
	matchIndex map[string]int

	store     StateMachine
	storage   Storage // nil = in-memory only
	transport *Transport

	applyCh chan struct{} // signals the apply loop that commitIndex advanced
}

func NewRaft(id string, peers []string, store StateMachine, storage Storage) *Raft {
	r := &Raft{
		id:                id,
		peers:             peers,
		state:             Follower,
		votedFor:          "",
		log:               []LogEntry{{Term: 0, Index: 0}}, // dummy sentinel at index 0
		lastHeartbeatSeen: time.Now(),
		store:             store,
		storage:           storage,
		nextIndex:         make(map[string]int),
		matchIndex:        make(map[string]int),
		applyCh:           make(chan struct{}, 64),
	}
	r.transport = NewTransport(r)
	return r
}

// RestoreFromStorage loads persisted term/vote/log/commitIndex (if any)
// before the node starts participating in the cluster, and synchronously
// re-applies every entry up to the persisted commitIndex to the state
// machine. Those entries are safe to replay unconditionally: Raft never
// truncates a majority-committed prefix, so anything at or below a
// previously-persisted commitIndex is permanently settled. This is what
// lets a crashed-and-restarted node come back with its resources intact
// immediately, without waiting to hear from a leader first.
func (r *Raft) RestoreFromStorage() error {
	if r.storage == nil {
		return nil
	}
	term, votedFor, commitIndex, found, err := r.storage.LoadMeta()
	if err != nil {
		return err
	}
	log, err := r.storage.LoadLog()
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if found {
		r.currentTerm = term
		r.votedFor = votedFor
	}
	if len(log) > 0 {
		r.log = log
	}
	if commitIndex > 0 && commitIndex < len(r.log) {
		r.commitIndex = commitIndex
		for r.lastApplied < r.commitIndex {
			r.lastApplied++
			r.store.Apply(r.log[r.lastApplied].Command)
		}
	}
	return nil
}

func (r *Raft) persistMetaLocked() {
	if r.storage == nil {
		return
	}
	if err := r.storage.SaveMeta(r.currentTerm, r.votedFor, r.commitIndex); err != nil {
		log.Printf("[%s] WARNING: failed to persist raft metadata: %v", r.id, err)
	}
}

// quorum returns the number of votes/acks needed for a majority of the
// FULL cluster (self + peers).
func (r *Raft) quorum() int {
	return (len(r.peers)+1)/2 + 1
}

func (r *Raft) lastLogIndexTerm() (int, int) {
	last := r.log[len(r.log)-1]
	return last.Index, last.Term
}

// Run starts the election timer and the background apply loop. Call once,
// in a goroutine, after construction (and after RestoreFromStorage).
func (r *Raft) Run() {
	go r.applyLoop()
	go r.electionTimerLoop()
}

func randomElectionTimeout() time.Duration {
	// Deliberately wide (1.5s-3s) so it comfortably exceeds typical
	// pod-to-pod network latency inside a k8s cluster and avoids
	// split-vote storms during CI/kind demos.
	return time.Duration(1500+rand.Intn(1500)) * time.Millisecond
}

func (r *Raft) electionTimerLoop() {
	for {
		timeout := randomElectionTimeout()
		time.Sleep(50 * time.Millisecond)

		r.mu.Lock()
		elapsed := time.Since(r.lastHeartbeatSeen)
		state := r.state
		r.mu.Unlock()

		if state != Leader && elapsed >= timeout {
			r.startElection()
		}
	}
}

// startElection transitions to Candidate, votes for itself, and requests
// votes from every peer in parallel. If it wins a majority (quorum) before
// the term changes, it becomes Leader.
func (r *Raft) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.currentTerm++
	term := r.currentTerm
	r.votedFor = r.id
	r.lastHeartbeatSeen = time.Now()
	r.persistMetaLocked()
	lastIdx, lastTerm := r.lastLogIndexTerm()
	r.mu.Unlock()

	log.Printf("[%s] term=%d starting election, need quorum=%d/%d", r.id, term, r.quorum(), len(r.peers)+1)

	votes := 1 // vote for self
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range r.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			reply, err := r.transport.SendRequestVote(peer, RequestVoteArgs{
				Term:         term,
				CandidateID:  r.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}
			r.mu.Lock()
			if reply.Term > r.currentTerm {
				r.becomeFollower(reply.Term, "")
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == Candidate && r.currentTerm == term && votes >= r.quorum() {
		r.becomeLeader()
	}
}

// becomeLeader must be called with r.mu held.
func (r *Raft) becomeLeader() {
	r.state = Leader
	r.leaderID = r.id
	log.Printf("[%s] *** ELECTED LEADER *** term=%d votes>=quorum", r.id, r.currentTerm)

	lastIdx, _ := r.lastLogIndexTerm()
	for _, p := range r.peers {
		r.nextIndex[p] = lastIdx + 1
		r.matchIndex[p] = 0
	}
	go r.leaderHeartbeatLoop(r.currentTerm)
}

// becomeFollower must be called with r.mu held.
func (r *Raft) becomeFollower(term int, leaderID string) {
	if r.state != Follower {
		log.Printf("[%s] stepping down to FOLLOWER (saw higher term %d)", r.id, term)
	}
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	if leaderID != "" {
		r.leaderID = leaderID
	}
	r.lastHeartbeatSeen = time.Now()
	r.persistMetaLocked()
}

// leaderHeartbeatLoop periodically sends AppendEntries (heartbeat or real
// replication) to every peer while this node remains leader for `term`.
func (r *Raft) leaderHeartbeatLoop(term int) {
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		r.mu.Lock()
		if r.state != Leader || r.currentTerm != term {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
		r.replicateToAllPeers(term)
	}
}

func (r *Raft) replicateToAllPeers(term int) {
	var wg sync.WaitGroup
	for _, peer := range r.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			r.replicateToPeer(peer, term)
		}(peer)
	}
	wg.Wait()
	r.advanceCommitIndex(term)
}

func (r *Raft) replicateToPeer(peer string, term int) {
	r.mu.Lock()
	if r.state != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}
	ni := r.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	prevIdx := ni - 1
	prevTerm := 0
	if prevIdx < len(r.log) && prevIdx >= 0 {
		prevTerm = r.log[prevIdx].Term
	}
	var entries []LogEntry
	if ni < len(r.log) {
		entries = append(entries, r.log[ni:]...)
	}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	reply, err := r.transport.SendAppendEntries(peer, args)
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term, "")
		return
	}
	if r.state != Leader || r.currentTerm != term {
		return
	}
	if reply.Success {
		r.matchIndex[peer] = prevIdx + len(entries)
		r.nextIndex[peer] = r.matchIndex[peer] + 1
	} else {
		if r.nextIndex[peer] > 1 {
			r.nextIndex[peer]--
		}
	}
}

// advanceCommitIndex checks, for each un-committed index, whether a QUORUM
// of matchIndex[] values have reached it. This is the heart of "quorum
// consensus": an entry is only safe to apply once a majority of the
// cluster has durably replicated it.
func (r *Raft) advanceCommitIndex(term int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Leader || r.currentTerm != term {
		return
	}
	for n := len(r.log) - 1; n > r.commitIndex; n-- {
		if r.log[n].Term != r.currentTerm {
			continue // Raft safety: never commit entries from older terms directly
		}
		count := 1 // leader itself has the entry
		for _, p := range r.peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if count >= r.quorum() {
			r.commitIndex = n
			r.persistMetaLocked()
			select {
			case r.applyCh <- struct{}{}:
			default:
			}
			break
		}
	}
}

// applyLoop applies committed-but-not-yet-applied entries to the state
// machine, in order.
func (r *Raft) applyLoop() {
	for range r.applyCh {
		r.mu.Lock()
		for r.lastApplied < r.commitIndex {
			r.lastApplied++
			entry := r.log[r.lastApplied]
			r.mu.Unlock()
			r.store.Apply(entry.Command)
			r.mu.Lock()
		}
		r.mu.Unlock()
	}
}

// Propose is called on the leader to append a new command to the log and
// replicate it. It blocks until the entry is applied to the state machine
// (i.e. committed by quorum) or a timeout elapses. Returns an error if
// this node isn't the leader.
func (r *Raft) Propose(cmd Command) (int, error) {
	r.mu.Lock()
	if r.state != Leader {
		leader := r.leaderID
		r.mu.Unlock()
		return 0, &NotLeaderError{LeaderID: leader}
	}
	entry := LogEntry{
		Term:    r.currentTerm,
		Index:   len(r.log),
		Command: cmd,
	}
	r.log = append(r.log, entry)
	if r.storage != nil {
		if err := r.storage.AppendLog([]LogEntry{entry}); err != nil {
			log.Printf("[%s] WARNING: failed to persist log entry: %v", r.id, err)
		}
	}
	term := r.currentTerm
	idx := entry.Index
	r.mu.Unlock()

	// Trigger immediate replication instead of waiting for the next
	// heartbeat tick, then poll briefly for commit + apply.
	r.replicateToAllPeers(term)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		applied := r.lastApplied >= idx && r.state == Leader && r.currentTerm == term
		stillLeader := r.state == Leader
		r.mu.Unlock()
		if applied {
			return idx, nil
		}
		if !stillLeader {
			return 0, &NotLeaderError{LeaderID: ""}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, &TimeoutError{}
}

type NotLeaderError struct{ LeaderID string }

func (e *NotLeaderError) Error() string { return "not leader" }

type TimeoutError struct{}

func (e *TimeoutError) Error() string { return "commit timeout waiting for quorum" }

// HandleRequestVote implements the RequestVote RPC (receiver side).
func (r *Raft) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term, "")
	}
	reply := RequestVoteReply{Term: r.currentTerm}

	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		return reply
	}

	lastIdx, lastTerm := r.lastLogIndexTerm()
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (r.votedFor == "" || r.votedFor == args.CandidateID) && logOK {
		r.votedFor = args.CandidateID
		r.lastHeartbeatSeen = time.Now()
		r.persistMetaLocked()
		reply.VoteGranted = true
	}
	return reply
}

// HandleAppendEntries implements the AppendEntries RPC (receiver side),
// also used as the heartbeat mechanism.
func (r *Raft) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term, args.LeaderID)
	}
	reply := AppendEntriesReply{Term: r.currentTerm}

	if args.Term < r.currentTerm {
		reply.Success = false
		return reply
	}

	// Valid leader for our term: reset election timer, acknowledge leadership.
	r.state = Follower
	r.leaderID = args.LeaderID
	r.lastHeartbeatSeen = time.Now()

	if args.PrevLogIndex >= len(r.log) || r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return reply
	}

	// Append new entries, truncating any conflicting suffix.
	insertAt := args.PrevLogIndex + 1
	logChanged := false
	for i, e := range args.Entries {
		idx := insertAt + i
		if idx < len(r.log) {
			if r.log[idx].Term != e.Term {
				r.log = r.log[:idx]
				r.log = append(r.log, args.Entries[i:]...)
				logChanged = true
				if r.storage != nil {
					if err := r.storage.TruncateLogFrom(idx); err != nil {
						log.Printf("[%s] WARNING: failed to persist truncate: %v", r.id, err)
					}
					if err := r.storage.AppendLog(args.Entries[i:]); err != nil {
						log.Printf("[%s] WARNING: failed to persist appended entries: %v", r.id, err)
					}
				}
				break
			}
		} else {
			r.log = append(r.log, args.Entries[i:]...)
			logChanged = true
			if r.storage != nil {
				if err := r.storage.AppendLog(args.Entries[i:]); err != nil {
					log.Printf("[%s] WARNING: failed to persist appended entries: %v", r.id, err)
				}
			}
			break
		}
	}
	_ = logChanged

	if args.LeaderCommit > r.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNew {
			r.commitIndex = args.LeaderCommit
		} else {
			r.commitIndex = lastNew
		}
		r.persistMetaLocked()
		select {
		case r.applyCh <- struct{}{}:
		default:
		}
	}

	reply.Success = true
	return reply
}

// Status is a snapshot used by the /status HTTP endpoint.
type Status struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	Term        int    `json:"term"`
	LeaderID    string `json:"leader_id"`
	CommitIndex int    `json:"commit_index"`
	LogLength   int    `json:"log_length"`
	Peers       int    `json:"peers"`
	Quorum      int    `json:"quorum_size"`
	Persistent  bool   `json:"persistent_storage"`
}

func (r *Raft) GetStatus() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		ID:          r.id,
		State:       r.state.String(),
		Term:        r.currentTerm,
		LeaderID:    r.leaderID,
		CommitIndex: r.commitIndex,
		LogLength:   len(r.log),
		Peers:       len(r.peers),
		Quorum:      r.quorum(),
		Persistent:  r.storage != nil,
	}
}
