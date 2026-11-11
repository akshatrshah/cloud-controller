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

// Timing. Failover time is bounded by roughly electionTimeoutMax plus one
// pre-vote and one vote round trip (a few ms inside a cluster), so these
// values target < 1s leader failover.
//
// The usual Raft rule is broadcastTime << electionTimeout << MTBF. Pod-to-pod
// RTT inside Kubernetes is ~1ms, so a 300-600ms window is still ~300x the
// network delay. heartbeatInterval is kept at 1/4 of the minimum timeout, so
// a follower would have to miss 4 heartbeats in a row before it suspects the
// leader. The randomized spread (300ms wide) is what prevents split votes.
const (
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
	heartbeatInterval  = 75 * time.Millisecond
	electionTick       = 10 * time.Millisecond
	maxEntriesPerRPC   = 512 // caps AppendEntries size so a far-behind follower catches up in chunks instead of one giant request that times out
	proposeTimeout     = 3 * time.Second
)

// Command is a single state-machine mutation applied once a log entry
// commits. It represents an IaaS control-plane operation: creating a
// compute/network/storage resource, or transitioning it through its
// lifecycle (PENDING -> PROVISIONING -> RUNNING -> TERMINATING ->
// TERMINATED, with FAILED reachable from any non-terminal state).
type Command struct {
	Op            string `json:"op"` // "CREATE_RESOURCE", "TRANSITION_STATE", or "NOOP" (appended by each new leader)
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
// Raft consensus algorithm: randomized-timeout leader election (with the
// pre-vote extension) and majority-quorum log replication. See: Ongaro &
// Ousterhout, "In Search of an Understandable Consensus Algorithm" (2014),
// and Ongaro's thesis section 9.6 for pre-vote.
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
	lastHeartbeatSeen time.Time // resets the election timer (heartbeats, granted votes, own elections)
	lastLeaderContact time.Time // last valid AppendEntries from a leader; drives pre-vote rejection and the failover metric
	leaderID          string
	persistedIndex    int // highest log index durably written to Storage on THIS node
	persistedCommit   int // commit index last written to Storage

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[string]int
	matchIndex map[string]int
	replicateC map[string]chan struct{} // per-peer "send now" triggers for the current term
	lastAck    map[string]time.Time     // last successful AppendEntries reply per peer (check-quorum)

	lastFailover time.Duration // gap between losing the previous leader and this node taking over (0 = never measured)

	store     StateMachine
	storage   Storage // nil = in-memory only
	transport *Transport
	metrics   *Metrics // nil = metrics disabled

	applyCh   chan struct{} // signals the apply loop that commitIndex advanced
	appliedCh chan struct{} // closed (and replaced) whenever entries are applied or leadership changes; wakes Propose waiters
}

// SetMetrics attaches a metrics recorder after construction. Safe to leave
// unset entirely (all metric calls are nil-checked) -- metrics are an
// observability add-on, never a correctness dependency.
func (r *Raft) SetMetrics(m *Metrics) {
	r.metrics = m
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
		replicateC:        make(map[string]chan struct{}),
		lastAck:           make(map[string]time.Time),
		applyCh:           make(chan struct{}, 1),
		appliedCh:         make(chan struct{}),
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
	r.persistedIndex = len(r.log) - 1
	if commitIndex > 0 && commitIndex < len(r.log) {
		r.commitIndex = commitIndex
		r.persistedCommit = commitIndex
		for r.lastApplied < r.commitIndex {
			r.lastApplied++
			r.store.Apply(r.log[r.lastApplied].Command)
		}
	}
	return nil
}

// persistMetaLocked synchronously writes term/vote (and the current commit
// index). Raft requires term and vote to be durable before the node acts
// on them, so this is called with r.mu held on every term/vote change.
func (r *Raft) persistMetaLocked() {
	if r.storage == nil {
		return
	}
	if err := r.storage.SaveMeta(r.currentTerm, r.votedFor, r.lastApplied); err != nil {
		log.Printf("[%s] WARNING: failed to persist raft metadata: %v", r.id, err)
		return
	}
	r.persistedCommit = r.lastApplied
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

// notifyLocked wakes every goroutine blocked in Propose so it can recheck
// whether its entry was applied (or leadership was lost). r.mu must be held.
func (r *Raft) notifyLocked() {
	close(r.appliedCh)
	r.appliedCh = make(chan struct{})
}

func kick(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default: // a wakeup is already pending; it will pick up this work too
	}
}

// Run starts the election timer and the background apply loop. Call once,
// after RestoreFromStorage and after the HTTP listener is bound.
func (r *Raft) Run() {
	r.mu.Lock()
	// Restoring a long log from Postgres can take longer than an election
	// timeout. Start the clock now, not at construction, so a restarted
	// node gives the current leader a full timeout to reach it first.
	r.lastHeartbeatSeen = time.Now()
	r.mu.Unlock()
	go r.applyLoop()
	go r.electionTimerLoop()
}

func randomElectionTimeout() time.Duration {
	spread := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(spread))
}

// electionTimerLoop draws ONE random timeout per waiting period and checks
// it every electionTick. (Re-drawing on every tick would bias the effective
// timeout toward the minimum and defeat the randomization.)
func (r *Raft) electionTimerLoop() {
	timeout := randomElectionTimeout()
	for {
		time.Sleep(electionTick)

		r.mu.Lock()
		elapsed := time.Since(r.lastHeartbeatSeen)
		state := r.state
		if state == Leader {
			r.checkQuorumLocked()
		}
		r.mu.Unlock()

		if state == Leader || elapsed < timeout {
			continue
		}
		if r.preVote() {
			r.startElection()
		} else {
			// Not enough peers think the leader is gone. Wait another full
			// timeout before asking again rather than hammering them.
			r.mu.Lock()
			r.lastHeartbeatSeen = time.Now()
			r.mu.Unlock()
		}
		timeout = randomElectionTimeout()
	}
}

// preVote asks peers "would you vote for me if I started an election?"
// WITHOUT incrementing this node's term. Peers that heard from a live
// leader within electionTimeoutMin say no. This stops a node that was
// briefly unreachable (e.g. a freshly restarted pod whose peers still have
// a stale DNS entry for it) from bumping the term and forcing a healthy
// leader to step down, which would cause a second, avoidable outage.
func (r *Raft) preVote() bool {
	r.mu.Lock()
	if r.state == Leader {
		r.mu.Unlock()
		return false
	}
	lastIdx, lastTerm := r.lastLogIndexTerm()
	args := RequestVoteArgs{
		Term:         r.currentTerm + 1,
		CandidateID:  r.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
		PreVote:      true,
	}
	r.mu.Unlock()
	return r.collectVotes(args)
}

// startElection transitions to Candidate, votes for itself, and requests
// votes from every peer in parallel. It becomes Leader as soon as a quorum
// says yes, without waiting on slow or dead peers.
func (r *Raft) startElection() {
	r.mu.Lock()
	if r.state == Leader {
		r.mu.Unlock()
		return
	}
	r.state = Candidate
	r.currentTerm++
	term := r.currentTerm
	r.votedFor = r.id
	r.lastHeartbeatSeen = time.Now()
	r.persistMetaLocked()
	lastIdx, lastTerm := r.lastLogIndexTerm()
	r.mu.Unlock()

	if r.metrics != nil {
		r.metrics.IncElectionsStarted()
	}
	log.Printf("[%s] term=%d starting election, need quorum=%d/%d", r.id, term, r.quorum(), len(r.peers)+1)

	won := r.collectVotes(RequestVoteArgs{
		Term:         term,
		CandidateID:  r.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	})

	r.mu.Lock()
	defer r.mu.Unlock()
	if won && r.state == Candidate && r.currentTerm == term {
		r.becomeLeader()
	}
}

// collectVotes sends a (pre-)vote request to every peer in parallel and
// returns true the moment a quorum (counting self) has granted it, or
// false once every peer has answered or failed without reaching one.
func (r *Raft) collectVotes(args RequestVoteArgs) bool {
	need := r.quorum()
	votes := 1 // self
	if votes >= need {
		return true
	}
	results := make(chan bool, len(r.peers)) // buffered: stragglers never block after we return
	for _, peer := range r.peers {
		go func(peer string) {
			reply, err := r.transport.SendRequestVote(peer, args)
			if err != nil {
				results <- false
				return
			}
			if !args.PreVote && reply.Term > args.Term {
				r.mu.Lock()
				if reply.Term > r.currentTerm {
					r.becomeFollower(reply.Term, "")
				}
				r.mu.Unlock()
			}
			results <- reply.VoteGranted
		}(peer)
	}
	for range r.peers {
		if <-results {
			votes++
			if votes >= need {
				return true
			}
		}
	}
	return false
}

// becomeLeader must be called with r.mu held.
func (r *Raft) becomeLeader() {
	r.state = Leader
	r.leaderID = r.id
	if !r.lastLeaderContact.IsZero() {
		r.lastFailover = time.Since(r.lastLeaderContact)
		log.Printf("[%s] *** ELECTED LEADER *** term=%d failover=%dms (since last contact with previous leader)",
			r.id, r.currentTerm, r.lastFailover.Milliseconds())
	} else {
		log.Printf("[%s] *** ELECTED LEADER *** term=%d (initial election)", r.id, r.currentTerm)
	}
	if r.metrics != nil {
		r.metrics.IncElectionsWon()
	}

	// Append a no-op in the new term. Raft only lets a leader commit
	// entries from its OWN term (Figure 8 in the paper), so without this,
	// writes accepted by the previous leader but not yet committed would
	// sit in limbo until the next client write happened to arrive.
	r.log = append(r.log, LogEntry{Term: r.currentTerm, Index: len(r.log), Command: Command{Op: "NOOP"}})
	r.flushLogLocked()

	lastIdx, _ := r.lastLogIndexTerm()
	r.replicateC = make(map[string]chan struct{}, len(r.peers))
	for _, p := range r.peers {
		r.nextIndex[p] = lastIdx // probe from the entry before the no-op
		r.matchIndex[p] = 0
		r.lastAck[p] = time.Now() // grace period: they just voted for us
		ch := make(chan struct{}, 1)
		r.replicateC[p] = ch
		go r.replicator(p, r.currentTerm, ch)
		kick(ch) // announce leadership immediately instead of after the first heartbeat tick
	}
	r.advanceCommitIndexLocked() // single-node cluster commits the no-op right away
}

// checkQuorumLocked makes a leader step down if it hasn't had a successful
// reply from a majority within electionTimeoutMax. A leader cut off from
// the cluster (network partition, or the rest of the cluster moved on)
// would otherwise keep accepting writes it can never commit, holding each
// client until its proposeTimeout. Stepping down fails those writes fast,
// so the API layer retries them on the real leader. r.mu must be held.
func (r *Raft) checkQuorumLocked() {
	acks := 1 // self
	for _, p := range r.peers {
		if time.Since(r.lastAck[p]) < electionTimeoutMax {
			acks++
		}
	}
	if acks >= r.quorum() {
		return
	}
	log.Printf("[%s] term=%d lost contact with a quorum (%d/%d reachable), stepping down", r.id, r.currentTerm, acks, len(r.peers)+1)
	// Same term: keep votedFor as-is, or this node could vote twice in one term.
	r.state = Follower
	r.leaderID = ""
	r.lastHeartbeatSeen = time.Now()
	r.notifyLocked()
}

// becomeFollower must be called with r.mu held.
func (r *Raft) becomeFollower(term int, leaderID string) {
	if r.state != Follower {
		log.Printf("[%s] stepping down to FOLLOWER (saw higher term %d)", r.id, term)
	}
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	// Overwrite even with "": after a term change the old leader is no
	// longer valid, and pointing clients at it would just fail.
	r.leaderID = leaderID
	r.lastHeartbeatSeen = time.Now()
	r.persistMetaLocked()
	r.notifyLocked()
}

// flushLogLocked durably writes every not-yet-persisted entry in ONE
// Postgres transaction. Many concurrent Propose calls append to the
// in-memory log and then each call this. Whichever gets the lock first
// writes the whole pending batch, and the rest find their entries already
// persisted. That turns N client writes into ~1 database round trip
// (group commit), which is what makes 500 concurrent writers workable.
// r.mu must be held; holding it keeps the write ordered with any log
// truncation from a new leader.
func (r *Raft) flushLogLocked() {
	if r.persistedIndex >= len(r.log)-1 {
		return
	}
	if r.storage != nil {
		if err := r.storage.AppendLog(r.log[r.persistedIndex+1:]); err != nil {
			log.Printf("[%s] WARNING: failed to persist log entries: %v", r.id, err)
			return // not durable: don't count ourselves toward quorum for these
		}
	}
	r.persistedIndex = len(r.log) - 1
}

// replicator is the one goroutine per peer that the leader uses to talk to
// that peer for a single term. It sends whenever new entries are proposed
// (trigger) or at least every heartbeatInterval. Exactly one RPC per peer
// is in flight at a time, so a dead or slow peer only delays itself: the
// healthy peers keep getting heartbeats on schedule.
func (r *Raft) replicator(peer string, term int, trigger chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		for {
			more, ok := r.replicateToPeer(peer, term)
			if !ok || !more {
				break
			}
		}
		if !r.isLeaderFor(term) {
			return
		}
		select {
		case <-trigger:
		case <-ticker.C:
		}
	}
}

func (r *Raft) isLeaderFor(term int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Leader && r.currentTerm == term
}

// replicateToPeer sends one AppendEntries (a heartbeat if the peer is
// caught up). Returns more=true if the peer is still behind and another
// RPC should go out immediately, ok=false if the RPC failed.
func (r *Raft) replicateToPeer(peer string, term int) (more, ok bool) {
	r.mu.Lock()
	if r.state != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return false, false
	}
	ni := r.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	if ni > len(r.log) {
		ni = len(r.log)
	}
	prevIdx := ni - 1
	end := len(r.log)
	if end-ni > maxEntriesPerRPC {
		end = ni + maxEntriesPerRPC
	}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  r.log[prevIdx].Term,
		Entries:      append([]LogEntry(nil), r.log[ni:end]...),
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	reply, err := r.transport.SendAppendEntries(peer, args)
	if err != nil {
		return false, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term, "")
		return false, false
	}
	if r.state != Leader || r.currentTerm != term {
		return false, false
	}
	r.lastAck[peer] = time.Now() // any same-term reply proves the peer still follows us
	if reply.Success {
		if match := prevIdx + len(args.Entries); match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
		}
		r.nextIndex[peer] = r.matchIndex[peer] + 1
		r.advanceCommitIndexLocked()
		return r.nextIndex[peer] < len(r.log), true
	}
	// Log mismatch: back up. The follower tells us its last index, so we
	// can jump straight there instead of decrementing one entry per RPC.
	next := ni - 1
	if hint := reply.LastLogIndex + 1; hint < next {
		next = hint
	}
	if next < 1 {
		next = 1
	}
	r.nextIndex[peer] = next
	return true, true
}

// advanceCommitIndexLocked checks, for each un-committed index, whether a
// QUORUM of nodes have durably stored it. This is the heart of "quorum
// consensus": an entry is only safe to apply once a majority of the
// cluster has it. The leader only counts itself once its own Postgres
// write for that entry has landed. r.mu must be held.
func (r *Raft) advanceCommitIndexLocked() {
	if r.state != Leader {
		return
	}
	for n := len(r.log) - 1; n > r.commitIndex; n-- {
		if r.log[n].Term != r.currentTerm {
			break // Raft safety: never commit entries from older terms directly (and every lower index is older still)
		}
		count := 0
		if r.persistedIndex >= n {
			count = 1
		}
		for _, p := range r.peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if count >= r.quorum() {
			r.commitIndex = n
			kick(r.applyCh)
			return
		}
	}
}

// applyLoop applies committed-but-not-yet-applied entries to the state
// machine, in order, then records progress in Postgres once per batch
// (not once per entry) and wakes any Propose callers waiting on them.
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
		if r.storage != nil && r.lastApplied > r.persistedCommit {
			if err := r.storage.SaveMeta(r.currentTerm, r.votedFor, r.lastApplied); err != nil {
				log.Printf("[%s] WARNING: failed to persist commit index: %v", r.id, err)
			} else {
				r.persistedCommit = r.lastApplied
			}
		}
		r.notifyLocked()
		r.mu.Unlock()
	}
}

// Propose is called on the leader to append a new command to the log and
// replicate it. It blocks until the entry is applied to the state machine
// (i.e. committed by quorum) or a timeout elapses.
//
// Errors: *NotLeaderError means nothing was written and the caller may
// safely retry on the real leader. *LeadershipLostError and *TimeoutError
// mean the entry was appended but its fate is unknown (it may still
// commit), so blindly retrying could apply it twice.
func (r *Raft) Propose(cmd Command) (int, error) {
	r.mu.Lock()
	if r.state != Leader {
		leader := r.leaderID
		r.mu.Unlock()
		if r.metrics != nil {
			r.metrics.IncProposalsFailed()
		}
		return 0, &NotLeaderError{LeaderID: leader}
	}
	term := r.currentTerm
	idx := len(r.log)
	r.log = append(r.log, LogEntry{Term: term, Index: idx, Command: cmd})
	triggers := make([]chan struct{}, 0, len(r.replicateC))
	for _, ch := range r.replicateC {
		triggers = append(triggers, ch)
	}
	r.mu.Unlock()

	// Ship to followers in parallel with our own disk write (allowed by
	// Raft: the leader just can't count itself until its write lands).
	for _, ch := range triggers {
		kick(ch)
	}
	r.mu.Lock()
	if r.state == Leader && r.currentTerm == term {
		r.flushLogLocked()
		r.advanceCommitIndexLocked()
	}
	r.mu.Unlock()

	timer := time.NewTimer(proposeTimeout)
	defer timer.Stop()
	for {
		r.mu.Lock()
		if r.lastApplied >= idx {
			ours := r.log[idx].Term == term
			r.mu.Unlock()
			if ours {
				if r.metrics != nil {
					r.metrics.IncProposalsSucceeded()
				}
				return idx, nil
			}
			if r.metrics != nil {
				r.metrics.IncProposalsFailed()
			}
			return 0, &LeadershipLostError{}
		}
		if r.state != Leader || r.currentTerm != term {
			r.mu.Unlock()
			if r.metrics != nil {
				r.metrics.IncProposalsFailed()
			}
			return 0, &LeadershipLostError{}
		}
		wake := r.appliedCh
		r.mu.Unlock()

		select {
		case <-wake:
		case <-timer.C:
			if r.metrics != nil {
				r.metrics.IncProposalsFailed()
			}
			return 0, &TimeoutError{}
		}
	}
}

type NotLeaderError struct{ LeaderID string }

func (e *NotLeaderError) Error() string { return "not leader" }

type LeadershipLostError struct{}

func (e *LeadershipLostError) Error() string {
	return "leadership changed before the write committed; outcome unknown"
}

type TimeoutError struct{}

func (e *TimeoutError) Error() string { return "commit timeout waiting for quorum" }

// HandleRequestVote implements the RequestVote RPC (receiver side),
// including pre-vote requests.
func (r *Raft) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.PreVote {
		// Answer the hypothetical without changing any of our own state.
		reply := RequestVoteReply{Term: r.currentTerm}
		if args.Term < r.currentTerm || r.state == Leader {
			return reply
		}
		if r.leaderID != "" && time.Since(r.lastLeaderContact) < electionTimeoutMin {
			return reply // our leader is alive as far as we can tell
		}
		lastIdx, lastTerm := r.lastLogIndexTerm()
		reply.VoteGranted = args.LastLogTerm > lastTerm ||
			(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
		return reply
	}

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
	reply := AppendEntriesReply{Term: r.currentTerm, LastLogIndex: len(r.log) - 1}

	if args.Term < r.currentTerm {
		reply.Success = false
		return reply
	}

	// Valid leader for our term: reset election timer, acknowledge leadership.
	if r.state != Follower {
		r.state = Follower
		r.notifyLocked()
	}
	r.leaderID = args.LeaderID
	now := time.Now()
	r.lastHeartbeatSeen = now
	r.lastLeaderContact = now

	if args.PrevLogIndex >= len(r.log) || r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		if args.PrevLogIndex < len(r.log) {
			reply.LastLogIndex = args.PrevLogIndex - 1 // our entry there conflicts too
		}
		return reply
	}

	// Skip entries we already have; find the first new or conflicting one.
	insertAt := args.PrevLogIndex + 1
	i := 0
	for ; i < len(args.Entries); i++ {
		idx := insertAt + i
		if idx >= len(r.log) || r.log[idx].Term != args.Entries[i].Term {
			break
		}
	}
	if i < len(args.Entries) {
		idx := insertAt + i
		newEntries := args.Entries[i:]
		if r.storage != nil {
			// Durable BEFORE we acknowledge, or the leader could count a
			// vote toward quorum for an entry that vanishes on our crash.
			var err error
			if idx < len(r.log) {
				err = r.storage.TruncateLogFrom(idx)
			}
			if err == nil {
				err = r.storage.AppendLog(newEntries)
			}
			if err != nil {
				log.Printf("[%s] WARNING: failed to persist appended entries: %v", r.id, err)
				if idx < len(r.log) {
					r.log = r.log[:idx]
				}
				if r.persistedIndex > len(r.log)-1 {
					r.persistedIndex = len(r.log) - 1
				}
				reply.LastLogIndex = len(r.log) - 1
				reply.Success = false
				return reply
			}
		}
		r.log = append(r.log[:idx], newEntries...)
		r.persistedIndex = len(r.log) - 1
	}

	if args.LeaderCommit > r.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNew {
			r.commitIndex = args.LeaderCommit
		} else {
			r.commitIndex = lastNew
		}
		kick(r.applyCh)
	}

	reply.LastLogIndex = len(r.log) - 1
	reply.Success = true
	return reply
}

// Status is a snapshot used by the /status HTTP endpoint.
type Status struct {
	ID             string `json:"id"`
	State          string `json:"state"`
	Term           int    `json:"term"`
	LeaderID       string `json:"leader_id"`
	CommitIndex    int    `json:"commit_index"`
	LogLength      int    `json:"log_length"`
	Peers          int    `json:"peers"`
	Quorum         int    `json:"quorum_size"`
	Persistent     bool   `json:"persistent_storage"`
	LastFailoverMs int64  `json:"last_failover_ms"` // set on a node that won an election after a previous leader failed
}

func (r *Raft) GetStatus() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		ID:             r.id,
		State:          r.state.String(),
		Term:           r.currentTerm,
		LeaderID:       r.leaderID,
		CommitIndex:    r.commitIndex,
		LogLength:      len(r.log),
		Peers:          len(r.peers),
		Quorum:         r.quorum(),
		Persistent:     r.storage != nil,
		LastFailoverMs: r.lastFailover.Milliseconds(),
	}
}
