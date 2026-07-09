package main

import "testing"

func newTestRaft(id string, peers []string) *Raft {
	r := NewRaft(id, peers, NewResourceStore(), nil) // nil storage: in-memory only for unit tests
	go r.applyLoop()                                 // apply committed entries to the state machine, same as production Run()
	return r
}

func TestQuorumSize(t *testing.T) {
	cases := []struct {
		name    string
		peers   int // OTHER nodes, not counting self
		wantMin int
	}{
		{"single node cluster", 0, 1},
		{"2 nodes total", 1, 2},
		{"3 nodes total", 2, 2},
		{"4 nodes total", 3, 3},
		{"5 nodes total", 4, 3},
		{"7 nodes total", 6, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			peers := make([]string, tc.peers)
			for i := range peers {
				peers[i] = "peer"
			}
			r := newTestRaft("self", peers)
			if got := r.quorum(); got != tc.wantMin {
				t.Errorf("quorum() = %d, want %d (cluster size %d)", got, tc.wantMin, tc.peers+1)
			}
		})
	}
}

func TestRequestVote_GrantsWhenTermHigherAndLogUpToDate(t *testing.T) {
	r := newTestRaft("n1", []string{"n2", "n3"})
	reply := r.HandleRequestVote(RequestVoteArgs{
		Term: 1, CandidateID: "n2", LastLogIndex: 0, LastLogTerm: 0,
	})
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted for equally up-to-date log, term bump")
	}
	if reply.Term != 1 {
		t.Fatalf("expected reply term 1, got %d", reply.Term)
	}
}

func TestRequestVote_RejectsStaleTerm(t *testing.T) {
	r := newTestRaft("n1", []string{"n2"})
	r.currentTerm = 5
	reply := r.HandleRequestVote(RequestVoteArgs{
		Term: 3, CandidateID: "n2", LastLogIndex: 0, LastLogTerm: 0,
	})
	if reply.VoteGranted {
		t.Fatalf("must never grant a vote for a term older than currentTerm")
	}
}

func TestRequestVote_RejectsSecondCandidateSameTerm(t *testing.T) {
	// Safety property: at most one leader per term. A node must not vote
	// twice in the same term for two different candidates.
	r := newTestRaft("n1", []string{"n2", "n3"})
	first := r.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "n2"})
	second := r.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "n3"})
	if !first.VoteGranted {
		t.Fatalf("expected first vote to be granted")
	}
	if second.VoteGranted {
		t.Fatalf("expected second vote in the same term to be REJECTED (already voted for n2)")
	}
}

func TestRequestVote_RejectsOutOfDateLog(t *testing.T) {
	r := newTestRaft("n1", []string{"n2"})
	r.log = append(r.log, LogEntry{Term: 2, Index: 1})
	r.currentTerm = 2
	// Candidate's log is behind ours (lower last log term) -> must reject,
	// otherwise we could elect a leader missing committed entries.
	reply := r.HandleRequestVote(RequestVoteArgs{
		Term: 2, CandidateID: "n2", LastLogIndex: 0, LastLogTerm: 0,
	})
	if reply.VoteGranted {
		t.Fatalf("must not grant vote to a candidate with an out-of-date log")
	}
}

func TestAppendEntries_RejectsLowerTerm(t *testing.T) {
	r := newTestRaft("n1", []string{"n2"})
	r.currentTerm = 5
	reply := r.HandleAppendEntries(AppendEntriesArgs{Term: 2, LeaderID: "n2"})
	if reply.Success {
		t.Fatalf("must reject AppendEntries from a stale-term leader")
	}
}

func TestAppendEntries_RejectsOnLogMismatch(t *testing.T) {
	r := newTestRaft("n1", []string{"n2"})
	r.currentTerm = 1
	// Follower's log is just the sentinel (index 0); leader claims
	// PrevLogIndex=5, which doesn't exist locally -> must reject so the
	// leader backs off nextIndex and retries with an earlier index.
	reply := r.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "n2", PrevLogIndex: 5, PrevLogTerm: 1,
	})
	if reply.Success {
		t.Fatalf("must reject AppendEntries when PrevLogIndex is beyond local log")
	}
}

func TestAppendEntries_AppendsAndCommits(t *testing.T) {
	r := newTestRaft("n1", []string{"n2", "n3"})
	r.currentTerm = 1
	reply := r.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 1, Index: 1, Command: Command{Op: "CREATE_RESOURCE", ResourceID: "compute-abc", Kind: "compute", TimestampUnix: 1000}},
		},
		LeaderCommit: 1,
	})
	if !reply.Success {
		t.Fatalf("expected AppendEntries to succeed")
	}
	if len(r.log) != 2 {
		t.Fatalf("expected log length 2 (sentinel + 1 entry), got %d", len(r.log))
	}
	if r.commitIndex != 1 {
		t.Fatalf("expected commitIndex=1 after LeaderCommit=1, got %d", r.commitIndex)
	}
	if r.leaderID != "n2" {
		t.Fatalf("expected follower to record leaderID=n2, got %q", r.leaderID)
	}
}

func TestAppendEntries_TruncatesConflictingSuffix(t *testing.T) {
	r := newTestRaft("n1", []string{"n2"})
	r.currentTerm = 2
	// Follower has a stale, uncommitted entry at index 1 from term 1.
	r.log = append(r.log, LogEntry{Term: 1, Index: 1, Command: Command{Op: "CREATE_RESOURCE", ResourceID: "stale"}})

	// New leader (term 2) overwrites index 1 with a different entry.
	reply := r.HandleAppendEntries(AppendEntriesArgs{
		Term:         2,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Term: 2, Index: 1, Command: Command{Op: "CREATE_RESOURCE", ResourceID: "correct"}},
		},
	})
	if !reply.Success {
		t.Fatalf("expected success")
	}
	if r.log[1].Command.ResourceID != "correct" {
		t.Fatalf("expected conflicting suffix to be truncated and replaced, got resource_id=%q", r.log[1].Command.ResourceID)
	}
}

func TestBecomeFollower_ResetsVoteAndTerm(t *testing.T) {
	r := newTestRaft("n1", []string{"n2"})
	r.mu.Lock()
	r.state = Leader
	r.currentTerm = 3
	r.votedFor = "n1"
	r.becomeFollower(5, "n2")
	defer r.mu.Unlock()

	if r.state != Follower {
		t.Fatalf("expected state Follower, got %v", r.state)
	}
	if r.currentTerm != 5 {
		t.Fatalf("expected currentTerm=5, got %d", r.currentTerm)
	}
	if r.votedFor != "" {
		t.Fatalf("expected votedFor reset to empty, got %q", r.votedFor)
	}
}

func TestPropose_RejectsWhenNotLeader(t *testing.T) {
	r := newTestRaft("n1", []string{"n2", "n3"})
	// Fresh node starts as Follower.
	_, err := r.Propose(Command{Op: "CREATE_RESOURCE", ResourceID: "x", Kind: "compute"})
	if err == nil {
		t.Fatalf("expected error proposing on a non-leader")
	}
	if _, ok := err.(*NotLeaderError); !ok {
		t.Fatalf("expected *NotLeaderError, got %T", err)
	}
}
