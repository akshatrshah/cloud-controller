package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

var errNodeDown = errors.New("node is down")

// Transport carries Raft RPCs (RequestVote, AppendEntries) between nodes
// over plain HTTP+JSON. A production system would likely use gRPC with
// mTLS; HTTP/JSON keeps this project dependency-free and easy to curl
// during a demo or interview walkthrough.
type Transport struct {
	raft       *Raft
	voteClient *http.Client
	appendCli  *http.Client
	down       atomic.Bool // tests set this to simulate a crashed node: every outgoing RPC fails
}

// Timeouts are sized against the election timeout (300-600ms). A vote RPC
// to a dead peer must give up well before the next election would start.
// AppendEntries gets longer because a follower writes the batch to
// Postgres before replying, but since each peer has its own replicator a
// slow peer never delays heartbeats to the others.
const (
	voteRPCTimeout   = 150 * time.Millisecond
	appendRPCTimeout = 500 * time.Millisecond
)

// sharedHTTPTransport keeps connections to peers alive and pooled. Go's
// default of 2 idle connections per host is far too low once followers
// forward hundreds of concurrent client writes to the leader: every extra
// request would open (and later TIME_WAIT) a fresh TCP connection.
var sharedHTTPTransport = &http.Transport{
	Proxy:               nil,
	DialContext:         (&net.Dialer{Timeout: 100 * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext,
	MaxIdleConns:        2048,
	MaxIdleConnsPerHost: 1024,
	IdleConnTimeout:     90 * time.Second,
}

func NewTransport(r *Raft) *Transport {
	return &Transport{
		raft:       r,
		voteClient: &http.Client{Timeout: voteRPCTimeout, Transport: sharedHTTPTransport},
		appendCli:  &http.Client{Timeout: appendRPCTimeout, Transport: sharedHTTPTransport},
	}
}

type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
	PreVote      bool   `json:"pre_vote,omitempty"` // "would you vote for me?" -- receiver answers without changing its own term or vote
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesReply struct {
	Term         int  `json:"term"`
	Success      bool `json:"success"`
	LastLogIndex int  `json:"last_log_index"` // follower's last index; lets the leader skip straight back on a mismatch
}

func (t *Transport) post(client *http.Client, addr, path string, in, out interface{}) error {
	if t.down.Load() {
		return errNodeDown
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s%s", addr, path)
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body) // drain so the connection can be reused
		return fmt.Errorf("%s%s: HTTP %d", addr, path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (t *Transport) SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	err := t.post(t.voteClient, peer, "/raft/request-vote", args, &reply)
	return reply, err
}

func (t *Transport) SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	err := t.post(t.appendCli, peer, "/raft/append-entries", args, &reply)
	return reply, err
}

// RegisterHandlers wires the receiver-side RPC endpoints onto an HTTP mux.
func (t *Transport) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, req *http.Request) {
		var args RequestVoteArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := t.raft.HandleRequestVote(args)
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, req *http.Request) {
		var args AppendEntriesArgs
		if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := t.raft.HandleAppendEntries(args)
		json.NewEncoder(w).Encode(reply)
	})
}
