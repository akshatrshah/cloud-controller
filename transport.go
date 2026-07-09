package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Transport carries Raft RPCs (RequestVote, AppendEntries) between nodes
// over plain HTTP+JSON. A production system would likely use gRPC with
// mTLS; HTTP/JSON keeps this project dependency-free and easy to curl
// during a demo or interview walkthrough.
type Transport struct {
	raft   *Raft
	client *http.Client
}

func NewTransport(r *Raft) *Transport {
	return &Transport{
		raft:   r,
		client: &http.Client{Timeout: 800 * time.Millisecond},
	}
}

type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
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
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

func (t *Transport) post(addr, path string, in, out interface{}) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s%s", addr, path)
	resp, err := t.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func (t *Transport) SendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	err := t.post(peer, "/raft/request-vote", args, &reply)
	return reply, err
}

func (t *Transport) SendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	err := t.post(peer, "/raft/append-entries", args, &reply)
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
