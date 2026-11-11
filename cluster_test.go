package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests run a real 5-node cluster in-process: separate Raft
// instances talking over real HTTP on loopback, with no mocks in the
// consensus or API path. A "crash" makes a node drop every incoming
// request and fail every outgoing RPC, which looks the same to its peers
// as a killed process.

type testNode struct {
	raft  *Raft
	store *ResourceStore
	addr  string
	down  atomic.Bool
}

func (n *testNode) crash() {
	n.down.Store(true)
	n.raft.transport.down.Store(true)
}

func startTestCluster(t *testing.T, size int) []*testNode {
	t.Helper()
	listeners := make([]net.Listener, size)
	addrs := make([]string, size)
	for i := range listeners {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	nodes := make([]*testNode, size)
	for i := range nodes {
		var peers []string
		for j, a := range addrs {
			if j != i {
				peers = append(peers, a)
			}
		}
		store := NewResourceStore()
		r := NewRaft(addrs[i], peers, store, nil)
		node := &testNode{raft: r, store: store, addr: addrs[i]}
		nodes[i] = node

		mux := http.NewServeMux()
		r.transport.RegisterHandlers(mux)
		RegisterAPIHandlers(mux, r, store, NewResourceManager(r, store))
		srv := &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if node.down.Load() {
					panic(http.ErrAbortHandler) // drop the connection, like a dead process
				}
				mux.ServeHTTP(w, req)
			}),
			ErrorLog: log.New(io.Discard, "", 0),
		}
		go srv.Serve(listeners[i])
		t.Cleanup(func() {
			node.crash()
			srv.Close()
		})
	}
	for _, n := range nodes {
		n.raft.Run()
	}
	return nodes
}

// waitForLeader polls every live node until exactly one claims leadership.
func waitForLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *testNode
		count := 0
		for _, n := range nodes {
			if n.down.Load() {
				continue
			}
			if n.raft.GetStatus().State == "LEADER" {
				leader = n
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no single leader elected within %s", timeout)
	return nil
}

var testHTTPClient = &http.Client{
	Timeout:   10 * time.Second,
	Transport: &http.Transport{MaxIdleConns: 2048, MaxIdleConnsPerHost: 1024},
}

func postResource(addr string) (int, error) {
	resp, err := testHTTPClient.Post("http://"+addr+"/resources", "application/json",
		bytes.NewReader([]byte(`{"kind":"compute","spec":"{}"}`)))
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

// TestCluster_LeaderFailoverUnderOneSecond kills the leader of a healthy
// 5-node cluster and measures how long until a new leader exists among
// the survivors. Repeated across several fresh clusters because election
// timing is randomized.
func TestCluster_LeaderFailoverUnderOneSecond(t *testing.T) {
	const trials = 5
	var times []time.Duration
	for trial := 0; trial < trials; trial++ {
		nodes := startTestCluster(t, 5)
		old := waitForLeader(t, nodes, 5*time.Second)
		time.Sleep(300 * time.Millisecond) // let heartbeats settle into steady state

		killedAt := time.Now()
		old.crash()
		newLeader := waitForLeader(t, nodes, 3*time.Second)
		took := time.Since(killedAt)
		times = append(times, took)

		if newLeader == old {
			t.Fatalf("trial %d: crashed node still reported as leader", trial)
		}
		if took >= time.Second {
			t.Errorf("trial %d: failover took %s, want < 1s", trial, took)
		}
		if ms := newLeader.raft.GetStatus().LastFailoverMs; ms <= 0 || ms >= 1000 {
			t.Errorf("trial %d: new leader's own failover metric = %dms, want (0, 1000)", trial, ms)
		}
		t.Logf("trial %d: kill -> new leader in %s (leader-reported gap %dms)",
			trial, took.Round(time.Millisecond), newLeader.raft.GetStatus().LastFailoverMs)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	t.Logf("failover over %d trials: min=%s median=%s max=%s", trials,
		times[0].Round(time.Millisecond), times[trials/2].Round(time.Millisecond), times[trials-1].Round(time.Millisecond))
}

// TestCluster_500ConcurrentWritesAllSucceed sends 500 simultaneous creates
// spread across ALL nodes (so ~80% land on followers and must be
// forwarded) and requires every one to commit, then checks every replica
// converges on all 500.
func TestCluster_500ConcurrentWritesAllSucceed(t *testing.T) {
	nodes := startTestCluster(t, 5)
	waitForLeader(t, nodes, 5*time.Second)

	const clients = 500
	var wg sync.WaitGroup
	var ok atomic.Int64
	failures := make(chan string, clients)
	startGun := make(chan struct{})
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-startGun
			status, err := postResource(nodes[i%len(nodes)].addr)
			if err != nil || status != http.StatusOK {
				failures <- fmt.Sprintf("client %d: status=%d err=%v", i, status, err)
				return
			}
			ok.Add(1)
		}(i)
	}
	t0 := time.Now()
	close(startGun)
	wg.Wait()
	close(failures)
	for f := range failures {
		t.Error(f)
	}
	t.Logf("%d/%d concurrent writes succeeded in %s", ok.Load(), clients, time.Since(t0).Round(time.Millisecond))

	deadline := time.Now().Add(3 * time.Second)
	for _, n := range nodes {
		for len(n.store.List()) < clients && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if got := len(n.store.List()); got != clients {
			t.Errorf("node %s has %d resources, want %d", n.addr, got, clients)
		}
	}
}

// TestCluster_WritesSurviveLeaderFailover keeps writing through the
// followers while the leader is killed. Followers hold each write and
// retry until the new leader is up, so clients see a latency spike
// rather than errors.
func TestCluster_WritesSurviveLeaderFailover(t *testing.T) {
	nodes := startTestCluster(t, 5)
	leader := waitForLeader(t, nodes, 5*time.Second)
	var targets []*testNode
	for _, n := range nodes {
		if n != leader {
			targets = append(targets, n)
		}
	}

	const clients = 50
	var total, ok atomic.Int64
	var maxLatency atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(target *testNode) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				t0 := time.Now()
				status, err := postResource(target.addr)
				if l := int64(time.Since(t0)); l > maxLatency.Load() {
					maxLatency.Store(l)
				}
				total.Add(1)
				if err == nil && status == http.StatusOK {
					ok.Add(1)
				}
			}
		}(targets[i%len(targets)])
	}

	time.Sleep(500 * time.Millisecond)
	leader.crash()
	waitForLeader(t, nodes, 3*time.Second)
	time.Sleep(1 * time.Second)
	close(stop)
	wg.Wait()

	t.Logf("writes during failover: %d/%d succeeded, max latency %s",
		ok.Load(), total.Load(), time.Duration(maxLatency.Load()).Round(time.Millisecond))
	if ok.Load() != total.Load() {
		t.Errorf("%d writes failed during leader failover, want 0", total.Load()-ok.Load())
	}
}
