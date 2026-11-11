package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// runLoadgen is a small closed-loop load generator (like `hey`), built into
// the same binary so it can run as a pod INSIDE the cluster:
//
//	kubectl -n orbitcp run loadgen --rm -i --restart=Never --image=orbitcp:latest \
//	  --image-pull-policy=IfNotPresent --command -- /usr/local/bin/orbitcp loadgen -c 500 -d 30s
//
// Running in-cluster matters: from a laptop, every request goes through a
// single `kubectl port-forward` tunnel, which caps out long before the
// control plane does and makes the client the bottleneck being measured.
//
// Besides the usual throughput/latency numbers it reports the longest
// stretch with no successful response (the client-visible outage during
// a failover) and a per-second timeline, which is what the chaos test reads.
func runLoadgen(args []string) {
	fs := flag.NewFlagSet("loadgen", flag.ExitOnError)
	url := fs.String("url", "http://orbitcp:8080", "base URL of the control plane (any node or the Service)")
	conc := fs.Int("c", 500, "concurrent clients, each with one request in flight at a time")
	dur := fs.Duration("d", 30*time.Second, "test duration")
	mode := fs.String("mode", "write", "write (POST /resources) | read (GET /resources/{id})")
	timeout := fs.Duration("timeout", 10*time.Second, "per-request timeout")
	jsonOut := fs.Bool("json", false, "also print a one-line JSON summary (prefixed RESULT_JSON=)")
	fs.Parse(args)

	client := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConns:        *conc * 2,
			MaxIdleConnsPerHost: *conc * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	readID := ""
	if *mode == "read" {
		res, err := postCreate(client, *url)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not seed a resource for reads: %v\n", err)
			os.Exit(1)
		}
		readID = res
	}

	type sample struct {
		end     time.Time
		latency time.Duration
		status  int // 0 = transport error
	}
	var (
		mu      sync.Mutex
		samples = make([]sample, 0, 1<<16)
		errs    = map[string]int{}
		stop    atomic.Bool
		wg      sync.WaitGroup
	)

	start := time.Now()
	time.AfterFunc(*dur, func() { stop.Store(true) })
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				t0 := time.Now()
				var resp *http.Response
				var err error
				if *mode == "read" {
					resp, err = client.Get(*url + "/resources/" + readID)
				} else {
					resp, err = client.Post(*url+"/resources", "application/json",
						bytes.NewReader([]byte(`{"kind":"compute","spec":"{\"cpu\":2}"}`)))
				}
				status := 0
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					status = resp.StatusCode
				}
				s := sample{end: time.Now(), latency: time.Since(t0), status: status}
				mu.Lock()
				samples = append(samples, s)
				if err != nil {
					errs[shortErr(err)]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// --- summarize ---
	sort.Slice(samples, func(i, j int) bool { return samples[i].end.Before(samples[j].end) })
	codes := map[int]int{}
	var ok int
	lat := make([]time.Duration, 0, len(samples))
	var maxGap time.Duration
	lastOK := start
	perSec := map[int][2]int{} // second -> [ok, failed]
	for _, s := range samples {
		codes[s.status]++
		lat = append(lat, s.latency)
		sec := int(s.end.Sub(start).Seconds())
		c := perSec[sec]
		if s.status == http.StatusOK {
			ok++
			c[0]++
			if g := s.end.Sub(lastOK); g > maxGap {
				maxGap = g
			}
			lastOK = s.end
		} else {
			c[1]++
		}
		perSec[sec] = c
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*p)]
	}
	total := len(samples)
	successPct := 0.0
	if total > 0 {
		successPct = 100 * float64(ok) / float64(total)
	}

	fmt.Printf("mode=%s concurrency=%d duration=%s url=%s\n", *mode, *conc, elapsed.Round(time.Millisecond), *url)
	fmt.Printf("requests:   %d  (%.0f req/s)\n", total, float64(total)/elapsed.Seconds())
	fmt.Printf("success:    %d / %d = %.3f%%\n", ok, total, successPct)
	fmt.Printf("latency:    p50=%s p90=%s p99=%s max=%s\n", ms(pct(0.50)), ms(pct(0.90)), ms(pct(0.99)), ms(pct(1)))
	fmt.Printf("longest gap with no successful response: %s\n", ms(maxGap))
	fmt.Printf("status codes:")
	keys := make([]int, 0, len(codes))
	for k := range codes {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		label := fmt.Sprint(k)
		if k == 0 {
			label = "conn-error"
		}
		fmt.Printf(" [%s]=%d", label, codes[k])
	}
	fmt.Println()
	for e, n := range errs {
		fmt.Printf("  error x%d: %s\n", n, e)
	}
	fmt.Println("timeline (second: ok/failed):")
	for sec := 0; sec <= int(elapsed.Seconds()); sec++ {
		c := perSec[sec]
		fmt.Printf("  t=%02ds  %6d ok  %4d failed\n", sec, c[0], c[1])
	}

	if *jsonOut {
		out, _ := json.Marshal(map[string]interface{}{
			"mode": *mode, "concurrency": *conc, "requests": total, "success": ok,
			"success_pct": successPct, "rps": float64(total) / elapsed.Seconds(),
			"p50_ms": pct(0.50).Milliseconds(), "p99_ms": pct(0.99).Milliseconds(), "max_ms": pct(1).Milliseconds(),
			"max_gap_ms": maxGap.Milliseconds(),
		})
		fmt.Printf("RESULT_JSON=%s\n", out)
	}
}

func postCreate(client *http.Client, base string) (string, error) {
	resp, err := client.Post(base+"/resources", "application/json", bytes.NewReader([]byte(`{"kind":"compute","spec":"{}"}`)))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var r Resource
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil || r.ID == "" {
		return "", fmt.Errorf("HTTP %d, decode: %v", resp.StatusCode, err)
	}
	return r.ID, nil
}

func ms(d time.Duration) string { return fmt.Sprintf("%dms", d.Milliseconds()) }

func shortErr(err error) string {
	s := err.Error()
	if len(s) > 120 {
		s = s[:120]
	}
	return s
}
