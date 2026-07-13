#!/usr/bin/env bash
#
# test/load_test.sh
#
# A heavier, more comprehensive load test than the one-off `hey` commands:
#   1. Escalating write concurrency (find where throughput/latency bends)
#   2. Escalating read concurrency
#   3. Sustained mixed read/write workload
#   4. CHAOS UNDER LOAD: kill the leader pod mid-traffic, measure real
#      availability (% of requests that succeeded) during failover --
#      the most interesting number this script produces.
#
# Requires: `hey` (brew install hey), `kubectl` port-forwarded to the
# LEADER pod on localhost:8080 (see README/earlier chat for why it must
# be the leader specifically, not just any pod behind the Service).
#
# Usage: bash test/load_test.sh
#
set -uo pipefail

BASE_URL="http://127.0.0.1:8080"
NAMESPACE="orbitcp"

echo "======================================================"
echo " 0. PRE-FLIGHT CHECK"
echo "======================================================"
if ! command -v hey >/dev/null 2>&1; then
  echo "ERROR: 'hey' not found. Install with: brew install hey"
  exit 1
fi

STATUS=$(curl -s --max-time 3 "${BASE_URL}/status")
if [[ -z "$STATUS" ]]; then
  echo "ERROR: can't reach ${BASE_URL}/status -- is port-forward running?"
  echo "  kubectl -n ${NAMESPACE} port-forward svc/orbitcp 8080:8080 &"
  exit 1
fi
echo "$STATUS"
IS_LEADER=$(echo "$STATUS" | grep -o '"state":"[A-Za-z]*"' | grep -c "Leader" || true)
if [[ "$IS_LEADER" -eq 0 ]]; then
  LEADER_ID=$(echo "$STATUS" | grep -o '"leader_id":"[^"]*"' | cut -d'"' -f4)
  echo "WARNING: the pod on :8080 is NOT the leader (leader is $LEADER_ID)."
  echo "  Write tests below will fail with 421s. Port-forward the leader pod instead:"
  echo "  kubectl -n ${NAMESPACE} port-forward pod/<leader-pod-name> 8080:8080 &"
  echo "  (continuing anyway -- read tests will still be valid)"
fi
echo

echo "======================================================"
echo " 1. ESCALATING WRITE CONCURRENCY"
echo "    (10s per level -- watch where req/sec plateaus or"
echo "    p99 latency climbs sharply, that's your practical ceiling)"
echo "======================================================"
for c in 5 10 20 50 100; do
  echo "--- concurrency=${c} ---"
  hey -z 10s -c "${c}" -m POST -d '{"kind":"compute","spec":"{}"}' "${BASE_URL}/resources" \
    | grep -E "Requests/sec|Total:|50%|90%|95%|99%|\[200\]|\[421\]|\[50" || true
  echo
done

echo "======================================================"
echo " 2. ESCALATING READ CONCURRENCY"
echo "    (single-resource reads -- fixed payload size, unlike the"
echo "    full /resources list which grows and skews latency)"
echo "======================================================"
SAMPLE_ID=$(curl -s -X POST "${BASE_URL}/resources" -d '{"kind":"compute","spec":"{}"}' | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
echo "using resource $SAMPLE_ID for read tests"
for c in 20 50 100 200; do
  echo "--- concurrency=${c} ---"
  hey -z 10s -c "${c}" "${BASE_URL}/resources/${SAMPLE_ID}" \
    | grep -E "Requests/sec|Total:|50%|90%|95%|99%|\[200\]" || true
  echo
done

echo "======================================================"
echo " 3. SUSTAINED MIXED WORKLOAD (30s, writes + reads concurrently)"
echo "    (realistic traffic shape: most systems aren't 100% one or the other)"
echo "======================================================"
hey -z 30s -c 15 -m POST -d '{"kind":"compute","spec":"{}"}' "${BASE_URL}/resources" > /tmp/loadtest-mixed-writes.txt 2>&1 &
WRITE_PID=$!
hey -z 30s -c 30 "${BASE_URL}/resources/${SAMPLE_ID}" > /tmp/loadtest-mixed-reads.txt 2>&1 &
READ_PID=$!
wait "$WRITE_PID" "$READ_PID"
echo "--- writes (15 concurrent) ---"
grep -E "Requests/sec|Total:|50%|90%|95%|99%|\[200\]|\[421\]" /tmp/loadtest-mixed-writes.txt || true
echo "--- reads (30 concurrent) ---"
grep -E "Requests/sec|Total:|50%|90%|95%|99%|\[200\]" /tmp/loadtest-mixed-reads.txt || true
echo

echo "======================================================"
echo " 4. CHAOS UNDER LOAD (the main event)"
echo "    Start a sustained write load, kill the leader pod MID-TRAFFIC,"
echo "    measure real availability (% success) while failover happens."
echo "======================================================"
CURRENT_LEADER_ID=$(curl -s "${BASE_URL}/status" | grep -o '"leader_id":"[^"]*"' | cut -d'"' -f4)
CURRENT_LEADER_POD=$(echo "$CURRENT_LEADER_ID" | cut -d'.' -f1)
echo "current leader pod: ${CURRENT_LEADER_POD}"

if [[ -z "$CURRENT_LEADER_POD" ]]; then
  echo "SKIPPING: could not determine leader pod name"
else
  echo "starting 25s write load (10 concurrent) against the Service (round-robins across pods)..."
  # NOTE: deliberately hits the Service, not the pinned port-forward, so
  # this measures what a REAL client load-balanced across the cluster
  # would experience during a leader failure -- some requests will
  # naturally land on non-leader pods and get 421s even before the kill,
  # which is realistic and part of the honest picture, not a bug in the test.
  kubectl -n "${NAMESPACE}" port-forward svc/orbitcp 8091:8080 > /tmp/loadtest-chaos-pf.log 2>&1 &
  CHAOS_PF_PID=$!
  sleep 2

  hey -z 25s -c 10 -m POST -d '{"kind":"compute","spec":"{}"}' http://127.0.0.1:8091/resources > /tmp/loadtest-chaos.txt 2>&1 &
  CHAOS_LOAD_PID=$!

  sleep 8
  echo ">>> killing leader pod ${CURRENT_LEADER_POD} NOW (load test still running) <<<"
  KILL_START_MS=$(($(date +%s%N) / 1000000))
  kubectl -n "${NAMESPACE}" delete pod "${CURRENT_LEADER_POD}" --grace-period=0 --force > /dev/null 2>&1

  # Poll for new leader while the load test continues in the background
  NEW_LEADER=""
  for i in $(seq 1 30); do
    STATE=$(curl -s --max-time 1 "${BASE_URL}/status" 2>/dev/null | grep -o '"state":"[A-Za-z]*"' | cut -d'"' -f4)
    LID=$(curl -s --max-time 1 "${BASE_URL}/status" 2>/dev/null | grep -o '"leader_id":"[^"]*"' | cut -d'"' -f4)
    if [[ -n "$LID" ]] && [[ "$LID" != "$CURRENT_LEADER_ID" ]]; then
      NEW_LEADER="$LID"
      break
    fi
    sleep 0.3
  done
  KILL_END_MS=$(($(date +%s%N) / 1000000))
  if [[ -n "$NEW_LEADER" ]]; then
    echo ">>> new leader ($NEW_LEADER) confirmed $((KILL_END_MS - KILL_START_MS))ms after kill <<<"
  else
    echo ">>> WARNING: no new leader confirmed within 9s <<<"
  fi

  wait "$CHAOS_LOAD_PID"
  kill -9 "$CHAOS_PF_PID" >/dev/null 2>&1 || true

  echo
  echo "--- chaos load test results (25s, spans the leader kill) ---"
  cat /tmp/loadtest-chaos.txt | grep -E "Requests/sec|Total:|50%|90%|95%|99%"
  echo "status code breakdown (200 = success, 421 = hit a non-leader, other = real failure):"
  grep -A5 "Status code distribution" /tmp/loadtest-chaos.txt || true
fi

echo
echo "======================================================"
echo " DONE. Key numbers to pull out for your resume:"
echo "   - Section 1: write req/sec + p99 at your chosen concurrency"
echo "   - Section 2: read req/sec + p99 (single-resource, apples-to-apples)"
echo "   - Section 3: sustained mixed-workload numbers"
echo "   - Section 4: ms from leader kill to new leader confirmed,"
echo "     plus % of requests that still succeeded (200s) during that window"
echo "======================================================"
