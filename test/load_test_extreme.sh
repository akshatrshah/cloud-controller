#!/usr/bin/env bash
#
# test/load_test_extreme.sh
#
# Two goals, kept separate on purpose:
#   1. Find the regime where numbers genuinely look good (low concurrency
#      reads, which skip the consensus round-trip entirely).
#   2. Find the actual breaking point -- push concurrency until errors
#      appear or latency stops being linear, so "the system's ceiling"
#      is a measured fact instead of a guess.
#
# Requires: `hey`, kubectl port-forwarded to the LEADER pod on :8080.
#
# Usage: bash test/load_test_extreme.sh
#
set -uo pipefail

BASE_URL="http://127.0.0.1:8080"

echo "======================================================"
echo " PRE-FLIGHT"
echo "======================================================"
if ! command -v hey >/dev/null 2>&1; then
  echo "ERROR: 'hey' not found. Install with: brew install hey"
  exit 1
fi
STATUS=$(curl -s --max-time 3 "${BASE_URL}/status")
if [[ -z "$STATUS" ]]; then
  echo "ERROR: can't reach ${BASE_URL}/status -- is port-forward running?"
  exit 1
fi
echo "$STATUS"
if ! echo "$STATUS" | grep -q '"state":"Leader"'; then
  echo "WARNING: not connected to the leader -- writes below will fail with 421s."
fi

# Seed one resource to read repeatedly (fixed payload size -- this is
# what makes read latency numbers clean and comparable, unlike the
# /resources list endpoint which grows and skews its own latency).
SAMPLE_ID=$(curl -s -X POST "${BASE_URL}/resources" -d '{"kind":"compute","spec":"{}"}' | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
echo "seed resource for reads: $SAMPLE_ID"
echo

echo "======================================================"
echo " PART A: WHERE THE NUMBERS LOOK GOOD"
echo "         (single-resource reads, no consensus round-trip,"
echo "          low-to-moderate concurrency)"
echo "======================================================"
for c in 1 5 10 20; do
  echo "--- reads, concurrency=${c} ---"
  hey -z 8s -c "${c}" -t 5 "${BASE_URL}/resources/${SAMPLE_ID}" \
    | grep -E "Requests/sec|Average|Fastest|50%|90%|95%|99%|\[200\]|\[50" 2>/dev/null
  echo
done

echo "======================================================"
echo " PART B: PUSHING WRITES TO FIND THE ACTUAL CEILING"
echo "         (watch for: req/sec stops climbing, p99 grows"
echo "          faster than concurrency, or errors appear)"
echo "======================================================"
for c in 100 200 400 800; do
  echo "--- writes, concurrency=${c} ---"
  hey -z 12s -c "${c}" -t 10 -m POST -d '{"kind":"compute","spec":"{}"}' "${BASE_URL}/resources" \
    > /tmp/loadtest-extreme-w${c}.txt 2>&1
  grep -E "Requests/sec|Average|Slowest|50%|90%|95%|99%" /tmp/loadtest-extreme-w${c}.txt
  echo "status codes:"
  grep -A10 "Status code distribution" /tmp/loadtest-extreme-w${c}.txt
  ERROR_COUNT=$(grep -oE "\[(0|[45][0-9][0-9])\] [0-9]+ responses" /tmp/loadtest-extreme-w${c}.txt | grep -v "\[421\]" | awk '{sum+=$2} END {print sum+0}')
  echo "non-421 errors: ${ERROR_COUNT:-0}"
  echo
done

echo "======================================================"
echo " PART C: PUSHING READS TO FIND THE ACTUAL CEILING"
echo "======================================================"
for c in 200 500 1000; do
  echo "--- reads, concurrency=${c} ---"
  hey -z 12s -c "${c}" -t 10 "${BASE_URL}/resources/${SAMPLE_ID}" \
    > /tmp/loadtest-extreme-r${c}.txt 2>&1
  grep -E "Requests/sec|Average|Slowest|50%|90%|95%|99%" /tmp/loadtest-extreme-r${c}.txt
  echo "status codes:"
  grep -A10 "Status code distribution" /tmp/loadtest-extreme-r${c}.txt
  echo
done

echo "======================================================"
echo " DONE."
echo "   Part A = your 'good numbers' for the resume (fast, low concurrency reads)"
echo "   Parts B/C = your measured ceiling (where throughput plateaus / errors start)"
echo "======================================================"
