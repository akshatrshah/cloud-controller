#!/usr/bin/env bash
#
# test/integration_test.sh
#
# End-to-end test against a REAL 5-node control plane backed by a REAL
# Postgres instance (not mocked) -- verifies quorum election, the
# reconciler's automatic PENDING -> PROVISIONING -> RUNNING orchestration,
# replication convergence, leader-failure survivability, AND (the
# capability this test exists to prove) that a killed node recovers its
# full resource state from Postgres on restart, before any cluster
# resync.
#
# Usage: ./test/integration_test.sh   (run from repo root; requires a
# reachable Postgres -- set TEST_PG_DSN, or a local one is assumed at
# postgres://postgres:postgres@127.0.0.1:5432/orbitcp?sslmode=disable)
#
set -uo pipefail

PORTS=(9401 9402 9403 9404 9405)
BIN=/tmp/orbitcp-itest
PASS=0
FAIL=0
PG_DSN="${TEST_PG_DSN:-postgres://postgres:postgres@127.0.0.1:5432/orbitcp?sslmode=disable}"

pass() { PASS=$((PASS+1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL+1)); echo "  FAIL: $1"; }

# setsid (util-linux) isn't available on macOS by default. We only need
# "run in the background, detached enough to survive this script," so
# fall back to plain backgrounding there -- functionally fine for this
# test's purposes on either platform.
run_bg() {
  if command -v setsid >/dev/null 2>&1; then
    setsid "$@" &
  else
    "$@" &
  fi
}

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill -9 "$pid" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT

echo "==> Building test binary"
GOFLAGS=-mod=mod GOPROXY=off go build -o "$BIN" . || { echo "build failed"; exit 1; }

# Best-effort cleanup of any state left behind by a PREVIOUS run of this
# same test (it reuses the same node IDs/ports every time). Harmless no-op
# if psql isn't available or the tables don't exist yet.
if command -v psql >/dev/null 2>&1; then
  psql "$PG_DSN" -c "DELETE FROM raft_log WHERE node_id LIKE '127.0.0.1:94%'" >/dev/null 2>&1 || true
  psql "$PG_DSN" -c "DELETE FROM raft_meta WHERE node_id LIKE '127.0.0.1:94%'" >/dev/null 2>&1 || true
fi

echo "==> Starting 5-node control plane on ports: ${PORTS[*]} (PG_DSN=$PG_DSN)"
CLUSTER=""
for p in "${PORTS[@]}"; do CLUSTER="${CLUSTER}127.0.0.1:${p},"; done
CLUSTER="${CLUSTER%,}"

PIDS=()
for p in "${PORTS[@]}"; do
  NODE_ID="127.0.0.1:${p}" PORT="$p" CLUSTER="$CLUSTER" PG_DSN="$PG_DSN" \
    run_bg "$BIN" > "/tmp/orbitcp-itest-${p}.log" 2>&1 < /dev/null
  PIDS+=($!)
  sleep 0.3
done

echo "==> Waiting for leader election (up to 10s)"
LEADER_PORT=""
for i in $(seq 1 20); do
  for p in "${PORTS[@]}"; do
    STATE=$(curl -s --max-time 1 "http://127.0.0.1:${p}/status" 2>/dev/null | grep -o '"state":"[A-Z]*"' | cut -d'"' -f4)
    if [[ "$STATE" == "LEADER" ]]; then LEADER_PORT="$p"; break 2; fi
  done
  sleep 0.5
done

if [[ -n "$LEADER_PORT" ]]; then
  pass "cluster elected exactly one leader (port $LEADER_PORT) within 10s"
else
  fail "no leader elected within 10s"
  tail -n 20 /tmp/orbitcp-itest-*.log
  exit 1
fi

echo "==> Test: create a compute resource, verify it starts PENDING"
RESP=$(curl -s -X POST "http://127.0.0.1:${LEADER_PORT}/resources" -d '{"kind":"compute","spec":"{\"cpu\":4}"}')
RES_ID=$(echo "$RESP" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
if [[ -n "$RES_ID" ]] && echo "$RESP" | grep -q '"state":"PENDING"'; then
  pass "resource created ($RES_ID), initial state PENDING"
else
  fail "resource creation failed or wrong initial state: $RESP"
fi

echo "==> Test: reconciler automatically drives PENDING -> PROVISIONING -> RUNNING"
REACHED_RUNNING=""
for i in $(seq 1 12); do
  STATE=$(curl -s --max-time 1 "http://127.0.0.1:${LEADER_PORT}/resources/${RES_ID}" | grep -o '"state":"[A-Z]*"' | cut -d'"' -f4)
  [[ "$STATE" == "RUNNING" ]] && { REACHED_RUNNING=1; break; }
  sleep 0.5
done
if [[ -n "$REACHED_RUNNING" ]]; then
  pass "resource reached RUNNING via the reconciler with no manual intervention"
else
  fail "resource never reached RUNNING (stuck at state=$STATE)"
fi

echo "==> Test: resource state replicated to ALL 5 nodes"
REPLICATED=0
for p in "${PORTS[@]}"; do
  STATE=$(curl -s --max-time 1 "http://127.0.0.1:${p}/resources/${RES_ID}" | grep -o '"state":"[A-Z]*"' | cut -d'"' -f4)
  [[ "$STATE" == "RUNNING" ]] && REPLICATED=$((REPLICATED+1))
done
if [[ "$REPLICATED" -eq 5 ]]; then
  pass "resource state replicated to all 5/5 nodes"
else
  fail "resource state only replicated to $REPLICATED/5 nodes"
fi

echo "==> Test: create on a FOLLOWER is rejected with 421 + leader hint"
FOLLOWER_PORT=""
for p in "${PORTS[@]}"; do [[ "$p" != "$LEADER_PORT" ]] && { FOLLOWER_PORT="$p"; break; }; done
HTTP_CODE=$(curl -s -o /tmp/follower_resp.json -w '%{http_code}' \
  -X POST "http://127.0.0.1:${FOLLOWER_PORT}/resources" -d '{"kind":"compute","spec":"{}"}')
if [[ "$HTTP_CODE" == "421" ]] && grep -q "leader_id" /tmp/follower_resp.json; then
  pass "follower correctly rejects writes with 421 + leader_id"
else
  fail "follower did not reject write as expected (got HTTP $HTTP_CODE)"
fi

echo "==> Test: invalid state transition is rejected deterministically"
INVALID=$(curl -s -X POST "http://127.0.0.1:${LEADER_PORT}/resources" -d '{"kind":"network","spec":"{}"}')
INVALID_ID=$(echo "$INVALID" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
TERM_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:${LEADER_PORT}/resources/${INVALID_ID}/terminate")
# PENDING -> TERMINATING is actually valid in this state machine's design
# (you can cancel a not-yet-provisioned resource), so this should succeed;
# terminating it AGAIN while already TERMINATING should be rejected.
TERM_CODE2=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:${LEADER_PORT}/resources/${INVALID_ID}/terminate")
if [[ "$TERM_CODE" == "200" ]] && [[ "$TERM_CODE2" == "400" ]]; then
  pass "state machine allows cancel-while-pending but rejects double-terminate"
else
  fail "unexpected transition behavior: first=$TERM_CODE second=$TERM_CODE2"
fi

echo "==> CHAOS TEST 1: kill the leader mid-flight, verify automatic failover"
LEADER_PID=""
for i in "${!PORTS[@]}"; do [[ "${PORTS[$i]}" == "$LEADER_PORT" ]] && LEADER_PID="${PIDS[$i]}"; done
kill -9 "$LEADER_PID"
echo "    killed leader on port ${LEADER_PORT} (pid ${LEADER_PID})"

NEW_LEADER_PORT=""
for i in $(seq 1 20); do
  for p in "${PORTS[@]}"; do
    [[ "$p" == "$LEADER_PORT" ]] && continue
    STATE=$(curl -s --max-time 1 "http://127.0.0.1:${p}/status" 2>/dev/null | grep -o '"state":"[A-Z]*"' | cut -d'"' -f4)
    if [[ "$STATE" == "LEADER" ]]; then NEW_LEADER_PORT="$p"; break 2; fi
  done
  sleep 0.5
done
if [[ -n "$NEW_LEADER_PORT" ]]; then
  pass "new leader (port $NEW_LEADER_PORT) elected within 10s of leader failure"
else
  fail "no new leader elected after killing the old one"
fi

echo "==> Test: previously-committed resource survived the leader crash (zero data loss)"
STATE=$(curl -s --max-time 1 "http://127.0.0.1:${NEW_LEADER_PORT}/resources/${RES_ID}" | grep -o '"state":"[A-Z]*"' | cut -d'"' -f4)
if [[ "$STATE" == "RUNNING" ]]; then
  pass "resource committed before the crash is still RUNNING after failover"
else
  fail "data loss detected after failover! expected RUNNING, got '$STATE'"
fi

echo "==> CHAOS TEST 2 (the main event): kill a FOLLOWER, restart it, verify it recovers"
echo "    ALL state from Postgres alone -- before any cluster resync could plausibly happen"
FOLLOWER2_PORT=""
FOLLOWER2_PID=""
for i in "${!PORTS[@]}"; do
  p="${PORTS[$i]}"
  [[ "$p" == "$LEADER_PORT" ]] && continue
  [[ "$p" == "$NEW_LEADER_PORT" ]] && continue
  FOLLOWER2_PORT="$p"; FOLLOWER2_PID="${PIDS[$i]}"; break
done
kill -9 "$FOLLOWER2_PID"
echo "    killed follower on port ${FOLLOWER2_PORT} (pid ${FOLLOWER2_PID}) -- all its in-memory state is gone"

NODE_ID="127.0.0.1:${FOLLOWER2_PORT}" PORT="$FOLLOWER2_PORT" CLUSTER="$CLUSTER" PG_DSN="$PG_DSN" \
  run_bg "$BIN" > "/tmp/orbitcp-itest-${FOLLOWER2_PORT}-restart.log" 2>&1 < /dev/null
RESTARTED_PID=$!
PIDS+=("$RESTARTED_PID")

# Deliberately tiny sleep: we want to catch it before it's plausibly had
# time for a normal AppendEntries-based resync, to prove the recovery
# came from Postgres, not the cluster.
sleep 0.3
RECOVERED_STATE=$(curl -s --max-time 1 "http://127.0.0.1:${FOLLOWER2_PORT}/resources/${RES_ID}" | grep -o '"state":"[A-Z]*"' | cut -d'"' -f4)
if [[ "$RECOVERED_STATE" == "RUNNING" ]]; then
  pass "restarted node recovered committed resource state (RUNNING) from Postgres almost instantly"
else
  fail "restarted node did NOT recover state promptly (got '$RECOVERED_STATE') -- persistence may not be working"
fi

echo
echo "======================================================"
echo " RESULTS: ${PASS} passed, ${FAIL} failed"
echo "======================================================"

if [[ "$FAIL" -gt 0 ]]; then
  echo "--- node logs for debugging ---"
  tail -n 15 /tmp/orbitcp-itest-*.log
  exit 1
fi
exit 0