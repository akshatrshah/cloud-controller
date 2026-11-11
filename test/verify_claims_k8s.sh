#!/usr/bin/env bash
#
# test/verify_claims_k8s.sh
#
# Measures the project's two headline numbers on the real kind deployment
# (run ./setup.sh or `terraform apply` first):
#
#   1. LOAD:  500 concurrent clients writing through the `orbitcp` Service.
#             Every write goes through Raft and only returns 200 once a
#             quorum has persisted it to Postgres.
#   2. CHAOS: delete the leader pod while clients are writing, N times, and
#             record (a) how long until a new leader took over, as measured
#             by the new leader itself, (b) the longest stretch clients saw
#             with no successful write, and (c) how long the restarted pod
#             took to restore its state from Postgres.
#
# The load generator runs as a pod INSIDE the cluster (`orbitcp loadgen`,
# same image), so the numbers aren't capped by a kubectl port-forward
# tunnel. Note that on kind, the load generator, all 5 nodes and Postgres
# share the same Docker VM's CPUs.
#
# Usage: bash test/verify_claims_k8s.sh
#   env: CONC=500 DUR=30s TRIALS=5 CHAOS_CONC=50
#
set -uo pipefail

NS=orbitcp
IMAGE=orbitcp:latest
CONC="${CONC:-500}"
DUR="${DUR:-30s}"
TRIALS="${TRIALS:-5}"
CHAOS_CONC="${CHAOS_CONC:-50}"
PODS=(raft-0 raft-1 raft-2 raft-3 raft-4)

now_ms() { perl -MTime::HiRes=time -e 'printf("%d\n", time()*1000)'; }
status_of() { kubectl -n "$NS" exec "$1" -- wget -qO- -T 1 http://127.0.0.1:8080/status 2>/dev/null; }
json_field() { grep -o "\"$1\":[^,}]*" | head -1 | cut -d: -f2- | tr -d '"'; }

find_leader() {
  for p in "${PODS[@]}"; do
    [[ "${1:-}" == "$p" ]] && continue
    if status_of "$p" | grep -q '"state":"LEADER"'; then echo "$p"; return 0; fi
  done
  return 1
}

wait_all_ready() {
  kubectl -n "$NS" wait --for=condition=Ready pod -l app=orbitcp --timeout=120s >/dev/null
  for _ in $(seq 1 100); do find_leader >/dev/null && return 0; sleep 0.2; done
  echo "ERROR: no leader"; exit 1
}

# start_loadgen NAME ARGS... : launches an in-cluster loadgen pod (detached)
start_loadgen() {
  local name=$1; shift
  kubectl -n "$NS" delete pod "$name" --ignore-not-found --wait=true >/dev/null 2>&1
  kubectl -n "$NS" run "$name" --image="$IMAGE" --image-pull-policy=Never --restart=Never \
    --labels=app=loadgen --command -- /usr/local/bin/orbitcp loadgen -url http://orbitcp:8080 -json "$@" >/dev/null
  kubectl -n "$NS" wait --for=condition=Ready "pod/$name" --timeout=60s >/dev/null 2>&1 || true
}

finish_loadgen() {
  local name=$1
  kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$name" --timeout=300s >/dev/null
  kubectl -n "$NS" logs "pod/$name"
  kubectl -n "$NS" delete pod "$name" --wait=false >/dev/null 2>&1
}

echo "======================================================"
echo " 0. PRE-FLIGHT"
echo "======================================================"
kubectl -n "$NS" get pods -l app=orbitcp -o wide || exit 1
wait_all_ready
echo "leader: $(find_leader)"
echo "Docker VM CPUs: $(docker info --format '{{.NCPU}}' 2>/dev/null || echo '?')"
echo

echo "======================================================"
echo " 1. LOAD: ${CONC} concurrent clients, ${DUR}, writes via the Service"
echo "======================================================"
start_loadgen lg-load -c "$CONC" -d "$DUR"
LOAD_OUT=$(finish_loadgen lg-load)
echo "$LOAD_OUT" | grep -v '^  t='
LOAD_JSON=$(echo "$LOAD_OUT" | grep '^RESULT_JSON=' | cut -d= -f2-)
echo

echo "======================================================"
echo " 2. CHAOS: delete the leader pod under write load (${TRIALS} trials)"
echo "======================================================"
FAILOVERS=()
GAPS=()
SUCCESS=()
RESTORES=()
for trial in $(seq 1 "$TRIALS"); do
  wait_all_ready
  sleep 2
  LEADER=$(find_leader)
  echo "--- trial ${trial}: leader is ${LEADER}; starting ${CHAOS_CONC} writers ---"
  start_loadgen lg-chaos -c "$CHAOS_CONC" -d 15s
  sleep 5

  KILL_MS=$(now_ms)
  kubectl -n "$NS" delete pod "$LEADER" --grace-period=0 --force >/dev/null 2>&1
  NEW=""
  while (( $(now_ms) - KILL_MS < 10000 )); do
    NEW=$(find_leader "$LEADER") && break
    sleep 0.05
  done
  SEEN_MS=$(( $(now_ms) - KILL_MS ))
  SELF_MS=$(status_of "$NEW" | json_field last_failover_ms)
  echo "    new leader ${NEW}: self-reported failover ${SELF_MS}ms (script saw it after ${SEEN_MS}ms, incl. kubectl exec overhead)"

  OUT=$(finish_loadgen lg-chaos)
  echo "$OUT" | grep -E '^(success|latency|longest gap|status codes)'
  J=$(echo "$OUT" | grep '^RESULT_JSON=' | cut -d= -f2-)

  # The StatefulSet recreates the deleted pod; read how long its restore took.
  kubectl -n "$NS" wait --for=condition=Ready "pod/$LEADER" --timeout=120s >/dev/null
  RESTORE=$(kubectl -n "$NS" logs "pod/$LEADER" 2>/dev/null | grep -o 'restored from Postgres in [0-9]*ms[^"]*' | head -1)
  echo "    restarted ${LEADER}: ${RESTORE:-restore line not found}"

  FAILOVERS+=("$SELF_MS")
  GAPS+=("$(echo "$J" | json_field max_gap_ms)")
  SUCCESS+=("$(echo "$J" | json_field success_pct)")
  RESTORES+=("$(echo "$RESTORE" | grep -o 'in [0-9]*ms' | tr -dc 0-9)")
done

echo
echo "======================================================"
echo " SUMMARY"
echo "======================================================"
echo "load (${CONC} concurrent, ${DUR}): $LOAD_JSON"
echo
printf "%-6s %-22s %-26s %-16s %-14s\n" trial "leader failover (ms)" "client max gap, no 200 (ms)" "write success %" "pod restore (ms)"
for i in "${!FAILOVERS[@]}"; do
  printf "%-6s %-22s %-26s %-16s %-14s\n" "$((i+1))" "${FAILOVERS[$i]}" "${GAPS[$i]}" "${SUCCESS[$i]}" "${RESTORES[$i]}"
done
