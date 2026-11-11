#!/usr/bin/env bash
#
# test/ramp_k8s.sh
#
# Steps client concurrency up (default 100 -> 500) and, at every level,
# checks whether leader failover stays under 1 second WHILE that load is
# running. For each level:
#   1. reset Raft state (fresh, empty log) so levels don't affect each other
#   2. start an in-cluster loadgen pod writing through the Service for 20s
#   3. at t=8s, read the term (any rise before the kill = FALSE elections,
#      i.e. the load alone destabilized the cluster), then delete the leader pod
#   4. record the new leader's self-measured failover time, the longest
#      stretch clients saw with no successful write, and the success rate
#
# Usage: bash test/ramp_k8s.sh            (env: LEVELS="100 200 300 400 500")
#        NOKILL=1 DUR=30s bash test/ramp_k8s.sh   steady-state load only, no leader kill
#
set -uo pipefail
NS=orbitcp
IMAGE=orbitcp:latest
LEVELS="${LEVELS:-100 200 300 400 500}"
DUR="${DUR:-20s}"
NOKILL="${NOKILL:-}"
PODS=(raft-0 raft-1 raft-2 raft-3 raft-4)

status_of() { kubectl -n "$NS" exec "$1" -- wget -qO- -T 1 http://127.0.0.1:8080/status 2>/dev/null; }
json_field() { grep -o "\"$1\":[^,}]*" | head -1 | cut -d: -f2- | tr -d '"'; }
find_leader() {
  for p in "${PODS[@]}"; do
    [[ "${1:-}" == "$p" ]] && continue
    status_of "$p" | grep -q '"state":"LEADER"' && { echo "$p"; return 0; }
  done
  return 1
}
psql_q() { kubectl -n "$NS" exec postgres-0 -- psql -U orbitcp -d orbitcp -qc "$1" >/dev/null 2>&1; }

reset_cluster() {
  kubectl -n "$NS" scale sts raft --replicas=0 >/dev/null
  kubectl -n "$NS" wait --for=delete pod -l app=orbitcp --timeout=120s >/dev/null 2>&1
  psql_q "TRUNCATE raft_log; TRUNCATE raft_meta;"
  kubectl -n "$NS" scale sts raft --replicas=5 >/dev/null
  kubectl -n "$NS" wait --for=condition=Ready pod -l app=orbitcp --timeout=180s >/dev/null
  for _ in $(seq 1 100); do find_leader >/dev/null && return 0; sleep 0.2; done
  echo "ERROR: no leader after reset"; exit 1
}

ROWS=()
for C in $LEVELS; do
  echo "================ concurrency=${C} ================"
  reset_cluster
  sleep 2
  LEADER=$(find_leader)
  TERM0=$(status_of "$LEADER" | json_field term)

  kubectl -n "$NS" delete pod lg-ramp --ignore-not-found --wait=true >/dev/null 2>&1
  kubectl -n "$NS" run lg-ramp --image="$IMAGE" --image-pull-policy=Never --restart=Never \
    --command -- /usr/local/bin/orbitcp loadgen -url http://orbitcp:8080 -c "$C" -d "$DUR" -json >/dev/null
  kubectl -n "$NS" wait --for=condition=Ready pod/lg-ramp --timeout=60s >/dev/null 2>&1
  sleep 8

  LEADER=$(find_leader || echo none)
  TERM1=$(status_of "$LEADER" | json_field term)
  FALSE_ELECTIONS=$(( ${TERM1:-0} - ${TERM0:-0} ))
  echo "  steady load: term ${TERM0} -> ${TERM1} (${FALSE_ELECTIONS} elections with no failure)"
  FAILOVER="n/a"
  if [[ -z "$NOKILL" ]]; then
    echo "  deleting leader ${LEADER}"
    kubectl -n "$NS" delete pod "$LEADER" --grace-period=0 --force >/dev/null 2>&1
    NEW=""
    for _ in $(seq 1 100); do NEW=$(find_leader "$LEADER") && break; sleep 0.05; done
    FAILOVER=$(status_of "$NEW" | json_field last_failover_ms)
    echo "  new leader ${NEW:-none}: failover ${FAILOVER:-?}ms"
  fi

  kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/lg-ramp --timeout=180s >/dev/null
  OUT=$(kubectl -n "$NS" logs pod/lg-ramp)
  kubectl -n "$NS" delete pod lg-ramp --wait=false >/dev/null 2>&1
  echo "$OUT" | grep -E '^(requests|success|latency|longest gap|status codes)' | sed 's/^/  /'
  J=$(echo "$OUT" | grep '^RESULT_JSON=' | cut -d= -f2-)
  ROWS+=("$(printf "%-6s %-9s %-13s %-15s %-12s %-10s %-8s" "$C" \
    "$(echo "$J" | json_field rps | cut -d. -f1)" "${FALSE_ELECTIONS}" "${FAILOVER:-?}" \
    "$(echo "$J" | json_field max_gap_ms)" "$(echo "$J" | json_field success_pct | cut -c1-7)" "$(echo "$J" | json_field p99_ms)")")
done

echo
echo "================ SUMMARY ================"
printf "%-6s %-9s %-13s %-15s %-12s %-10s %-8s\n" conc req/s false-elect failover-ms max-gap-ms success% p99-ms
for r in "${ROWS[@]}"; do echo "$r"; done
