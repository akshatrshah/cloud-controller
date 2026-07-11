#!/usr/bin/env bash
#
# setup.sh — test, build, deploy, and smoke-test Orbit (a distributed IaaS
# control plane) on a local kind (Kubernetes-in-Docker) cluster.
#
# Usage:
#   ./setup.sh            # full pipeline: test + build + deploy + smoke test
#   ./setup.sh teardown   # delete the kind cluster and clean up
#
set -euo pipefail

CLUSTER_NAME="orbitcp-demo"
NAMESPACE="orbitcp"
IMAGE_NAME="orbitcp:latest"

c_green()  { printf "\033[0;32m%s\033[0m\n" "$1"; }
c_yellow() { printf "\033[0;33m%s\033[0m\n" "$1"; }
c_red()    { printf "\033[0;31m%s\033[0m\n" "$1"; }

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    c_red "Missing dependency: $1"
    case "$1" in
      docker) echo "  Install: https://docs.docker.com/get-docker/" ;;
      kind)   echo "  Install: go install sigs.k8s.io/kind@latest   (or) brew install kind" ;;
      kubectl) echo "  Install: https://kubernetes.io/docs/tasks/tools/" ;;
      go)     echo "  Install: https://go.dev/doc/install" ;;
    esac
    exit 1
  fi
}

teardown() {
  c_yellow "Tearing down kind cluster '${CLUSTER_NAME}'..."
  kind delete cluster --name "${CLUSTER_NAME}" || true
  c_green "Done."
  exit 0
}

if [[ "${1:-}" == "teardown" ]]; then
  need kind
  teardown
fi

c_yellow "==> [1/9] Checking dependencies (docker, kind, kubectl, go)"
need docker
need kind
need kubectl
need go
if ! docker info >/dev/null 2>&1; then
  c_red "The Docker CLI is installed, but the Docker daemon isn't running/reachable."
  c_red "On Mac: open Docker Desktop and wait for it to fully start, then re-run this script."
  exit 1
fi
c_green "All dependencies present, Docker daemon is running."

c_yellow "==> [2/9] Spinning up a throwaway Postgres container for tests"
docker rm -f orbitcp-test-postgres >/dev/null 2>&1 || true
docker run -d --name orbitcp-test-postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=orbitcp -p 55432:5432 postgres:16-alpine >/dev/null
trap 'docker rm -f orbitcp-test-postgres >/dev/null 2>&1 || true' EXIT
TEST_PG_DSN="postgres://postgres:postgres@127.0.0.1:55432/orbitcp?sslmode=disable"
c_yellow "    waiting for Postgres to accept connections..."
for i in $(seq 1 20); do
  docker exec orbitcp-test-postgres pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done
c_green "Postgres ready on 127.0.0.1:55432."

c_yellow "==> [3/9] Running unit test suite (go vet + go test -race, with Postgres persistence coverage)"
UNFORMATTED=$(gofmt -l . | grep -v third_party || true)
if [ -n "$UNFORMATTED" ]; then
  c_red "gofmt found unformatted files:"
  echo "$UNFORMATTED"
  c_red "run 'gofmt -w .' to fix"
  exit 1
fi
GOFLAGS=-mod=mod GOPROXY=off go vet ./...
TEST_PG_DSN="$TEST_PG_DSN" GOFLAGS=-mod=mod GOPROXY=off go test ./... -race -cover
c_green "Unit tests passed."

c_yellow "==> [4/9] Running integration test against a real 5-node local cluster + Postgres"
c_yellow "    (leader election, reconciler orchestration, chaos/failover, crash-recovery)"
TEST_PG_DSN="$TEST_PG_DSN" bash test/integration_test.sh
c_green "Integration tests passed."

c_yellow "==> [5/9] Creating kind cluster '${CLUSTER_NAME}' (skipped if it already exists)"
if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}\$"; then
  kind create cluster --name "${CLUSTER_NAME}"
else
  c_green "kind cluster '${CLUSTER_NAME}' already exists, reusing it."
fi
kubectl cluster-info --context "kind-${CLUSTER_NAME}"

c_yellow "==> [6/9] Building and loading Docker image ${IMAGE_NAME}"
docker build -t "${IMAGE_NAME}" .
# 'kind load docker-image' can fail on images carrying attestation/
# provenance manifests with a "content digest ... not found" error (a
# known kind/Docker Desktop interaction, common on Apple Silicon).
# Saving to a tarball first and loading THAT avoids it reliably for our
# own locally-built image.
docker save "${IMAGE_NAME}" -o /tmp/orbitcp-image.tar
kind load image-archive /tmp/orbitcp-image.tar --name "${CLUSTER_NAME}"
rm -f /tmp/orbitcp-image.tar

# postgres:16-alpine is a PUBLIC image already on Docker Hub -- unlike our
# own image, it doesn't need to be side-loaded into kind at all. Kubernetes'
# default imagePullPolicy (IfNotPresent, since the tag isn't "latest") means
# the kind node's own containerd will just pull it directly from Docker Hub
# the first time the Postgres pod schedules. Pre-loading it was needless
# extra work AND the thing actually triggering the attestation-manifest bug
# (Hub-pulled multi-arch images carry heavier attestation metadata than a
# locally-built single-arch one) -- removing the step fixes the failure by
# not needing the workaround in the first place.

c_yellow "==> [7/9] Applying Kubernetes manifests (Postgres + 5-node StatefulSet + PDB)"
kubectl apply -f k8s.yaml
kubectl -n "${NAMESPACE}" rollout status statefulset/postgres --timeout=120s
kubectl -n "${NAMESPACE}" rollout status statefulset/raft --timeout=180s

echo
kubectl -n "${NAMESPACE}" get pods -o wide
echo

c_yellow "==> [8/9] Deploying observability stack (Prometheus + Grafana)"
kubectl apply -f k8s-monitoring.yaml
kubectl -n "${NAMESPACE}" rollout status deployment/prometheus --timeout=120s
kubectl -n "${NAMESPACE}" rollout status deployment/grafana --timeout=120s

c_yellow "==> [9/9] Port-forwarding + live smoke test"
pkill -f "port-forward.*${NAMESPACE}.*orbitcp" 2>/dev/null || true
pkill -f "port-forward.*${NAMESPACE}.*prometheus" 2>/dev/null || true
pkill -f "port-forward.*${NAMESPACE}.*grafana" 2>/dev/null || true
pkill -f "port-forward.*${NAMESPACE}.*raft-" 2>/dev/null || true
kubectl -n "${NAMESPACE}" port-forward svc/orbitcp 8080:8080 >/tmp/orbitcp-pf.log 2>&1 &
PF_PID=$!
kubectl -n "${NAMESPACE}" port-forward svc/prometheus 9090:9090 >/tmp/orbitcp-prom-pf.log 2>&1 &
PROM_PF_PID=$!
kubectl -n "${NAMESPACE}" port-forward svc/grafana 3000:3000 >/tmp/orbitcp-grafana-pf.log 2>&1 &
GRAFANA_PF_PID=$!
sleep 3

STATUS_RESPONSE=$(curl -s http://127.0.0.1:8080/status)
echo "$STATUS_RESPONSE"
echo

echo "-- provisioning a compute resource (watch it move PENDING -> PROVISIONING -> RUNNING) --"
# IMPORTANT: 'kubectl port-forward svc/X' picks ONE backend pod when the
# tunnel starts and sends every request through that same pod for the
# tunnel's entire lifetime -- it does NOT load-balance per-request like a
# real Service does inside the cluster. So if port 8080 above landed on a
# follower, retrying against it would never help (there's nothing to
# retry against -- it's the same fixed pod every time). Instead: find the
# actual leader from /status, then open a SEPARATE, temporary port-forward
# directly to that specific leader pod just for this one call.
LEADER_ID=$(echo "$STATUS_RESPONSE" | grep -o '"leader_id":"[^"]*"' | cut -d'"' -f4)
LEADER_POD=$(echo "$LEADER_ID" | cut -d'.' -f1) # "raft-3.raft-headless:8080" -> "raft-3"

if [[ -n "$LEADER_POD" ]]; then
  kubectl -n "${NAMESPACE}" port-forward "pod/${LEADER_POD}" 8090:8080 >/tmp/orbitcp-leader-pf.log 2>&1 &
  LEADER_PF_PID=$!
  sleep 2
  CREATE_RESPONSE=$(curl -s -X POST http://127.0.0.1:8090/resources -d '{"kind":"compute","spec":"{\"cpu\":4,\"memory_gb\":16}"}')
  kill -9 "$LEADER_PF_PID" >/dev/null 2>&1 || true
else
  CREATE_RESPONSE='{"error":"could not determine leader from /status"}'
fi
echo "$CREATE_RESPONSE"
if ! echo "$CREATE_RESPONSE" | grep -q '"state":"PENDING"'; then
  echo "(unexpected -- the leader pod itself rejected the write; check 'kubectl -n ${NAMESPACE} logs pod/${LEADER_POD}')"
fi

c_green ""
c_green "==================================================================="
c_green " Orbit is up (5 control-plane nodes, quorum=3, Postgres-backed)."
c_green ""
c_green "   curl http://127.0.0.1:8080/status"
c_green "   curl -X POST http://127.0.0.1:8080/resources -d '{\"kind\":\"compute\",\"spec\":\"{}\"}'"
c_green "   curl http://127.0.0.1:8080/resources"
c_green "   curl http://127.0.0.1:8080/metrics"
c_green ""
c_green " NOTE: this port-forward is pinned to ONE pod for its whole lifetime"
c_green " (that's how 'kubectl port-forward svc/X' works -- it does NOT load"
c_green " balance per-request like the Service does inside the cluster). If"
c_green " POSTs above return 421 'not leader', check the leader_id in the"
c_green " response and port-forward that specific pod instead, e.g.:"
c_green "   kubectl -n ${NAMESPACE} port-forward pod/raft-3 8080:8080"
c_green ""
c_green " Grafana dashboard (leader changes, term, resource states over time):"
c_green "   open http://127.0.0.1:3000   (user: admin / pass: orbitcp-demo)"
c_green "   Dashboard 'Orbit Control Plane' is pre-loaded, no setup needed."
c_green ""
c_green " Prometheus (raw metrics/queries):"
c_green "   open http://127.0.0.1:9090"
c_green ""
c_green " Watch leader election + reconciliation live:"
c_green "   kubectl -n ${NAMESPACE} logs -f statefulset/raft --all-containers"
c_green ""
c_green " Chaos-test failover yourself (find the leader via /status first) --"
c_green " then watch it happen live on the Grafana dashboard:"
c_green "   kubectl -n ${NAMESPACE} delete pod raft-0"
c_green ""
c_green " Prove crash-recovery yourself: create a resource, delete its pod,"
c_green " then immediately GET it again once the pod's back -- state survives:"
c_green "   kubectl -n ${NAMESPACE} delete pod raft-2"
c_green ""
c_green " Tear everything down:"
c_green "   ./setup.sh teardown"
c_green "==================================================================="
c_green ""
c_yellow "(port-forward running in background as PID ${PF_PID}, logging to /tmp/orbitcp-pf.log)"