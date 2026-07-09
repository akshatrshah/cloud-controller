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

c_yellow "==> [1/8] Checking dependencies (docker, kind, kubectl, go)"
need docker
need kind
need kubectl
need go
c_green "All dependencies present."

c_yellow "==> [2/8] Running unit test suite (go vet + go test -race)"
UNFORMATTED=$(gofmt -l . | grep -v third_party || true)
if [ -n "$UNFORMATTED" ]; then
  c_red "gofmt found unformatted files:"
  echo "$UNFORMATTED"
  c_red "run 'gofmt -w .' to fix"
  exit 1
fi
GOFLAGS=-mod=mod GOPROXY=off go vet ./...
if command -v docker >/dev/null 2>&1 && docker ps >/dev/null 2>&1; then
  c_yellow "    (spinning up a throwaway Postgres container for persistence tests)"
  docker rm -f orbitcp-test-postgres >/dev/null 2>&1 || true
  docker run -d --name orbitcp-test-postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=orbitcp -p 55432:5432 postgres:16-alpine >/dev/null
  trap 'docker rm -f orbitcp-test-postgres >/dev/null 2>&1 || true' EXIT
  sleep 3
  TEST_PG_DSN="postgres://postgres:postgres@127.0.0.1:55432/orbitcp?sslmode=disable" \
    GOFLAGS=-mod=mod GOPROXY=off go test ./... -race -cover
else
  c_yellow "    docker not available for a test Postgres -- running tests without persistence coverage"
  GOFLAGS=-mod=mod GOPROXY=off go test ./... -race -cover
fi
c_green "Unit tests passed."

c_yellow "==> [3/8] Running integration test against a real 5-node local cluster + Postgres"
c_yellow "    (leader election, reconciler orchestration, chaos/failover, crash-recovery)"
TEST_PG_DSN="postgres://postgres:postgres@127.0.0.1:55432/orbitcp?sslmode=disable" bash test/integration_test.sh
c_green "Integration tests passed."

c_yellow "==> [4/8] Creating kind cluster '${CLUSTER_NAME}' (skipped if it already exists)"
if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}\$"; then
  kind create cluster --name "${CLUSTER_NAME}"
else
  c_green "kind cluster '${CLUSTER_NAME}' already exists, reusing it."
fi
kubectl cluster-info --context "kind-${CLUSTER_NAME}"

c_yellow "==> [5/8] Building and loading Docker image ${IMAGE_NAME}"
docker build -t "${IMAGE_NAME}" .
kind load docker-image "${IMAGE_NAME}" --name "${CLUSTER_NAME}"

c_yellow "==> [6/8] Also pre-pulling and loading postgres:16-alpine into kind"
docker pull postgres:16-alpine
kind load docker-image postgres:16-alpine --name "${CLUSTER_NAME}"

c_yellow "==> [7/8] Applying Kubernetes manifests (Postgres + 5-node StatefulSet + PDB)"
kubectl apply -f k8s.yaml
kubectl -n "${NAMESPACE}" rollout status statefulset/postgres --timeout=120s
kubectl -n "${NAMESPACE}" rollout status statefulset/raft --timeout=180s

echo
kubectl -n "${NAMESPACE}" get pods -o wide
echo

c_yellow "==> [8/8] Port-forwarding + live smoke test"
pkill -f "port-forward.*${NAMESPACE}.*orbitcp" 2>/dev/null || true
kubectl -n "${NAMESPACE}" port-forward svc/orbitcp 8080:8080 >/tmp/orbitcp-pf.log 2>&1 &
PF_PID=$!
sleep 3

curl -s http://127.0.0.1:8080/status | tee /dev/stderr | grep -q '"id"' && echo
echo
echo "-- provisioning a compute resource (watch it move PENDING -> PROVISIONING -> RUNNING) --"
curl -s -X POST http://127.0.0.1:8080/resources -d '{"kind":"compute","spec":"{\"cpu\":4,\"memory_gb\":16}"}'; echo

c_green ""
c_green "==================================================================="
c_green " Orbit is up (5 control-plane nodes, quorum=3, Postgres-backed)."
c_green ""
c_green "   curl http://127.0.0.1:8080/status"
c_green "   curl -X POST http://127.0.0.1:8080/resources -d '{\"kind\":\"compute\",\"spec\":\"{}\"}'"
c_green "   curl http://127.0.0.1:8080/resources"
c_green ""
c_green " Watch leader election + reconciliation live:"
c_green "   kubectl -n ${NAMESPACE} logs -f statefulset/raft --all-containers"
c_green ""
c_green " Chaos-test failover yourself (find the leader via /status first):"
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