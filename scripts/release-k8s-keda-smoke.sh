#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="${RELEASE_K8S_CLUSTER:-taskflow-release}"
NAMESPACE="${RELEASE_K8S_NAMESPACE:-taskflow-release}"
TOOLS_DIR="${RELEASE_K8S_TOOLS_DIR:-/tmp/taskflow-release-k8s-tools}"
KIND_VERSION="0.32.0"
KUBECTL_VERSION="1.35.0"
KEDA_VERSION="2.20.2"
KIND_NODE_IMAGE="kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f"
KIND="${KIND_BIN:-$TOOLS_DIR/kind}"
KUBECTL="${KUBECTL_BIN:-$TOOLS_DIR/kubectl}"
KEEP_CLUSTER="${RELEASE_K8S_KEEP_CLUSTER:-0}"
EXISTING_KUBECONFIG="${RELEASE_K8S_EXISTING_KUBECONFIG:-}"
KUBECTL_CONTEXT="${RELEASE_K8S_CONTEXT:-}"
NODE_CONTAINER="${RELEASE_K8S_NODE_CONTAINER:-}"
created_cluster=0
prometheus_pf_pid=""
phase="setup"

cleanup() {
  rc=$?
  if [ -n "$prometheus_pf_pid" ]; then
    kill "$prometheus_pf_pid" >/dev/null 2>&1 || true
  fi
  if [ "$rc" -ne 0 ]; then
    echo "real Kubernetes/KEDA smoke failed during phase: $phase" >&2
    k -n "$NAMESPACE" get pods,deploy,pdb,hpa,scaledobject 2>/dev/null >&2 || true
    k -n "$NAMESPACE" describe scaledobject taskflow-queue-scaler 2>/dev/null >&2 || true
  fi
  if [ "$KEEP_CLUSTER" != "1" ] && [ "$created_cluster" = "1" ]; then
    "$KIND" delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
  return "$rc"
}
trap cleanup EXIT

ensure_tools() {
  mkdir -p "$TOOLS_DIR"
  if [ ! -x "$KIND" ]; then
    curl -fsSL --retry 3 -o "$KIND.tmp" "https://kind.sigs.k8s.io/dl/v${KIND_VERSION}/kind-linux-amd64"
    mv "$KIND.tmp" "$KIND"
    chmod +x "$KIND"
  fi
  if [ ! -x "$KUBECTL" ]; then
    curl -fsSL --retry 3 -o "$KUBECTL.tmp" "https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
    mv "$KUBECTL.tmp" "$KUBECTL"
    chmod +x "$KUBECTL"
  fi
}

k() {
  if [ -n "$EXISTING_KUBECONFIG" ]; then
    if [ -n "$KUBECTL_CONTEXT" ]; then
      "$KUBECTL" --kubeconfig "$EXISTING_KUBECONFIG" --context "$KUBECTL_CONTEXT" "$@"
    else
      "$KUBECTL" --kubeconfig "$EXISTING_KUBECONFIG" "$@"
    fi
    return
  fi
  "$KUBECTL" --context "kind-$CLUSTER_NAME" "$@"
}

wait_deploy() {
  name="$1"
  ns="$2"
  k -n "$ns" rollout status "deployment/$name" --timeout=240s >/dev/null
}

wait_pdb_healthy() {
  i=0
  while [ "$i" -lt 120 ]; do
    healthy="$(k -n "$NAMESPACE" get pdb taskflow -o jsonpath='{.status.currentHealthy}' 2>/dev/null || true)"
    allowed="$(k -n "$NAMESPACE" get pdb taskflow -o jsonpath='{.status.disruptionsAllowed}' 2>/dev/null || true)"
    if [ "${healthy:-0}" -ge 2 ] 2>/dev/null && [ "${allowed:-0}" -ge 1 ] 2>/dev/null; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.5
  done
  return 1
}

evict_pod() {
  pod="$1"
  printf '{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"%s","namespace":"%s"}}' "$pod" "$NAMESPACE" |
    k create --raw "/api/v1/namespaces/$NAMESPACE/pods/$pod/eviction" -f -
}

ensure_tools

phase="connect or create Kubernetes cluster"
if [ -z "$EXISTING_KUBECONFIG" ]; then
  if ! "$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    "$KIND" create cluster --name "$CLUSTER_NAME" --image "$KIND_NODE_IMAGE" --wait 180s
    created_cluster=1
  fi
fi
k wait --for=condition=Ready node --all --timeout=120s >/dev/null

phase="load taskflow image"
docker tag taskflow-go:local taskflow-go:latest
if [ -n "$EXISTING_KUBECONFIG" ]; then
  if [ -n "$NODE_CONTAINER" ]; then
    docker save taskflow-go:latest | docker exec -i "$NODE_CONTAINER" k3s ctr images import - >/dev/null
  fi
else
  "$KIND" load docker-image taskflow-go:latest --name "$CLUSTER_NAME" >/dev/null
fi

phase="install release dependencies"
k create namespace "$NAMESPACE" >/dev/null 2>&1 || true
k -n "$NAMESPACE" apply -f deploy/k8s/release-validation-infra.yaml >/dev/null
k -n "$NAMESPACE" create secret generic taskflow-secret \
  --from-literal=MYSQL_DSN='taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true' \
  --from-literal=REDIS_PASSWORD='' \
  --from-literal=AI_BEARER_TOKEN='' \
  --from-literal=AI_FALLBACK_BEARER_TOKEN='' \
  --from-literal=CONTROL_PLANE_BEARER_TOKEN='release-control-plane-token' \
  --from-literal=CONTROL_PLANE_PREVIOUS_BEARER_TOKEN='' \
  --from-literal=QUEUE_DLQ_ARCHIVE_HTTP_BEARER_TOKEN='' \
  --from-literal=QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_BEARER_TOKEN='' \
  --dry-run=client -o yaml | k apply -f - >/dev/null
k -n "$NAMESPACE" apply -f deploy/k8s/configmap.yaml >/dev/null
k -n "$NAMESPACE" patch configmap taskflow-config --type merge \
  -p '{"data":{"WORKER_COUNT":"1","QUEUE_STATS_INTERVAL_MS":"1000","QUEUE_DLQ_LIFECYCLE_ENABLED":"false","RATE_LIMIT_RPS":"10000","RATE_LIMIT_BURST":"20000"}}' >/dev/null
if [ -n "$EXISTING_KUBECONFIG" ]; then
  # The production examples intentionally request RWX shared storage. Local
  # kind/k3s release-validation clusters commonly expose only a single-node
  # RWO storage class, which is sufficient for two replicas scheduled on that
  # node. Keep the production manifests unchanged and adapt only this test apply.
  sed 's/ReadWriteMany/ReadWriteOnce/' deploy/k8s/dlq-archive-pvc.yaml | k -n "$NAMESPACE" apply -f - >/dev/null
  sed 's/ReadWriteMany/ReadWriteOnce/' deploy/k8s/dlq-archive-replica-pvc.yaml | k -n "$NAMESPACE" apply -f - >/dev/null
else
  k -n "$NAMESPACE" apply -f deploy/k8s/dlq-archive-pvc.yaml -f deploy/k8s/dlq-archive-replica-pvc.yaml >/dev/null
fi
wait_deploy mysql "$NAMESPACE"
wait_deploy redis "$NAMESPACE"
wait_deploy prometheus "$NAMESPACE"

phase="deploy production taskflow manifests"
k -n "$NAMESPACE" apply -f deploy/k8s/executor-service.yaml -f deploy/k8s/executor-deployment.yaml -f deploy/k8s/executor-pdb.yaml >/dev/null
k -n "$NAMESPACE" apply -f deploy/k8s/service.yaml -f deploy/k8s/deployment.yaml -f deploy/k8s/taskflow-pdb.yaml >/dev/null
wait_deploy executor-worker "$NAMESPACE"
wait_deploy taskflow "$NAMESPACE"
wait_pdb_healthy

phase="real eviction API and PDB enforcement"
pods="$(k -n "$NAMESPACE" get pods -l app=taskflow -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"
first="$(printf '%s\n' "$pods" | sed -n '1p')"
second="$(printf '%s\n' "$pods" | sed -n '2p')"
test -n "$first"
test -n "$second"
evict_pod "$first" >/tmp/taskflow-release-first-eviction.json
blocked=0
i=0
while [ "$i" -lt 20 ]; do
  if evict_pod "$second" >/tmp/taskflow-release-second-eviction.out 2>/tmp/taskflow-release-second-eviction.err; then
    # A replacement may have become Ready before the second request. If so,
    # discover the current peer and retry only while the first disruption is
    # still in flight; PDB correctness is asserted by an actual 429/block.
    second="$(k -n "$NAMESPACE" get pods -l app=taskflow -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -v "$first" | head -1)"
  else
    if grep -Eqi 'disruption budget|TooManyRequests|429' /tmp/taskflow-release-second-eviction.err /tmp/taskflow-release-second-eviction.out; then
      blocked=1
      break
    fi
  fi
  i=$((i + 1))
  sleep 0.1
done
test "$blocked" = "1"
wait_deploy taskflow "$NAMESPACE"
wait_pdb_healthy

phase="rolling update availability"
k -n "$NAMESPACE" rollout restart deployment/taskflow >/dev/null
min_ready=999
i=0
while [ "$i" -lt 600 ]; do
  ready="$(k -n "$NAMESPACE" get deployment taskflow -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
  ready="${ready:-0}"
  if [ "$ready" -lt "$min_ready" ]; then
    min_ready="$ready"
  fi
  if k -n "$NAMESPACE" rollout status deployment/taskflow --timeout=1s >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.2
done
test "$i" -lt 600
test "$min_ready" -ge 1

phase="install KEDA"
KEDA_MANIFEST="$TOOLS_DIR/keda-${KEDA_VERSION}.yaml"
if [ ! -s "$KEDA_MANIFEST" ]; then
  curl -fsSL --retry 3 -o "$KEDA_MANIFEST" "https://github.com/kedacore/keda/releases/download/v${KEDA_VERSION}/keda-${KEDA_VERSION}.yaml"
fi
k apply --server-side -f "$KEDA_MANIFEST" >/dev/null
wait_deploy keda-operator keda
wait_deploy keda-metrics-apiserver keda

phase="apply live Prometheus scaler"
sed "s/prometheus.default.svc.cluster.local/prometheus.$NAMESPACE.svc.cluster.local/" deploy/k8s/keda-scaledobject.example.yaml |
  k -n "$NAMESPACE" apply -f - >/dev/null
k -n "$NAMESPACE" patch scaledobject taskflow-queue-scaler --type merge -p '{"spec":{"pollingInterval":5}}' >/dev/null

phase="verify guarded queue metric"
k -n "$NAMESPACE" port-forward service/prometheus 19090:9090 >/tmp/taskflow-release-prometheus-pf.log 2>&1 &
prometheus_pf_pid=$!
i=0
while [ "$i" -lt 60 ]; do
  if curl -fsS 'http://127.0.0.1:19090/-/ready' >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.25
done
test "$i" -lt 60
i=0
while [ "$i" -lt 60 ]; do
  fresh="$(curl -fsSG 'http://127.0.0.1:19090/api/v1/query' --data-urlencode 'query=max(taskflow_queue_stats_fresh)' 2>/dev/null || true)"
  if printf '%s' "$fresh" | grep -q '"1"'; then
    break
  fi
  i=$((i + 1))
  sleep 0.5
done
test "$i" -lt 60

phase="verify scaler healthy before backlog"
i=0
while [ "$i" -lt 60 ]; do
  fallback="$(k -n "$NAMESPACE" get scaledobject taskflow-queue-scaler -o jsonpath='{.status.conditions[?(@.type=="Fallback")].status}' 2>/dev/null || true)"
  replicas="$(k -n "$NAMESPACE" get deployment taskflow -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
  if [ "$fallback" != "True" ] && [ "${replicas:-0}" = "2" ]; then
    break
  fi
  i=$((i + 1))
  sleep 0.5
done
test "$i" -lt 60
test "$fallback" != "True"
test "$replicas" = "2"

phase="real KEDA backlog scale-up"
redis_pod="$(k -n "$NAMESPACE" get pods -l app=redis -o jsonpath='{.items[0].metadata.name}')"
k -n "$NAMESPACE" exec "$redis_pod" -- redis-cli EVAL \
  "for i=1,10000 do redis.call('XADD',KEYS[1],'*','task_id',tostring(9000000+i),'enqueue_generation','1') end return redis.call('XLEN',KEYS[1])" \
  1 taskflow:queue >/tmp/taskflow-release-backlog.txt

i=0
backlog=0
while [ "$i" -lt 60 ]; do
  response="$(curl -fsSG 'http://127.0.0.1:19090/api/v1/query' --data-urlencode 'query=max(taskflow_queue_backlog)' 2>/dev/null || true)"
  backlog="$(printf '%s' "$response" | python3 -c 'import json,sys; d=json.load(sys.stdin); r=d.get("data",{}).get("result",[]); print(int(float(r[0]["value"][1])) if r else 0)' 2>/dev/null || echo 0)"
  if [ "$backlog" -gt 100 ]; then
    break
  fi
  i=$((i + 1))
  sleep 0.5
done
test "$backlog" -gt 100

max_replicas=0
i=0
while [ "$i" -lt 180 ]; do
  replicas="$(k -n "$NAMESPACE" get deployment taskflow -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
  replicas="${replicas:-0}"
  if [ "$replicas" -gt "$max_replicas" ]; then
    max_replicas="$replicas"
  fi
  if [ "$max_replicas" -gt 2 ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
test "$max_replicas" -gt 2
k -n "$NAMESPACE" get hpa keda-hpa-taskflow-queue-scaler >/dev/null
ready_condition="$(k -n "$NAMESPACE" get scaledobject taskflow-queue-scaler -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')"
test "$ready_condition" = "True"

python3 scripts/release-k8s-keda-check.py
phase="complete"
echo "real Kubernetes eviction/PDB/rollout availability: ok (min_ready=$min_ready)"
echo "real KEDA Prometheus backlog scale-up: ok (backlog=$backlog max_replicas=$max_replicas)"
