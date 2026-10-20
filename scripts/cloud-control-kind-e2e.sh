#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

LOCK_FILE="${CLOUD_SAGA_E2E_LOCK_FILE:-/tmp/taskflow-cloud-saga-e2e.lock}"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "another cloud-control Kind E2E instance is already running" >&2
  exit 75
fi

CLUSTER_NAME="${CLOUD_SAGA_E2E_CLUSTER:-taskflow-cloud-saga}"
NAMESPACE="${CLOUD_SAGA_E2E_NAMESPACE:-taskflow-cloud-saga}"
TOOLS_DIR="${CLOUD_SAGA_E2E_TOOLS_DIR:-/tmp/taskflow-release-k8s-tools}"
KIND_VERSION="0.32.0"
KUBECTL_VERSION="1.35.0"
KIND_NODE_IMAGE="kindest/node:v1.35.0"
KIND="${KIND_BIN:-$TOOLS_DIR/kind}"
KUBECTL="${KUBECTL_BIN:-$TOOLS_DIR/kubectl}"
KUBECONFIG_FILE="${CLOUD_SAGA_E2E_KUBECONFIG:-/tmp/cloud-saga-e2e-kubeconfig}"
IMAGE="${CLOUD_SAGA_E2E_IMAGE:-taskflow-go:cloud-saga-e2e}"
KEEP_CLUSTER="${CLOUD_SAGA_E2E_KEEP_CLUSTER:-0}"
AUTH_TOKEN="release-control-plane-token"
TENANT="kind-saga-tenant"
RESOURCE_NAME="kind-saga-app"
created_cluster=0
service_pf_pid=""
pod1_pf_pid=""
pod2_pf_pid=""
phase="initialization"

# These ports are dedicated to this local E2E. Clean up stale kubectl
# port-forwards left behind by an interrupted previous run before starting new
# forwards. flock above prevents two live E2E instances from racing here.
for port in 18080 18081 18082; do
  fuser -k "${port}/tcp" >/dev/null 2>&1 || true
done

cleanup() {
  rc=$?
  for pid in "$service_pf_pid" "$pod1_pf_pid" "$pod2_pf_pid"; do
    if [ -n "$pid" ]; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  if [ "$rc" -ne 0 ]; then
    echo "cloud-control Kind E2E failed during phase: $phase" >&2
    k -n "$NAMESPACE" get pods,deploy,svc,pvc 2>/dev/null >&2 || true
    k -n "$NAMESPACE" get events --sort-by=.lastTimestamp 2>/dev/null | tail -80 >&2 || true
    k -n "$NAMESPACE" logs deployment/taskflow --all-containers --tail=200 2>/dev/null >&2 || true
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

ensure_mirrored_image() {
  local_image="$1"
  mirror_image="$2"
  if docker image inspect "$local_image" >/dev/null 2>&1; then
    return 0
  fi
  docker pull "$mirror_image" >/dev/null
  docker tag "$mirror_image" "$local_image"
}

repack_for_kind() {
  source_image="$1"
  repacked_image="$2"
  printf 'ARG BASE\nFROM ${BASE}\nLABEL taskflow.e2e.repack="1"\n' |
    docker build --pull=false --build-arg BASE="$source_image" -t "$repacked_image" - >/dev/null 2>&1
  docker tag "$repacked_image" "$source_image"
}

k() {
  "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" "$@"
}

wait_deploy() {
  k -n "$2" rollout status "deployment/$1" --timeout=240s >/dev/null
}

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 240 ]; do
    if curl --connect-timeout 1 --max-time 2 -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.25
  done
  echo "timeout waiting for $url" >&2
  return 1
}

start_pod1_forward() {
  if [ -n "$pod1_pf_pid" ]; then
    kill "$pod1_pf_pid" >/dev/null 2>&1 || true
  fi
  "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" port-forward "pod/$pod1" 18081:8080 9>&- >/tmp/cloud-saga-e2e-pod1-pf.log 2>&1 &
  pod1_pf_pid=$!
}

start_pod2_forward() {
  if [ -n "$pod2_pf_pid" ]; then
    kill "$pod2_pf_pid" >/dev/null 2>&1 || true
  fi
  "$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" port-forward "pod/$pod2" 18082:8080 9>&- >/tmp/cloud-saga-e2e-pod2-pf.log 2>&1 &
  pod2_pf_pid=$!
}

wait_pod_forwards() {
  i=0
  while [ "$i" -lt 120 ]; do
    ok1=0
    ok2=0
    curl --connect-timeout 1 --max-time 1 -fsS http://127.0.0.1:18081/health/ready >/dev/null 2>&1 && ok1=1
    curl --connect-timeout 1 --max-time 1 -fsS http://127.0.0.1:18082/health/ready >/dev/null 2>&1 && ok2=1
    if [ "$ok1" -eq 1 ] && [ "$ok2" -eq 1 ]; then
      return 0
    fi
    if [ "$ok1" -ne 1 ]; then
      start_pod1_forward
    fi
    if [ "$ok2" -ne 1 ]; then
      start_pod2_forward
    fi
    i=$((i + 1))
    sleep 0.5
  done
  echo "timeout waiting for individual TaskFlow pod forwards" >&2
  return 1
}

auth_curl() {
  curl -fsS -H "Authorization: Bearer $AUTH_TOKEN" "$@"
}

json_field() {
  field="$1"
  python3 -c "import json,sys; d=json.load(sys.stdin); x=d; [x:=x[p] for p in '$field'.split('.')]; print(x)"
}

workflow_status() {
  auth_curl "http://127.0.0.1:18080/api/v1/cloud/workflows/$1" | json_field workflow.status
}

drive_workflow() {
  workflow_id="$1"
  want="$2"
  i=0
  while [ "$i" -lt 360 ]; do
    status="$(workflow_status "$workflow_id")"
    if [ "$status" = "$want" ]; then
      return 0
    fi
    auth_curl -X POST "http://127.0.0.1:18080/api/v1/cloud/workflows/$workflow_id/reconcile" >/tmp/cloud-saga-e2e-reconcile.json 2>/dev/null || true
    i=$((i + 1))
    sleep 0.5
  done
  echo "workflow $workflow_id did not reach $want; last status=$(workflow_status "$workflow_id")" >&2
  return 1
}

wait_workflow() {
  workflow_id="$1"
  want="$2"
  i=0
  while [ "$i" -lt 360 ]; do
    status="$(workflow_status "$workflow_id")"
    if [ "$status" = "$want" ]; then
      return 0
    fi
    i=$((i + 1))
    sleep 0.25
  done
  echo "workflow $workflow_id did not automatically reach $want; last status=$(workflow_status "$workflow_id")" >&2
  return 1
}

ensure_tools

phase="build TaskFlow and workload images"
docker build --network=host --build-arg GOPROXY="${GOPROXY:-https://goproxy.cn|direct}" -t "$IMAGE" . >/tmp/cloud-saga-e2e-build.log
docker tag "$IMAGE" taskflow-go:latest
ensure_mirrored_image mysql:8.4 docker.m.daocloud.io/library/mysql:8.4
ensure_mirrored_image redis:8-alpine docker.m.daocloud.io/library/redis:8-alpine
ensure_mirrored_image nginx:1.27-alpine docker.m.daocloud.io/library/nginx:1.27-alpine
ensure_mirrored_image prom/prometheus:v2.55.1 docker.m.daocloud.io/prom/prometheus:v2.55.1
ensure_mirrored_image "$KIND_NODE_IMAGE" dockerproxy.net/kindest/node:v1.35.0
repack_for_kind mysql:8.4 taskflow-e2e/mysql:8.4
repack_for_kind redis:8-alpine taskflow-e2e/redis:8-alpine
repack_for_kind nginx:1.27-alpine taskflow-e2e/nginx:1.27-alpine
repack_for_kind prom/prometheus:v2.55.1 taskflow-e2e/prometheus:v2.55.1

phase="create Kind cluster"
if ! "$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  kind_config="/tmp/${CLUSTER_NAME}-kind-config.yaml"
  cat >"$kind_config" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  kubeProxyMode: iptables
EOF
  "$KIND" create cluster --name "$CLUSTER_NAME" --image "$KIND_NODE_IMAGE" --config "$kind_config" --wait 180s
  created_cluster=1
fi
"$KIND" get kubeconfig --name "$CLUSTER_NAME" >"$KUBECONFIG_FILE"
k wait --for=condition=Ready node --all --timeout=120s >/dev/null
k -n kube-system rollout status daemonset/kube-proxy --timeout=120s >/dev/null
k -n kube-system rollout status deployment/coredns --timeout=120s >/dev/null
phase="load single-platform images into Kind"
for image in taskflow-go:latest nginx:1.27-alpine mysql:8.4 redis:8-alpine prom/prometheus:v2.55.1; do
  "$KIND" load docker-image "$image" --name "$CLUSTER_NAME" >/dev/null
done

phase="deploy real MySQL Redis RBAC and two TaskFlow replicas"
if k get namespace "$NAMESPACE" >/dev/null 2>&1; then
  k delete namespace "$NAMESPACE" --wait=true --timeout=120s >/dev/null
fi
k delete persistentvolume "${CLUSTER_NAME}-saga-pv" --ignore-not-found=true >/dev/null 2>&1 || true
k create namespace "$NAMESPACE" >/dev/null
k -n "$NAMESPACE" apply -f deploy/k8s/release-validation-infra.yaml >/dev/null
k -n "$NAMESPACE" apply -f deploy/k8s/cloud-control-rbac.yaml >/dev/null
cat <<EOF | k apply -f - >/dev/null
apiVersion: v1
kind: PersistentVolume
metadata:
  name: ${CLUSTER_NAME}-saga-pv
spec:
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: manual
  hostPath:
    path: /var/local/${CLUSTER_NAME}-saga
    type: DirectoryOrCreate
EOF
k -n "$NAMESPACE" create secret generic taskflow-secret \
  --from-literal=MYSQL_DSN='taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true' \
  --from-literal=REDIS_PASSWORD='' \
  --from-literal=AI_BEARER_TOKEN='' \
  --from-literal=AI_FALLBACK_BEARER_TOKEN='' \
  --from-literal=CONTROL_PLANE_BEARER_TOKEN="$AUTH_TOKEN" \
  --from-literal=CONTROL_PLANE_PREVIOUS_BEARER_TOKEN='' \
  --from-literal=QUEUE_DLQ_ARCHIVE_HTTP_BEARER_TOKEN='' \
  --from-literal=QUEUE_DLQ_ARCHIVE_REPLICA_HTTP_BEARER_TOKEN='' \
  --dry-run=client -o yaml | k apply -f - >/dev/null
k -n "$NAMESPACE" apply -f deploy/k8s/configmap.yaml >/dev/null
k -n "$NAMESPACE" patch configmap taskflow-config --type merge -p "{\"data\":{\"CLOUD_CONTROL_ENABLED\":\"true\",\"CLOUD_CONTROL_KUBE_NAMESPACE\":\"$NAMESPACE\",\"CLOUD_SAGA_CONTROLLER_ENABLED\":\"false\",\"CLOUD_SAGA_RECONCILE_INTERVAL_MS\":\"100\",\"EXECUTION_BACKEND\":\"local\",\"WORKER_COUNT\":\"1\",\"QUEUE_DLQ_LIFECYCLE_ENABLED\":\"false\",\"QUEUE_DLQ_ARCHIVE_OBJECT_LIFECYCLE_ENABLED\":\"false\",\"RATE_LIMIT_RPS\":\"10000\",\"RATE_LIMIT_BURST\":\"20000\"}}" >/dev/null
wait_deploy mysql "$NAMESPACE"
wait_deploy redis "$NAMESPACE"
k -n "$NAMESPACE" apply -f deploy/k8s/cloud-saga-e2e-app.yaml >/dev/null
wait_deploy taskflow "$NAMESPACE"

phase="port-forward service and individual replicas"
"$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" port-forward service/taskflow 18080:80 9>&- >/tmp/cloud-saga-e2e-service-pf.log 2>&1 &
service_pf_pid=$!
wait_http http://127.0.0.1:18080/health/ready
pods="$(k -n "$NAMESPACE" get pods -l app=taskflow -o json | python3 -c '
import json,sys
d=json.load(sys.stdin)
for p in d.get("items",[]):
    if p.get("metadata",{}).get("deletionTimestamp"):
        continue
    ready=any(c.get("type")=="Ready" and c.get("status")=="True" for c in p.get("status",{}).get("conditions",[]))
    if ready:
        print(p["metadata"]["name"])
')"
pod1="$(printf '%s\n' "$pods" | sed -n '1p')"
pod2="$(printf '%s\n' "$pods" | sed -n '2p')"
test -n "$pod1"
test -n "$pod2"
start_pod1_forward
start_pod2_forward
wait_pod_forwards

phase="admit tenant quota through real HTTP control plane"
auth_curl -X PUT -H 'Content-Type: application/json' \
  -d '{"cpu_milli_limit":4000,"memory_mib_limit":4096,"replica_limit":8}' \
  "http://127.0.0.1:18080/api/v1/cloud/tenants/$TENANT/quota" >/tmp/cloud-saga-e2e-quota.json

phase="create provision Saga with real Service PVC Deployment"
cat >/tmp/cloud-saga-e2e-provision.json <<EOF
{
  "tenant_id":"$TENANT",
  "name":"$RESOURCE_NAME",
  "provider":"kubernetes",
  "spec":{
    "compute":{"namespace":"$NAMESPACE","image":"nginx:1.27-alpine","replicas":1,"cpu":"50m","memory":"64Mi","container_port":80},
    "network":{"enabled":true,"port":8080,"target_port":80},
    "storage":{"enabled":true,"size":"64Mi","mount_path":"/data","storage_class_name":"manual"}
  }
}
EOF
auth_curl -X POST -H 'Content-Type: application/json' -H 'Idempotency-Key: kind-saga-provision-001' \
  --data-binary @/tmp/cloud-saga-e2e-provision.json \
  http://127.0.0.1:18080/api/v1/cloud/workflows/provision >/tmp/cloud-saga-e2e-provision-response.json
workflow_id="$(json_field workflow.id </tmp/cloud-saga-e2e-provision-response.json)"
resource_id="$(json_field resource.id </tmp/cloud-saga-e2e-provision-response.json)"

phase="prove two replicas contend on one workflow lease"
rm -f /tmp/cloud-saga-e2e-contention-*.status /tmp/cloud-saga-e2e-contention-*.body
i=1
race_pids=""
while [ "$i" -le 16 ]; do
  port=18081
  if [ $((i % 2)) -eq 0 ]; then port=18082; fi
  (
    curl -sS -o "/tmp/cloud-saga-e2e-contention-$i.body" -w '%{http_code}' \
      -X POST -H "Authorization: Bearer $AUTH_TOKEN" \
      "http://127.0.0.1:$port/api/v1/cloud/workflows/$workflow_id/reconcile" \
      | { read -r code || true; printf '%s\n' "$code"; } >"/tmp/cloud-saga-e2e-contention-$i.status"
  ) &
  race_pids="$race_pids $!"
  i=$((i + 1))
done
for pid in $race_pids; do
  wait "$pid"
done
busy_count="$(cat /tmp/cloud-saga-e2e-contention-*.status | grep -c '^409$' || true)"
ok_count="$(cat /tmp/cloud-saga-e2e-contention-*.status | grep -c '^200$' || true)"
test "$busy_count" -ge 1
test "$ok_count" -ge 1
curl -fsS http://127.0.0.1:18081/metrics >/tmp/cloud-saga-e2e-pod1-metrics.txt
curl -fsS http://127.0.0.1:18082/metrics >/tmp/cloud-saga-e2e-pod2-metrics.txt
busy_metric="$(cat /tmp/cloud-saga-e2e-pod1-metrics.txt /tmp/cloud-saga-e2e-pod2-metrics.txt | awk '/taskflow_cloud_saga_workflow_claims_total\{outcome="busy"\}/ {s+=$NF} END {print s+0}')"
test "${busy_metric%.*}" -ge 1

phase="complete provision Saga and verify real Kubernetes objects"
drive_workflow "$workflow_id" succeeded
k -n "$NAMESPACE" rollout status "deployment/$RESOURCE_NAME" --timeout=180s >/dev/null
k -n "$NAMESPACE" get service "$RESOURCE_NAME-svc" >/dev/null
k -n "$NAMESPACE" get pvc "$RESOURCE_NAME-data" >/dev/null
k -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Bound "pvc/$RESOURCE_NAME-data" --timeout=120s >/dev/null
k -n "$NAMESPACE" get deployment "$RESOURCE_NAME" >/dev/null
managed_by="$(k -n "$NAMESPACE" get deployment "$RESOURCE_NAME" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/managed-by}')"
test "$managed_by" = "taskflow-cloud-saga"
replicas="$(k -n "$NAMESPACE" get deployment "$RESOURCE_NAME" -o jsonpath='{.spec.replicas}')"
test "$replicas" = "1"

phase="resize bundle and verify quota plus Kubernetes convergence"
cat >/tmp/cloud-saga-e2e-resize.json <<EOF
{
  "spec":{
    "compute":{"namespace":"$NAMESPACE","image":"nginx:1.27-alpine","replicas":2,"cpu":"60m","memory":"96Mi","container_port":80},
    "network":{"enabled":true,"port":8081,"target_port":80},
    "storage":{"enabled":true,"size":"64Mi","mount_path":"/data","storage_class_name":"manual"}
  }
}
EOF
auth_curl -X POST -H 'Content-Type: application/json' -H 'Idempotency-Key: kind-saga-resize-002' \
  --data-binary @/tmp/cloud-saga-e2e-resize.json \
  "http://127.0.0.1:18080/api/v1/cloud/resources/$resource_id/workflows/resize" >/tmp/cloud-saga-e2e-resize-response.json
resize_workflow_id="$(json_field workflow.id </tmp/cloud-saga-e2e-resize-response.json)"
drive_workflow "$resize_workflow_id" succeeded
k -n "$NAMESPACE" rollout status "deployment/$RESOURCE_NAME" --timeout=180s >/dev/null
replicas="$(k -n "$NAMESPACE" get deployment "$RESOURCE_NAME" -o jsonpath='{.spec.replicas}')"
service_port="$(k -n "$NAMESPACE" get service "$RESOURCE_NAME-svc" -o jsonpath='{.spec.ports[0].port}')"
test "$replicas" = "2"
test "$service_port" = "8081"
quota_used="$(auth_curl "http://127.0.0.1:18080/api/v1/cloud/tenants/$TENANT/quota")"
test "$(printf '%s' "$quota_used" | json_field cpu_milli_used)" = "120"
test "$(printf '%s' "$quota_used" | json_field memory_mib_used)" = "192"
test "$(printf '%s' "$quota_used" | json_field replica_used)" = "2"

phase="delete bundle fail-forward and verify actual disappearance before quota release"
auth_curl -X POST -H 'Idempotency-Key: kind-saga-delete-003' \
  "http://127.0.0.1:18080/api/v1/cloud/resources/$resource_id/workflows/delete" >/tmp/cloud-saga-e2e-delete-response.json
delete_workflow_id="$(json_field workflow.id </tmp/cloud-saga-e2e-delete-response.json)"
drive_workflow "$delete_workflow_id" succeeded
if k -n "$NAMESPACE" get deployment "$RESOURCE_NAME" >/dev/null 2>&1; then exit 1; fi
if k -n "$NAMESPACE" get pvc "$RESOURCE_NAME-data" >/dev/null 2>&1; then exit 1; fi
if k -n "$NAMESPACE" get service "$RESOURCE_NAME-svc" >/dev/null 2>&1; then exit 1; fi
quota_used="$(auth_curl "http://127.0.0.1:18080/api/v1/cloud/tenants/$TENANT/quota")"
test "$(printf '%s' "$quota_used" | json_field cpu_milli_used)" = "0"
test "$(printf '%s' "$quota_used" | json_field memory_mib_used)" = "0"
test "$(printf '%s' "$quota_used" | json_field replica_used)" = "0"

phase="verify durable lifecycle history and bounded metrics"
auth_curl "http://127.0.0.1:18080/api/v1/cloud/resources/$resource_id/workflows?limit=20&offset=0" >/tmp/cloud-saga-e2e-history.json
python3 - <<'PY'
import json
d=json.load(open('/tmp/cloud-saga-e2e-history.json'))
kinds=[x['kind'] for x in d['items']]
assert kinds[:3] == ['delete_bundle','resize_bundle','provision_bundle'], kinds
PY
metrics="$(curl -fsS http://127.0.0.1:18080/metrics)"
printf '%s\n' "$metrics" | grep 'taskflow_cloud_saga_workflow_claims_total' >/dev/null
printf '%s\n' "$metrics" | grep 'taskflow_cloud_saga_step_operations_total' >/dev/null
if printf '%s\n' "$metrics" | grep '^taskflow_cloud_saga_' | grep -Eq 'tenant=|resource_id='; then
  echo "high-cardinality tenant/resource label leaked into Saga metrics" >&2
  exit 1
fi

phase="enable two background Saga controllers and prove automatic convergence"
k -n "$NAMESPACE" set env deployment/taskflow \
  CLOUD_SAGA_CONTROLLER_ENABLED=true \
  CLOUD_SAGA_RECONCILE_INTERVAL_MS=200 >/dev/null
wait_deploy taskflow "$NAMESPACE"
for pid in "$service_pf_pid" "$pod1_pf_pid" "$pod2_pf_pid"; do
  if [ -n "$pid" ]; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
done
service_pf_pid=""
pod1_pf_pid=""
pod2_pf_pid=""
"$KUBECTL" --kubeconfig "$KUBECONFIG_FILE" -n "$NAMESPACE" port-forward service/taskflow 18080:80 9>&- >/tmp/cloud-saga-e2e-service-pf.log 2>&1 &
service_pf_pid=$!
wait_http http://127.0.0.1:18080/health/ready

cat >/tmp/cloud-saga-e2e-auto-provision.json <<EOF
{
  "tenant_id":"$TENANT",
  "name":"kind-saga-auto",
  "provider":"kubernetes",
  "spec":{
    "compute":{"namespace":"$NAMESPACE","image":"nginx:1.27-alpine","replicas":1,"cpu":"50m","memory":"64Mi","container_port":80},
    "network":{"enabled":false},
    "storage":{"enabled":false}
  }
}
EOF
auth_curl -X POST -H 'Content-Type: application/json' -H 'Idempotency-Key: kind-saga-auto-provision-004' \
  --data-binary @/tmp/cloud-saga-e2e-auto-provision.json \
  http://127.0.0.1:18080/api/v1/cloud/workflows/provision >/tmp/cloud-saga-e2e-auto-provision-response.json
auto_workflow_id="$(json_field workflow.id </tmp/cloud-saga-e2e-auto-provision-response.json)"
auto_resource_id="$(json_field resource.id </tmp/cloud-saga-e2e-auto-provision-response.json)"
wait_workflow "$auto_workflow_id" succeeded
k -n "$NAMESPACE" rollout status deployment/kind-saga-auto --timeout=180s >/dev/null

auth_curl -X POST -H 'Idempotency-Key: kind-saga-auto-delete-005' \
  "http://127.0.0.1:18080/api/v1/cloud/resources/$auto_resource_id/workflows/delete" >/tmp/cloud-saga-e2e-auto-delete-response.json
auto_delete_workflow_id="$(json_field workflow.id </tmp/cloud-saga-e2e-auto-delete-response.json)"
wait_workflow "$auto_delete_workflow_id" succeeded
if k -n "$NAMESPACE" get deployment kind-saga-auto >/dev/null 2>&1; then
  echo "background Saga controllers did not remove automatic test Deployment" >&2
  exit 1
fi
quota_used="$(auth_curl "http://127.0.0.1:18080/api/v1/cloud/tenants/$TENANT/quota")"
test "$(printf '%s' "$quota_used" | json_field cpu_milli_used)" = "0"
test "$(printf '%s' "$quota_used" | json_field memory_mib_used)" = "0"
test "$(printf '%s' "$quota_used" | json_field replica_used)" = "0"

phase="complete"
echo "CLOUD_CONTROL_KIND_E2E_OK"
echo "real Kubernetes lifecycle Saga: provision -> resize -> delete"
echo "two-replica workflow lease contention: ok=$ok_count busy=$busy_count metric_busy=$busy_metric"
echo "Kubernetes objects and quota convergence: verified"
echo "two background Saga controllers automatic provision/delete convergence: verified"
