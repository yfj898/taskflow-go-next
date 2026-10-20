#!/usr/bin/env sh
set -eu

IMAGE="${IMAGE:-taskflow-go:perf}"
CASE="${CASE:-taskflow-perf}"
HTTP_PORT="${HTTP_PORT:-18080}"
PPROF_PORT="${PPROF_PORT:-16060}"
CONCURRENCY="${CONCURRENCY:-100}"
DURATION="${DURATION:-15}"
WARMUP_CONCURRENCY="${WARMUP_CONCURRENCY:-4}"
WARMUP_DURATION="${WARMUP_DURATION:-1}"
PROFILE_SECONDS="${PROFILE_SECONDS:-10}"
OUTPUT_DIR="${PERF_OUTPUT_DIR:-/tmp/taskflow-perf}"
APP_LOG="${OUTPUT_DIR}/app.log"
MYSQL_DSN_OPTIONS="${MYSQL_DSN_OPTIONS:-}"
ACCESS_LOG="${ACCESS_LOG:-false}"

NET="${CASE}-net"
APP="${CASE}-app"
EXECUTOR="${CASE}-exec"
MYSQL="${CASE}-mysql"
REDIS="${CASE}-redis"

mkdir -p "$OUTPUT_DIR"

cleanup() {
  for container in "$APP" "$EXECUTOR" "$MYSQL" "$REDIS"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker network create "$NET" >/dev/null
docker run -d --name "$MYSQL" --network "$NET" \
  -e MYSQL_DATABASE=taskflow \
  -e MYSQL_USER=taskflow \
  -e MYSQL_PASSWORD=taskflow \
  -e MYSQL_ROOT_PASSWORD=root \
  mysql:8.4 >/dev/null
docker run -d --name "$REDIS" --network "$NET" redis:7.4-alpine >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$MYSQL" mysqladmin ping -h 127.0.0.1 -uroot -proot --silent >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$MYSQL" mysqladmin ping -h 127.0.0.1 -uroot -proot --silent >/dev/null

docker run -d --name "$EXECUTOR" --network "$NET" \
  -e EXECUTOR_GRPC_LISTEN=:9091 \
  "$IMAGE" /executor-worker >/dev/null

docker run -d --name "$APP" --network "$NET" \
  -p "127.0.0.1:${HTTP_PORT}:8080" \
  -p "127.0.0.1:${PPROF_PORT}:6060" \
  -e HTTP_ADDR=:8080 \
  -e PPROF_ADDR=:6060 \
  -e MYSQL_DSN="taskflow:taskflow@tcp(${MYSQL}:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local${MYSQL_DSN_OPTIONS}" \
  -e MYSQL_MAX_OPEN_CONNS="${MYSQL_MAX_OPEN_CONNS:-50}" \
  -e MYSQL_MAX_IDLE_CONNS="${MYSQL_MAX_IDLE_CONNS:-10}" \
  -e MYSQL_SKIP_DEFAULT_TX="${MYSQL_SKIP_DEFAULT_TX:-true}" \
  -e REDIS_ADDR="${REDIS}:6379" \
  -e QUEUE_BACKEND=redis \
  -e EXECUTION_BACKEND=grpc \
  -e EXECUTOR_GRPC_TARGET="${EXECUTOR}:9091" \
  -e EXECUTOR_TIMEOUT_MS=5000 \
  -e WORKER_COUNT=32 \
  -e MAX_RETRIES=3 \
  -e OUTBOX_INTERVAL_MS=200 \
  -e RECONCILE_SEC=5 \
  -e STALE_RUNNING_SEC=60 \
  -e QUEUE_DLQ_ARCHIVE_DIR=/var/lib/taskflow/dlq-archive \
  -e RATE_LIMIT_RPS=10000 \
  -e RATE_LIMIT_BURST=20000 \
  -e OTEL_LOG_SPANS=false \
  -e ACCESS_LOG="$ACCESS_LOG" \
  "$IMAGE" >/dev/null

for _ in $(seq 1 30); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${HTTP_PORT}/health/ready" || true)" = 200 ]; then
    break
  fi
  sleep 1
done
[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${HTTP_PORT}/health/ready" || true)" = 200 ]

python3 scripts/perf-load.py "$HTTP_PORT" "$WARMUP_CONCURRENCY" "$WARMUP_DURATION" >"${OUTPUT_DIR}/warmup.json"
python3 scripts/perf-load.py "$HTTP_PORT" "$CONCURRENCY" "$DURATION" >"${OUTPUT_DIR}/load.json" &
LOAD_PID=$!

sleep 5
curl -fsS "http://127.0.0.1:${PPROF_PORT}/debug/pprof/profile?seconds=${PROFILE_SECONDS}" \
  -o "${OUTPUT_DIR}/cpu.pprof" &
PROFILE_PID=$!

sleep 5
docker stats --no-stream \
  --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.NetIO}}|{{.BlockIO}}' \
  "$APP" "$EXECUTOR" "$MYSQL" "$REDIS" >"${OUTPUT_DIR}/docker-stats.txt"

docker logs "$APP" >"$APP_LOG" 2>&1 || true

wait "$PROFILE_PID"
wait "$LOAD_PID"
curl -fsS "http://127.0.0.1:${PPROF_PORT}/debug/pprof/heap?gc=1" -o "${OUTPUT_DIR}/heap.pprof"
curl -fsS "http://127.0.0.1:${PPROF_PORT}/debug/pprof/goroutine?debug=1" -o "${OUTPUT_DIR}/goroutine.txt"

cat "${OUTPUT_DIR}/load.json"
cat "${OUTPUT_DIR}/docker-stats.txt"
printf 'profiles: %s/cpu.pprof %s/heap.pprof\n' "$OUTPUT_DIR" "$OUTPUT_DIR"

