#!/usr/bin/env sh
set -eu

DB="${PHASE45_DB:-taskflow_phase45}"
QUEUE_KEY="${PHASE45_QUEUE_KEY:-taskflow:phase45:queue}"
GLOBAL_URL="${PHASE45_GLOBAL_URL:-http://127.0.0.1:18090}"
REGION_B_URL="${PHASE45_REGION_B_URL:-http://127.0.0.1:18091}"
PRIMARY_DSN="taskflow:taskflow@tcp(mysql:3306)/${DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
REGION_B_DSN="taskflow:taskflow@tcp(mysql-replica:3306)/${DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
phase="initialization"

wait_mysql() {
  service="$1"; i=0
  while [ "$i" -lt 300 ]; do
    if docker compose exec -T "$service" mysql -h127.0.0.1 -uroot -proot -Nse 'SELECT 1' >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "timeout waiting for $service" >&2
  return 1
}

wait_http() {
  url="$1"; i=0
  while [ "$i" -lt 300 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "timeout waiting for $url" >&2
  return 1
}

wait_replica_count() {
  id="$1"; i=0; got=0
  while [ "$i" -lt 300 ]; do
    got="$(docker compose exec -T mysql-replica mysql -uroot -proot -Nse "SELECT COUNT(*) FROM ${DB}.tasks WHERE id=${id}" 2>/dev/null | tr -d '\r' || true)"
    if [ "$got" = 1 ]; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "replica did not receive task id=$id" >&2
  return 1
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 45 multi-region failover smoke failed during phase: $phase" >&2
    docker compose --profile phase45-smoke logs --tail 300 taskflow taskflow-region-b global-router mysql mysql-replica >&2 2>/dev/null || true
  fi
  replica_id="$(docker compose --profile phase45-smoke ps -aq mysql-replica 2>/dev/null || true)"
  replica_volume=""
  if [ -n "$replica_id" ]; then
    replica_volume="$(docker inspect "$replica_id" --format '{{range .Mounts}}{{if eq .Destination "/var/lib/mysql"}}{{.Name}}{{end}}{{end}}' 2>/dev/null || true)"
  fi
  docker compose --profile phase45-smoke rm -sf global-router taskflow-region-b mysql-replica >/dev/null 2>&1 || true
  if [ -n "$replica_volume" ]; then docker volume rm "$replica_volume" >/dev/null 2>&1 || true; fi
  docker compose up -d mysql redis >/dev/null 2>&1 || true
  wait_mysql mysql >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${DB};" >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$QUEUE_KEY" >/dev/null 2>&1 || true
  MYSQL_DSN="$DEFAULT_DSN" MYSQL_FAILOVER_DSNS= REDIS_QUEUE_KEY=taskflow:queue EXECUTION_BACKEND=grpc WORKER_COUNT=8 CONTROL_PLANE_AUTH_ENABLED=false \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="build image and establish Region A to Region B GTID replication"
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
docker compose --profile phase45-smoke up -d --force-recreate mysql mysql-replica redis >/dev/null
wait_mysql mysql
wait_mysql mysql-replica
docker compose exec -T mysql mysql -uroot -proot -e "CREATE USER IF NOT EXISTS 'taskflow_repl'@'%' IDENTIFIED BY 'phase45-repl'; ALTER USER 'taskflow_repl'@'%' IDENTIFIED BY 'phase45-repl'; GRANT REPLICATION SLAVE ON *.* TO 'taskflow_repl'@'%'; FLUSH PRIVILEGES;" >/dev/null
docker compose exec -T mysql mysqldump -uroot -proot --single-transaction --set-gtid-purged=ON --databases taskflow > /tmp/phase45-replica-seed.sql
docker compose exec -T mysql-replica mysql -uroot -proot -e "STOP REPLICA; RESET REPLICA ALL; RESET BINARY LOGS AND GTIDS;" >/dev/null
docker compose exec -T mysql-replica mysql -uroot -proot < /tmp/phase45-replica-seed.sql
docker compose exec -T mysql-replica mysql -uroot -proot -e "SET sql_log_bin=0; CREATE USER IF NOT EXISTS 'taskflow'@'%' IDENTIFIED BY 'taskflow'; GRANT ALL PRIVILEGES ON taskflow.* TO 'taskflow'@'%'; SET sql_log_bin=1; FLUSH PRIVILEGES;" >/dev/null
docker compose exec -T mysql-replica mysql -uroot -proot -e "CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysql', SOURCE_PORT=3306, SOURCE_USER='taskflow_repl', SOURCE_PASSWORD='phase45-repl', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA;" >/dev/null
i=0
while [ "$i" -lt 300 ]; do
  status="$(docker compose exec -T mysql-replica mysql -uroot -proot -e 'SHOW REPLICA STATUS\G' 2>/dev/null || true)"
  if printf '%s\n' "$status" | grep -q 'Replica_IO_Running: Yes' && printf '%s\n' "$status" | grep -q 'Replica_SQL_Running: Yes'; then break; fi
  i=$((i + 1)); sleep 0.2
done
[ "$i" -lt 300 ]
docker compose exec -T mysql-replica mysql -uroot -proot -e "SET GLOBAL read_only=ON; SET GLOBAL super_read_only=ON;" >/dev/null

docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${DB}; CREATE DATABASE ${DB}; GRANT ALL PRIVILEGES ON ${DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES;" >/dev/null

phase="start Region A, warm Region B and global primary-backup traffic route"
MYSQL_DSN="$PRIMARY_DSN" MYSQL_FAILOVER_DSNS= REDIS_QUEUE_KEY="$QUEUE_KEY" EXECUTION_BACKEND=local WORKER_COUNT=1 CONTROL_PLANE_AUTH_ENABLED=false CONTROL_PLANE_AUDIT_RETENTION_ENABLED=false \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_http http://127.0.0.1:8080/health/ready
PHASE45_REGION_B_DSN="$PRIMARY_DSN" PHASE45_QUEUE_KEY="$QUEUE_KEY" docker compose --profile phase45-smoke up -d --no-deps --force-recreate taskflow-region-b >/dev/null
wait_http "$REGION_B_URL/health/ready"
docker compose --profile phase45-smoke up -d --no-deps --force-recreate global-router >/dev/null
wait_http "$GLOBAL_URL/health/ready"

phase="write durable Region A marker and prove zero-RPO copy exists in Region B"
before="$(curl -fsS -X POST "$GLOBAL_URL/api/v1/tasks" -H 'Content-Type: application/json' -d '{"name":"phase45-before-loss","type":"compute","payload":"region-a"}')"
before_id="$(printf '%s' "$before" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
wait_replica_count "$before_id"

phase="fence failed Region A writer and promote Region B"
failover_started="$(date +%s)"
docker compose stop -t 0 taskflow mysql >/dev/null
docker compose exec -T mysql-replica mysql -uroot -proot -e "STOP REPLICA; SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF;" >/dev/null
PHASE45_REGION_B_DSN="$REGION_B_DSN" PHASE45_QUEUE_KEY="$QUEUE_KEY" docker compose --profile phase45-smoke up -d --no-deps --force-recreate taskflow-region-b >/dev/null
wait_http "$REGION_B_URL/health/ready"
docker compose --profile phase45-smoke up -d --no-deps --force-recreate global-router >/dev/null
wait_http "$GLOBAL_URL/health/ready"

phase="prove same global endpoint reads old state and accepts new Region B writes"
curl -fsS "$GLOBAL_URL/api/v1/tasks/$before_id" >/tmp/phase45-old-task.json
python3 -c 'import json; d=json.load(open("/tmp/phase45-old-task.json")); assert d["name"]=="phase45-before-loss"'
after="$(curl -fsS -X POST "$GLOBAL_URL/api/v1/tasks" -H 'Content-Type: application/json' -d '{"name":"phase45-after-promotion","type":"compute","payload":"region-b"}')"
after_id="$(printf '%s' "$after" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
[ "$(docker compose exec -T mysql-replica mysql -uroot -proot -Nse "SELECT COUNT(*) FROM ${DB}.tasks WHERE id=${after_id} AND name='phase45-after-promotion'" 2>/dev/null | tr -d '\r')" = 1 ]
failover_finished="$(date +%s)"
rto=$((failover_finished - failover_started))
[ "$rto" -le 60 ]
printf '{"rpo_lost_tasks":0,"rto_seconds":%s,"pre_failure_task_id":%s,"post_promotion_task_id":%s}\n' "$rto" "$before_id" "$after_id" >/tmp/phase45-failover-evidence.json

echo "Phase 45 Region A fencing, GTID Region B promotion and zero-RPO durable marker: ok"
echo "Phase 45 global primary-backup endpoint recovered with RTO=${rto}s and accepted post-promotion writes: ok"
