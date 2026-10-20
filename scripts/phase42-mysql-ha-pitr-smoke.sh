#!/usr/bin/env sh
set -eu

DB="${PHASE42_DB:-taskflow_phase42}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
DEFAULT_DSN="taskflow:taskflow@tcp(mysql:3306)/taskflow?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
GOOD_DSN="taskflow:taskflow@tcp(mysql:3306)/${DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
BAD_DSN="taskflow:taskflow@tcp(mysql-does-not-exist:3306)/${DB}?charset=utf8mb4&parseTime=True&loc=Local&interpolateParams=true"
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

wait_replica_value() {
  query="$1"; want="$2"; i=0; got=""
  while [ "$i" -lt 300 ]; do
    got="$(docker compose exec -T mysql-replica mysql -uroot -proot -Nse "$query" 2>/dev/null | tr -d '\r' || true)"
    if [ "$got" = "$want" ]; then return 0; fi
    i=$((i + 1)); sleep 0.2
  done
  echo "replica value timeout got=$got want=$want query=$query" >&2
  return 1
}

cleanup() {
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "Phase 42 MySQL HA/PITR smoke failed during phase: $phase" >&2
    docker compose --profile phase42-smoke logs --tail 300 taskflow mysql mysql-replica mysql-recovery >&2 2>/dev/null || true
  fi
  MYSQL_DSN="$DEFAULT_DSN" MYSQL_FAILOVER_DSNS= MYSQL_CONNECT_TIMEOUT_MS=3000 \
    docker compose up -d --no-deps --force-recreate taskflow >/dev/null 2>&1 || true
  docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${DB};" >/dev/null 2>&1 || true
  docker compose --profile phase42-smoke rm -sf mysql-replica mysql-recovery >/dev/null 2>&1 || true
  project="${COMPOSE_PROJECT_NAME:-$(basename "$PWD")}"
  docker volume rm "${project}_mysql-replica-data" >/dev/null 2>&1 || true
  docker volume rm "${project}_mysql-recovery-data" >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT

phase="start GTID primary and replica"
docker compose stop -t 0 taskflow >/dev/null 2>&1 || true
project="${COMPOSE_PROJECT_NAME:-$(basename "$PWD")}"
docker compose --profile phase42-smoke rm -sf mysql-replica mysql-recovery >/dev/null 2>&1 || true
docker volume rm "${project}_mysql-replica-data" >/dev/null 2>&1 || true
docker volume rm "${project}_mysql-recovery-data" >/dev/null 2>&1 || true
docker compose --profile phase42-smoke up -d --force-recreate mysql mysql-replica mysql-recovery >/dev/null
wait_mysql mysql
wait_mysql mysql-replica
wait_mysql mysql-recovery
docker compose exec -T mysql mysql -uroot -proot -e "CREATE USER IF NOT EXISTS 'taskflow_repl'@'%' IDENTIFIED BY 'phase42-repl'; ALTER USER 'taskflow_repl'@'%' IDENTIFIED BY 'phase42-repl'; GRANT REPLICATION SLAVE ON *.* TO 'taskflow_repl'@'%'; FLUSH PRIVILEGES;" >/dev/null
docker compose exec -T mysql mysqldump -uroot -proot --single-transaction --set-gtid-purged=ON --databases taskflow > /tmp/phase42-replica-seed.sql
docker compose exec -T mysql-replica mysql -uroot -proot -e "STOP REPLICA; RESET REPLICA ALL; RESET BINARY LOGS AND GTIDS;" >/dev/null
docker compose exec -T mysql-replica mysql -uroot -proot < /tmp/phase42-replica-seed.sql
docker compose exec -T mysql-replica mysql -uroot -proot -e "SET sql_log_bin=0; CREATE USER IF NOT EXISTS 'taskflow'@'%' IDENTIFIED BY 'taskflow'; GRANT ALL PRIVILEGES ON taskflow.* TO 'taskflow'@'%'; SET sql_log_bin=1; FLUSH PRIVILEGES;" >/dev/null
docker compose exec -T mysql-replica mysql -uroot -proot -e "CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysql', SOURCE_PORT=3306, SOURCE_USER='taskflow_repl', SOURCE_PASSWORD='phase42-repl', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA;" >/dev/null
i=0
while [ "$i" -lt 300 ]; do
  status="$(docker compose exec -T mysql-replica mysql -uroot -proot -e 'SHOW REPLICA STATUS\G' 2>/dev/null || true)"
  if printf '%s\n' "$status" | grep -q 'Replica_IO_Running: Yes' && printf '%s\n' "$status" | grep -q 'Replica_SQL_Running: Yes'; then break; fi
  i=$((i + 1)); sleep 0.2
done
[ "$i" -lt 300 ]
docker compose exec -T mysql-replica mysql -uroot -proot -e "SET GLOBAL read_only=ON; SET GLOBAL super_read_only=ON;" >/dev/null

phase="prove GTID replication"
docker compose exec -T mysql mysql -uroot -proot -e "DROP DATABASE IF EXISTS ${DB}; CREATE DATABASE ${DB}; CREATE TABLE ${DB}.phase42_events(id BIGINT PRIMARY KEY, label VARCHAR(64) NOT NULL); INSERT INTO ${DB}.phase42_events VALUES (1,'baseline'); GRANT ALL PRIVILEGES ON ${DB}.* TO 'taskflow'@'%'; FLUSH PRIVILEGES;" >/dev/null
wait_replica_value "SELECT label FROM ${DB}.phase42_events WHERE id=1" baseline
gtid="$(docker compose exec -T mysql-replica mysql -uroot -proot -Nse 'SELECT @@GLOBAL.gtid_executed' 2>/dev/null | tr -d '\r')"
[ -n "$gtid" ]

phase="take verified consistent backup and capture PITR start position"
docker compose exec -T mysql mysql -uroot -proot -e "FLUSH LOGS" >/dev/null
docker compose exec -T mysql mysqldump -uroot -proot --single-transaction --set-gtid-purged=OFF --databases "$DB" > /tmp/phase42-base.sql
sha256sum /tmp/phase42-base.sql > /tmp/phase42-base.sha256
sha256sum -c /tmp/phase42-base.sha256 >/dev/null
status="$(docker compose exec -T mysql mysql -uroot -proot -Nse 'SHOW BINARY LOG STATUS' 2>/dev/null | tr -d '\r')"
binlog="$(printf '%s\n' "$status" | awk '{print $1}')"
start_pos="$(printf '%s\n' "$status" | awk '{print $2}')"
[ -n "$binlog" ] && [ -n "$start_pos" ]

phase="write post-backup transaction and replay binlog into isolated recovery database"
docker compose exec -T mysql mysql -uroot -proot -e "INSERT INTO ${DB}.phase42_events VALUES (2,'after-backup');" >/dev/null
wait_replica_value "SELECT label FROM ${DB}.phase42_events WHERE id=2" after-backup
status="$(docker compose exec -T mysql mysql -uroot -proot -Nse 'SHOW BINARY LOG STATUS' 2>/dev/null | tr -d '\r')"
end_binlog="$(printf '%s\n' "$status" | awk '{print $1}')"
end_pos="$(printf '%s\n' "$status" | awk '{print $2}')"
[ "$binlog" = "$end_binlog" ]
docker compose exec -T mysql-recovery mysql -uroot -proot -e "RESET BINARY LOGS AND GTIDS;" >/dev/null
docker compose exec -T mysql-recovery mysql -uroot -proot < /tmp/phase42-base.sql
docker compose exec -T mysql-recovery mysql -uroot -proot -e "CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysql', SOURCE_PORT=3306, SOURCE_USER='taskflow_repl', SOURCE_PASSWORD='phase42-repl', SOURCE_LOG_FILE='${binlog}', SOURCE_LOG_POS=${start_pos}, GET_SOURCE_PUBLIC_KEY=1; START REPLICA;" >/dev/null
i=0
while [ "$i" -lt 300 ]; do
  restored="$(docker compose exec -T mysql-recovery mysql -uroot -proot -Nse "SELECT COUNT(*) FROM ${DB}.phase42_events WHERE id=2 AND label='after-backup'" 2>/dev/null | tr -d '\r' || true)"
  if [ "$restored" = 1 ]; then break; fi
  i=$((i + 1)); sleep 0.2
done
[ "$i" -lt 300 ]
docker compose exec -T mysql-recovery mysql -uroot -proot -e "STOP REPLICA;" >/dev/null
recovery_status="$(docker compose exec -T mysql-recovery mysql -uroot -proot -e 'SHOW REPLICA STATUS\G' 2>/dev/null || true)"
exec_pos="$(printf '%s\n' "$recovery_status" | awk -F': ' '/Exec_Source_Log_Pos:/ {print $2; exit}' | tr -d '\r')"
[ -n "$exec_pos" ] && [ "$exec_pos" -ge "$end_pos" ]
[ "$(docker compose exec -T mysql-recovery mysql -uroot -proot -Nse "SELECT COUNT(*) FROM ${DB}.phase42_events WHERE label IN ('baseline','after-backup')" 2>/dev/null | tr -d '\r')" = 2 ]

phase="prove TaskFlow startup writer failover selects second candidate"
docker build -t taskflow-go:local --build-arg GOPROXY=https://goproxy.cn,direct . >/dev/null
MYSQL_DSN="$BAD_DSN" MYSQL_FAILOVER_DSNS="$GOOD_DSN" MYSQL_CONNECT_TIMEOUT_MS=500 \
  docker compose up -d --no-deps --force-recreate taskflow >/dev/null
wait_http "$BASE_URL/health/ready"
docker compose logs --no-color --tail 200 taskflow >/tmp/phase42-taskflow.log
grep -q 'mysql writer startup candidate selected=1' /tmp/phase42-taskflow.log

echo "Phase 42 MySQL GTID primary/replica replication: ok"
echo "Phase 42 verified backup plus binary-log position PITR restore: ok"
echo "Phase 42 TaskFlow writer startup DSN failover: ok"
