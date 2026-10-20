#!/usr/bin/env sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"

restore_redis() {
  docker compose start redis >/dev/null 2>&1 || true
}
trap restore_redis EXIT

echo "stopping redis to simulate broker outage"
docker compose stop -t 0 redis >/dev/null

response="$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"name":"outbox-recovery","type":"compute","payload":"durable"}')"
echo "$response"

task_id="$(printf '%s' "$response" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')"
if [ -z "$task_id" ]; then
  echo "failed to parse task id" >&2
  exit 1
fi

atomic_rows="$(docker compose exec -T mysql mysql -N -uroot -proot taskflow -e \
  "SELECT CONCAT((SELECT COUNT(*) FROM tasks WHERE id=$task_id),':',(SELECT COUNT(*) FROM outbox_events WHERE task_id=$task_id AND published_at IS NULL));" \
  2>/dev/null)"
echo "atomic rows: $atomic_rows"
test "$atomic_rows" = "1:1"

down_state=""
i=0
while [ "$i" -lt 15 ]; do
  down_state="$(docker compose exec -T mysql mysql -N -uroot -proot taskflow -e \
    "SELECT CONCAT(t.status,':',o.attempts,':',IF(o.published_at IS NULL,1,0)) FROM tasks t JOIN outbox_events o ON o.task_id=t.id WHERE t.id=$task_id;" \
    2>/dev/null)"
  if printf '%s' "$down_state" | grep -Eq '^pending:[1-9][0-9]*:1$'; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
echo "broker-down state: $down_state"
printf '%s' "$down_state" | grep -Eq '^pending:[1-9][0-9]*:1$'

echo "starting redis"
docker compose start redis >/dev/null

i=0
while [ "$i" -lt 40 ]; do
  task="$(curl -fsS "$BASE_URL/api/v1/tasks/$task_id")"
  echo "$task"
  case "$task" in
    *'"status":"success"'*) break ;;
    *'"status":"failed"'*) exit 1 ;;
  esac
  i=$((i + 1))
  sleep 1
done

case "$task" in
  *'"status":"success"'*) ;;
  *) echo "task did not recover after redis restart" >&2; exit 1 ;;
esac

final_state="$(docker compose exec -T mysql mysql -N -uroot -proot taskflow -e \
  "SELECT CONCAT(IF(published_at IS NOT NULL,1,0),':',attempts) FROM outbox_events WHERE task_id=$task_id;" \
  2>/dev/null)"
echo "outbox final: $final_state"
published="${final_state%%:*}"
attempts="${final_state#*:}"
test "$published" = "1"
test "$attempts" -ge 2

echo "outbox recovery smoke passed"
