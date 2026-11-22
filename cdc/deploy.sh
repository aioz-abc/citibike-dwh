#!/usr/bin/env bash
# Создаёт/обновляет коннекторы Kafka Connect. Секреты — только из переменных окружения:
#   export PG_HOST=... PG_PORT=5432 PG_USER=ozbekova PG_PASS=...   CONNECT_URL=http://<host>:8083
#   bash cdc/deploy.sh
# Плейсхолдеры ${...} в JSON подставляются в момент отправки; в файлы пароль не попадает.
set -euo pipefail
cd "$(dirname "$0")"
: "${CONNECT_URL:=http://localhost:8083}"
: "${PG_HOST:?задайте PG_HOST}" "${PG_PORT:=5432}" "${PG_USER:?задайте PG_USER}" "${PG_PASS:?задайте PG_PASS}"
export PG_HOST PG_PORT PG_USER PG_PASS

for name in maintenance_tickets-source-ozbekova sink-ozbekova; do
  echo "==> $name"
  python3 -c 'import os,sys; sys.stdout.write(os.path.expandvars(open(sys.argv[1]).read()))' "$name.json" \
    | curl -sS -f -X PUT -H "Content-Type: application/json" \
        --data @- "$CONNECT_URL/connectors/$name/config" > /dev/null
done

sleep 5
for name in maintenance_tickets-source-ozbekova sink-ozbekova; do
  curl -sS "$CONNECT_URL/connectors/$name/status"; echo
done
