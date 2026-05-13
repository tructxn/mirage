#!/bin/bash
set -e
BASE=http://localhost:9000

echo "=== MySQL Mock Example ==="
echo "Note: requires mysql-client"

SESSION=$(curl -sf -X POST $BASE/sessions | jq -r .id)
echo "Session: $SESSION"

curl -sf -X POST $BASE/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d "{
    \"protocol\": \"mysql\",
    \"match\": { \"table\": \"users\" },
    \"response\": \"{\\\"id\\\":1,\\\"name\\\":\\\"ada\\\"}\"
  }" | jq .

echo "--- Verifying mock row in mirage_mocks table ---"
if command -v mysql &>/dev/null; then
  mysql -h 127.0.0.1 -P 3307 -u root -pmirage mirage \
    -e "SELECT session_id, target_table, response_json FROM mirage_mocks WHERE session_id='$SESSION';" 2>/dev/null
else
  echo "mysql client not installed — check via: docker exec \$(docker ps -qf name=mysql) mysql -u root -pmirage mirage -e 'SELECT * FROM mirage_mocks;'"
fi

echo "--- Traffic ---"
curl -sf $BASE/sessions/$SESSION/traffic | jq .

echo "--- Reset ---"
curl -sf -X DELETE $BASE/sessions/$SESSION
echo "Done."
