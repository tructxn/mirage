#!/bin/bash
set -e
BASE=http://localhost:9000

echo "=== Redis Mock Example ==="

SESSION=$(curl -sf -X POST $BASE/sessions | jq -r .id)
echo "Session: $SESSION"

curl -sf -X POST $BASE/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d '{
    "protocol": "redis",
    "match": { "command": "GET", "key": "user:1" },
    "response": "{\"id\":1,\"name\":\"ada\"}"
  }' | jq .

echo "--- Triggering mock ---"
RESULT=$(redis-cli -h localhost -p 6380 GET user:1)
echo "Response: $RESULT"

echo "--- Traffic ---"
curl -sf $BASE/sessions/$SESSION/traffic | jq .

echo "--- Reset ---"
curl -sf -X DELETE $BASE/sessions/$SESSION
echo "Done."
