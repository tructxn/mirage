#!/bin/bash
set -e
BASE=http://localhost:9000

echo "=== HTTP Mock Example ==="

SESSION=$(curl -sf -X POST $BASE/sessions | jq -r .id)
echo "Session: $SESSION"

curl -sf -X POST $BASE/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d '{
    "protocol": "http",
    "match": { "method": "GET", "path": "/hello" },
    "response": { "status": 200, "body": "{\"message\":\"hello from mirage\"}" }
  }' | jq .

echo "--- Triggering mock ---"
RESULT=$(curl -sf http://localhost:8080/hello)
echo "Response: $RESULT"

echo "--- Traffic ---"
curl -sf $BASE/sessions/$SESSION/traffic | jq .

echo "--- Reset ---"
curl -sf -X DELETE $BASE/sessions/$SESSION
echo "Done."
