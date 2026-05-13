#!/bin/bash
set -e
BASE=http://localhost:9000

echo "=== Kafka Mock Example ==="
echo "Note: requires rpk (Redpanda CLI) — install: https://docs.redpanda.com/current/get-started/rpk-install/"

SESSION=$(curl -sf -X POST $BASE/sessions | jq -r .id)
echo "Session: $SESSION"

curl -sf -X POST $BASE/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d '{
    "protocol": "kafka",
    "match": { "topic": "orders" },
    "response": "{\"orderId\":1,\"status\":\"confirmed\"}"
  }' | jq .

echo "--- Reading from topic (mock message was produced) ---"
if command -v rpk &>/dev/null; then
  rpk topic consume orders --brokers localhost:9092 --num 1 --offset start 2>/dev/null | head -5
else
  echo "rpk not installed — check Redpanda UI or use: docker run --rm --network host edenhill/kcat:1.7.1 -b localhost:9092 -t orders -o beginning -e"
fi

echo "--- Traffic ---"
curl -sf $BASE/sessions/$SESSION/traffic | jq .

echo "--- Reset ---"
curl -sf -X DELETE $BASE/sessions/$SESSION
echo "Done."
