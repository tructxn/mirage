#!/bin/bash
set -e
BASE=http://localhost:9000

echo "=== AMQP Fire-and-Forget Example ==="
echo "Publishes a message to an exchange when PushRule is called."
echo "Requires RabbitMQ management UI at http://localhost:15673 (guest/guest)"

SESSION=$(curl -sf -X POST $BASE/sessions | jq -r .id)
echo "Session: $SESSION"

curl -sf -X POST $BASE/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d '{
    "protocol": "amqp",
    "match": {
      "exchange": "events",
      "routing_key": "user.created"
    },
    "response": "{\"userId\":1,\"event\":\"user.created\"}"
  }' | jq .

echo "--- Message published to exchange 'events' with routing key 'user.created' ---"
echo "--- Verify via RabbitMQ management: http://localhost:15673 ---"

echo "--- Traffic ---"
curl -sf $BASE/sessions/$SESSION/traffic | jq .

echo "--- Reset ---"
curl -sf -X DELETE $BASE/sessions/$SESSION
echo "Done."
