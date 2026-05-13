#!/bin/bash
set -e

BASE=http://localhost:9000

# Create session
SESSION=$(curl -sf -X POST $BASE/sessions | jq -r .id)
echo "Session: $SESSION"

# Add HTTP mock
curl -sf -X POST $BASE/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d '{"protocol":"http","match":{"method":"GET","path":"/users/1"},"response":{"status":200,"body":"{\"id\":1,\"name\":\"ada\"}"}}'

# Add Redis mock
curl -sf -X POST $BASE/sessions/$SESSION/mocks \
  -H "Content-Type: application/json" \
  -d '{"protocol":"redis","match":{"command":"GET","key":"user:1"},"response":"{\"id\":1}"}'

# Verify HTTP mock fires
RESULT=$(curl -sf http://localhost:8080/users/1)
echo "HTTP mock result: $RESULT"

# Verify Redis mock fires
RESULT=$(docker compose exec redis redis-cli -p 6380 GET user:1)
echo "Redis mock result: $RESULT"

# Check status
curl -sf $BASE/status | jq .

# Get traffic
curl -sf $BASE/sessions/$SESSION/traffic | jq .

# Reset session
curl -sf -X DELETE $BASE/sessions/$SESSION
echo "Session reset OK"
echo "All smoke tests passed"
