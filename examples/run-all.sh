#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Checking control plane is reachable..."
if ! curl -sf http://localhost:9000/status > /dev/null 2>&1; then
  echo "ERROR: Control plane not reachable at http://localhost:9000"
  echo "Start the stack first: cd deploy && docker compose up -d"
  exit 1
fi

echo "Control plane OK. Running all examples..."
echo ""

for protocol in http redis kafka mysql amqp amqp-rpc; do
  echo "=========================================="
  bash "$SCRIPT_DIR/$protocol/example.sh" || echo "WARNING: $protocol example failed"
  echo ""
done

echo "All examples complete."
