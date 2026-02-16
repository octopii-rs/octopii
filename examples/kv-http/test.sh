#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "=== Cleaning up ==="
docker compose down -v 2>/dev/null || true

echo "=== Building ==="
docker compose build

echo "=== Starting cluster ==="
docker compose up -d

echo "=== Waiting for cluster to form (15s) ==="
sleep 15

echo "=== Health checks ==="
echo "Node 1:"
curl -sf http://localhost:8001/health | jq .
echo "Node 2:"
curl -sf http://localhost:8002/health | jq .
echo "Node 3:"
curl -sf http://localhost:8003/health | jq .

echo ""
echo "=== PUT test ==="
curl -sf -X PUT http://localhost:8001/kv/test-key \
  -H "Content-Type: application/json" \
  -d '{"value":"test-value"}' | jq .

echo ""
echo "=== GET from all nodes ==="
echo "Node 1:"
curl -sf http://localhost:8001/kv/test-key | jq .
echo "Node 2:"
curl -sf http://localhost:8002/kv/test-key | jq .
echo "Node 3:"
curl -sf http://localhost:8003/kv/test-key | jq .

echo ""
echo "=== Multiple SET/GET tests ==="
for i in 1 2 3 4 5; do
  curl -sf -X PUT "http://localhost:8001/kv/key$i" \
    -H "Content-Type: application/json" \
    -d "{\"value\":\"value$i\"}" > /dev/null
done
echo "SET 5 keys"

echo "Verify all keys from different nodes:"
for i in 1 2 3 4 5; do
  NODE=$((($i % 3) + 1))
  PORT=$((8000 + $NODE))
  VALUE=$(curl -sf "http://localhost:$PORT/kv/key$i" | jq -r '.value')
  if [ "$VALUE" = "value$i" ]; then
    echo "  key$i from node$NODE: OK"
  else
    echo "  key$i from node$NODE: FAIL (expected value$i, got $VALUE)"
    exit 1
  fi
done

echo ""
echo "=== Overwrite test ==="
curl -sf -X PUT http://localhost:8001/kv/key1 \
  -H "Content-Type: application/json" \
  -d '{"value":"updated-value"}' > /dev/null
VALUE=$(curl -sf http://localhost:8002/kv/key1 | jq -r '.value')
if [ "$VALUE" = "updated-value" ]; then
  echo "Overwrite: OK"
else
  echo "Overwrite: FAIL (expected updated-value, got $VALUE)"
  exit 1
fi

echo ""
echo "=== DELETE test ==="
curl -sf -X DELETE http://localhost:8001/kv/key1 | jq .

echo ""
echo "=== Verify deleted ==="
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8001/kv/key1)
if [ "$HTTP_CODE" = "404" ]; then
  echo "OK: Key deleted (404)"
else
  echo "FAIL: Expected 404, got $HTTP_CODE"
  exit 1
fi

echo ""
echo "=== Cleanup ==="
docker compose down -v

echo ""
echo "=== ALL TESTS PASSED ==="
