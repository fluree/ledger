#!/bin/bash
# Generate test transactions via HTTP API

API_URL="${1:-http://localhost:8090}"
NETWORK="${2:-test}"
DBID="${3:-mydb}"
NUM_BLOCKS="${4:-100}"

echo "=== Generating Test Data ==="
echo "API: $API_URL"
echo "Ledger: $NETWORK/$DBID"
echo "Target blocks: $NUM_BLOCKS"
echo ""

# Generate many small transactions
echo "Generating $NUM_BLOCKS transactions..."
for i in $(seq 1 $NUM_BLOCKS); do
  # Every 10th: add a new person
  if [ $((i % 10)) -eq 0 ]; then
    curl -s -X POST "$API_URL/fdb/$NETWORK/$DBID/transact" \
      -H "Content-Type: application/json" \
      -d "[{\"_id\": \"person\", \"person/name\": \"Person$i\", \"person/age\": $((20 + i % 50))}]" > /dev/null

    if [ $((i % 20)) -eq 0 ]; then
      echo "  Block ~$i: Added Person$i"
    fi

  # Every 5th: update a person
  elif [ $((i % 5)) -eq 0 ] && [ $i -gt 10 ]; then
    target=$((i - 10))
    curl -s -X POST "$API_URL/fdb/$NETWORK/$DBID/transact" \
      -H "Content-Type: application/json" \
      -d "[{\"_id\": [\"person/name\", \"Person$target\"], \"person/age\": $((25 + i % 40))}]" > /dev/null

  # Other: small update
  elif [ $i -gt 20 ]; then
    target=$((i - (i % 10)))
    curl -s -X POST "$API_URL/fdb/$NETWORK/$DBID/transact" \
      -H "Content-Type: application/json" \
      -d "[{\"_id\": [\"person/name\", \"Person$target\"], \"person/age\": $((30 + i % 30))}]" > /dev/null
  fi

  # Small delay to not overwhelm
  sleep 0.05
done

echo ""
echo "=== Querying final state ==="
curl -s -X POST "$API_URL/fdb/$NETWORK/$DBID/query" \
  -H "Content-Type: application/json" \
  -d "{\"select\": [\"*\"], \"from\": \"person\"}" | jq '. | length' 2>/dev/null | xargs echo "Total people:"

curl -s -X POST "$API_URL/fdb/$NETWORK/$DBID/block" \
  -H "Content-Type: application/json" \
  -d "latest" | jq '.block' 2>/dev/null | xargs echo "Latest block:"

echo ""
echo "=== Test Data Generated ==="
echo "Now you can:"
echo "1. Stop the server (Ctrl+C)"
echo "2. Run: clojure -M dev/ledger_toolkit/rebuild/run_rebuild.clj --base-path /tmp/fluree-test-data --network $NETWORK --dbid $DBID"
