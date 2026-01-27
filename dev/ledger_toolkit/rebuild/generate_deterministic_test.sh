#!/bin/bash
# Generate deterministic test data with known final state
# Final state: 10 people, each with specific name and age

API_URL="${1:-http://localhost:8090}"
NETWORK="${2:-test}"
DBID="${3:-mydb}"

echo "=== Generating Deterministic Test Data ==="
echo "API: $API_URL"
echo "Ledger: $NETWORK/$DBID"
echo ""

echo "Creating ledger $NETWORK/$DBID..."
curl -s -X POST "$API_URL/fdb/new-ledger" \
  -H "Content-Type: application/json" \
  -d "{\"ledger/id\": \"$NETWORK/$DBID\"}"
echo ""
echo "Waiting for ledger to be ready..."
sleep 8

echo ""
echo "Creating schema (collection and predicates)..."
curl -s -X POST "$API_URL/fdb/$NETWORK/$DBID/transact" \
  -H "Content-Type: application/json" \
  -d '[
    {"_id": "_collection", "_collection/name": "person"},
    {"_id": "_predicate", "_predicate/name": "person/name", "_predicate/type": "string", "_predicate/unique": true},
    {"_id": "_predicate", "_predicate/name": "person/age", "_predicate/type": "int"}
  ]' > /dev/null
echo "Schema created"
sleep 2

echo ""
echo "Creating 10 people (blocks 1-10)..."
for i in $(seq 1 10); do
  curl -s -X POST "$API_URL/fdb/$NETWORK/$DBID/transact" \
    -H "Content-Type: application/json" \
    -d "[{\"_id\": \"person\", \"person/name\": \"Person$i\", \"person/age\": 20}]" > /dev/null
  echo "  Block $i: Created Person$i with age 20"
  sleep 0.1
done

echo ""
echo "Updating ages to create retractions..."
# Update each person's age multiple times, creating retractions
# Each update replaces the previous age value
for round in $(seq 1 7); do
  for person_id in $(seq 1 10); do
    new_age=$((20 + round * 5))
    curl -s -X POST "$API_URL/fdb/$NETWORK/$DBID/transact" \
      -H "Content-Type: application/json" \
      -d "[{\"_id\": [\"person/name\", \"Person$person_id\"], \"person/age\": $new_age}]" > /dev/null

    if [ $((person_id % 10)) -eq 0 ] && [ $((round % 2)) -eq 0 ]; then
      echo "  Round $round: Updated all people to age $new_age"
    fi
    sleep 0.05
  done
done

echo "Waiting for blocks to be written..."
sleep 5

echo ""
echo "=== Expected Final State ==="
echo "Total people: 10"
echo "Each person should have:"
echo "  - person/name: Person1 through Person10"
echo "  - person/age: 55 (20 + 7*5)"
echo ""
echo "Expected flake count:"
echo "  - 10 _id flakes (one per person)"
echo "  - 10 person/name flakes (one per person)"
echo "  - 10 person/age flakes (one per person, final value after all retractions)"
echo "  - Total: 30 user flakes"
echo ""

echo "=== Querying Actual Final State ==="
curl -s -X POST "$API_URL/fdb/$NETWORK/$DBID/query" \
  -H "Content-Type: application/json" \
  -d "{\"select\": [\"*\"], \"from\": \"person\"}" | jq '. | length' 2>/dev/null | xargs echo "Total people:"

curl -s -X POST "$API_URL/fdb/$NETWORK/$DBID/block" \
  -H "Content-Type: application/json" \
  -d "latest" | jq '.block' 2>/dev/null | xargs echo "Latest block:"

echo ""
echo "=== Test Data Generated ==="
