#!/bin/bash
#
# Full workflow test for block consolidation/rebuild
#
# This script:
# 1. Builds the Fluree uberjar
# 2. Generates test data (100+ blocks)
# 3. Runs the rebuild script
# 4. Verifies the results
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
BASE_PATH="${1:-/tmp/fluree-rebuild-test}"
NETWORK="test"
DBID="large"
MAX_MB="50"  # Set consolidation threshold - adjust based on test data size

echo -e "${GREEN}=== Fluree Block Consolidation Test ===${NC}"
echo "Base path: $BASE_PATH"
echo "Network: $NETWORK"
echo "DB ID: $DBID"
echo "Consolidation threshold: ${MAX_MB}MB"
echo ""

# Step 1: Build uberjar
echo -e "${YELLOW}Step 1: Building Fluree uberjar...${NC}"
if [ ! -f "target/fluree-ledger.standalone.jar" ]; then
    echo "Building uberjar (this may take a few minutes)..."
    make uberjar
    echo -e "${GREEN}✓ Uberjar built${NC}"
else
    echo -e "${GREEN}✓ Uberjar already exists${NC}"
fi
echo ""

# Step 2: Generate test data
echo -e "${YELLOW}Step 2: Generating test data (100+ blocks)...${NC}"
echo "This will create a ledger with:"
echo "  - 50 people (some deleted and re-added)"
echo "  - 30 companies (10 deleted)"
echo "  - 100 products"
echo "  - Multiple updates, deletions, and re-additions"
echo "  - Multiple index points due to low reindex thresholds"
echo ""
clojure -M dev/ledger_toolkit/rebuild/create_large_test_data.clj \
    --base-path "$BASE_PATH" \
    --network "$NETWORK" \
    --dbid "$DBID"

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Test data generation failed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Test data generated${NC}"
echo ""

# Check block files
BLOCK_DIR="$BASE_PATH/ledger/$NETWORK/$DBID/block"
BLOCK_COUNT=$(find "$BLOCK_DIR" -name "*.fdbd" 2>/dev/null | wc -l | tr -d ' ')
echo "Block files created: $BLOCK_COUNT"
echo ""

# Step 3: Run rebuild/consolidation
echo -e "${YELLOW}Step 3: Running rebuild script...${NC}"
echo "This will:"
echo "  - Read all $BLOCK_COUNT blocks"
echo "  - Apply flakes sequentially"
echo "  - Create consolidated indexes"
echo "  - Threshold: ${MAX_MB}MB (will create multiple index points if data exceeds this)"
echo ""

clojure -M dev/ledger_toolkit/rebuild/run_rebuild.clj \
    --base-path "$BASE_PATH" \
    --network "$NETWORK" \
    --dbid "$DBID" \
    --max-mb "$MAX_MB"

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Rebuild failed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Rebuild completed${NC}"
echo ""

# Check index files
INDEX_DIR="$BASE_PATH/ledger/$NETWORK/$DBID"
if [ -d "$INDEX_DIR/root" ]; then
    ROOT_COUNT=$(find "$INDEX_DIR/root" -name "*.fdbd" 2>/dev/null | wc -l | tr -d ' ')
    echo "Root/index files created: $ROOT_COUNT"
fi
echo ""

# Step 4: Verify results
echo -e "${YELLOW}Step 4: Verifying rebuild results...${NC}"
clojure -M dev/ledger_toolkit/rebuild/run_verify.clj \
    --base-path "$BASE_PATH" \
    --network "$NETWORK" \
    --dbid "$DBID"

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Verification failed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Verification passed${NC}"
echo ""

# Summary
echo -e "${GREEN}=== Test Complete ===${NC}"
echo "Test location: $BASE_PATH"
echo "Original blocks: $BLOCK_COUNT"
echo "Index points created: $ROOT_COUNT"
echo ""
echo "Expected final state:"
echo "  - ~49-50 people (some deleted and re-added)"
echo "  - 20 companies (10 were deleted from original 30)"
echo "  - 100 products"
echo ""
echo "To manually inspect:"
echo "  Block files: $BLOCK_DIR"
echo "  Index files: $INDEX_DIR"
echo ""
echo -e "${GREEN}All tests passed!${NC}"
