# Block Consolidation Testing Guide

## Overview
This guide explains how to test the block consolidation/rebuild functionality on Fluree v2.

## What We've Created

### 1. Test Data Generators
- **`create_test_data.clj`** - Small test dataset (7 blocks) for quick validation
- **`create_large_test_data.clj`** - Large test dataset (100+ blocks) for realistic testing
  - Creates 50 people (some deleted/re-added)
  - Creates 30 companies (10 deleted)
  - Creates 100 products
  - Multiple updates, deletions, and modifications across 112 blocks
  - Configured with low reindex thresholds to force multiple index points

### 2. Rebuild Scripts (Already in v2)
- **`index_rebuild.clj`** - Main rebuild logic from v1
- **`run_rebuild.clj`** - Command-line runner
- **`verify_rebuild.clj`** - Database verification
- **`full_rebuild.clj`** - Complete workflow

### 3. Test Workflow
- **`test_full_workflow.sh`** - Automated end-to-end test script

## Quick Start

### Option 1: Automated Full Test
```bash
# Run complete test workflow (builds jar, generates data, rebuilds, verifies)
./dev/ledger_toolkit/rebuild/test_full_workflow.sh /tmp/fluree-test
```

### Option 2: Step-by-Step

#### Step 1: Build the Uberjar
```bash
make uberjar
```

This creates `target/fluree-ledger.standalone.jar`

#### Step 2: Generate Test Data

**Small test (7 blocks):**
```bash
clojure -M dev/ledger_toolkit/rebuild/create_test_data.clj \
  --base-path /tmp/fluree-small-test \
  --network test \
  --dbid small
```

**Large test (100+ blocks):**
```bash
clojure -M dev/ledger_toolkit/rebuild/create_large_test_data.clj \
  --base-path /tmp/fluree-large-test \
  --network test \
  --dbid large
```

#### Step 3: Run Rebuild/Consolidation
```bash
clojure -M dev/ledger_toolkit/rebuild/run_rebuild.clj \
  --base-path /tmp/fluree-large-test \
  --network test \
  --dbid large \
  --max-mb 50
```

**Key Parameter:**
- `--max-mb` - Novelty threshold before triggering index write
  - Smaller value = more index points
  - Larger value = fewer index points (more consolidation)
  - If database < threshold, creates single consolidated state

#### Step 4: Verify Results
```bash
clojure -M dev/ledger_toolkit/rebuild/run_verify.clj \
  --base-path /tmp/fluree-large-test \
  --network test \
  --dbid large
```

## What the Test Validates

### Data Correctness
1. **Final counts** match expected state after all operations
2. **Deleted entities** are properly absent (e.g., Bob, Companies 21-30)
3. **Updated values** reflect final state (e.g., Alice's age = 32)
4. **Re-added entities** have correct final data

### Index Functionality
1. Database is queryable after rebuild
2. Queries return correct results
3. Index points are created appropriately
4. RAFT state is properly initialized

### Expected Results (Large Test)
After consolidation, the database should contain:
- **~49-50 people** (1 deleted, some re-added with different data)
- **20 companies** (10 deleted from original 30)
- **100 products** (all present with final update state)

## Troubleshooting

### Build Errors
If `make uberjar` fails, check:
- Java version (requires Java 11+)
- Dependencies are downloading correctly
- You're on the maintenance/v2 branch

### Test Data Generation Fails
- Check available disk space in /tmp
- Ensure ports 11010 and 9990 are available
- Look for errors in console output

### Rebuild Fails
Common issues:
- Missing block files - check `--base-path` points to parent of `ledger/` directory
- RAFT state issues - rebuild script initializes this automatically
- Memory issues - reduce `--parallelism` or increase JVM heap

### Verification Fails
- Wait longer for indexes to settle (add sleep if needed)
- Check RAFT logs in `{base-path}/group/`
- Verify block files are intact

## Understanding the Consolidation

### How It Works
1. Reads all blocks sequentially from disk
2. Applies flakes using `dbproto/-with` (handles add/retract logic)
3. Accumulates changes in "novelty" (in-memory uncommitted flakes)
4. When novelty exceeds threshold, writes index point
5. Final state is queryable via indexes

### Block vs Index
- **Blocks** - Immutable transaction history (never modified)
- **Indexes** - Optimized query structures (rebuilt from blocks)
- Rebuilding creates new indexes from existing blocks

### Consolidation Effect
When `max-mb` threshold is larger than database size:
- Only 1-2 index points created
- Effectively consolidates all state into minimal indexes
- Original blocks remain unchanged
- Achieves the "single consolidated state" goal

## Next Steps

1. ✅ Build uberjar: `make uberjar`
2. ✅ Run automated test: `./dev/ledger_toolkit/rebuild/test_full_workflow.sh`
3. 🔄 Review results and verify correctness
4. 🔄 Test with different `--max-mb` thresholds
5. 🔄 Test with production data (if available)
6. 🔄 Document any v2-specific issues or adjustments

## File Locations

- Test scripts: `/dev/ledger_toolkit/rebuild/`
- Generated data: `/tmp/fluree-*-test/`
- Block files: `{base-path}/ledger/{network}/{dbid}/block/`
- Index files: `{base-path}/ledger/{network}/{dbid}/root/`
- RAFT state: `{base-path}/group/`

## Performance Notes

- Large test takes ~2-5 minutes to generate data
- Rebuild time depends on block count and `--max-mb` setting
- Verification is quick (~10-30 seconds)
