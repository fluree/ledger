# Block Consolidation/Rebuild for V2

## Overview
The rebuild scripts in this directory can be used to rebuild indexes from block files, which effectively consolidates multiple blocks into optimized index structures. When the database is smaller than the reindexing threshold, this creates minimal index points.

## Status
✅ All v1 rebuild scripts are already present in the v2 maintenance branch
✅ Core dependencies (reindex.clj, indexing.clj, storage.clj) exist in v2
✅ Ready to test with v2 data

## Files Available
- `index_rebuild.clj` - Main rebuild logic with configurable thresholds
- `run_rebuild.clj` - Command-line runner
- `verify_rebuild.clj` - Database verification
- `full_rebuild.clj` - Complete workflow
- `test_rebuild.clj` - Testing utilities
- `README.md` - Original v1 documentation
- `RAFT_ANALYSIS.md` - RAFT state analysis

## Quick Start

### 1. Create Test Data
First, create a small test ledger with known data:

```clojure
;; Start a v2 Fluree instance
;; Create a test ledger with some transactions
;; This gives us blocks to work with
```

### 2. Run Rebuild
```bash
clojure -M dev/ledger_toolkit/rebuild/run_rebuild.clj \
  --base-path /path/to/test/data \
  --network test \
  --dbid testdb \
  --max-mb 200
```

### 3. Verify Results
```bash
clojure -M dev/ledger_toolkit/rebuild/run_verify.clj \
  --base-path /path/to/test/data \
  --network test \
  --dbid testdb
```

## Expected Behavior
- If database < 200MB (default threshold): Creates 1 index point (essentially 1 consolidated state)
- If database > threshold: Creates multiple index points as needed
- Result: Optimized, queryable database

## V2-Specific Considerations

### Potential Differences from V1
1. **Flake structure** - V2 should be compatible (6-tuple: s, p, o, t, op, m)
2. **Block format** - Should be compatible (`:block`, `:t`, `:flakes`)
3. **Index structure** - Need to verify compatibility
4. **RAFT state** - Should be similar

### Testing Plan
1. Create simple test ledger with v2
2. Run rebuild on test data
3. Verify database is queryable
4. Compare results with original
5. Document any v2-specific issues/adjustments

## Next Steps
1. ✅ Verify all dependencies exist
2. 🔄 Create test data script
3. 🔄 Run test rebuild
4. 🔄 Document any v2 adjustments needed
