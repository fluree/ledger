# Ledger Toolkit

Tools for maintaining, rebuilding, and compressing Fluree v2 ledgers.

## Overview

This toolkit provides utilities for:
- **Block Compression**: Consolidate multiple blocks into fewer blocks, removing assertion+retraction pairs that cancel out
- **Index Rebuilding**: Rebuild indexes from block files with configurable memory thresholds
- **RAFT State Management**: Update RAFT consensus state after rebuilding
- **Verification**: Verify rebuilt ledgers are queryable and correct

## Directory Structure

```
ledger_toolkit/
├── compress_blocks.clj          # Block compression CLI
├── block_processor/             # Block processing internals
│   ├── incremental_reader.clj   # Reads blocks from disk
│   ├── optimized_incremental.clj # Core compression engine
│   ├── optimized_state.clj      # Tracks assertions/retractions
│   ├── generator.clj            # Writes output blocks
│   ├── metadata.clj             # Block metadata handling
│   └── stats.clj                # Compression statistics
└── rebuild/                     # Index rebuild tools
    ├── index_rebuild.clj        # Main rebuild logic
    ├── run_rebuild.clj          # CLI runner for rebuild
    ├── full_rebuild.clj         # Complete workflow (RAFT + rebuild + verify)
    ├── verify_rebuild.clj       # Verification queries
    ├── run_verify.clj           # CLI runner for verify
    ├── register_ledger_in_raft.clj  # Register ledger in RAFT
    ├── analyze_raft.clj         # RAFT state analysis
    └── cleanup_garbage.clj      # Index garbage collection
```

## Typical Workflow

A full ledger rebuild typically involves these steps:

### Step 1: Compress Blocks (Optional)

If you want to consolidate blocks and remove cancelled assertions/retractions:

```bash
clojure -M dev/ledger_toolkit/compress_blocks.clj \
  --source-base-path /path/to/original \
  --output-base-path /path/to/compressed \
  --network mynetwork \
  --dbid mydb \
  --flakes-per-block 50000
```

This reads blocks from the source, consolidates them based on the flakes-per-block threshold, and writes new block files to the output path. Assertion+retraction pairs within the same output block are eliminated.

### Step 2: Rebuild Indexes

Rebuild indexes from block files (either original or compressed):

```bash
clojure -M dev/ledger_toolkit/rebuild/run_rebuild.clj \
  --base-path /path/to/data \
  --network mynetwork \
  --dbid mydb \
  --max-mb 200
```

Options:
- `--max-mb`: Memory threshold before flushing indexes to disk (default: 200)
- `--parallelism`: Parallel processing threads (default: 4)
- `--no-validate`: Skip block sequence validation

### Step 3: Verify

Verify the rebuilt database is queryable:

```bash
clojure -M dev/ledger_toolkit/rebuild/run_verify.clj \
  --base-path /path/to/data \
  --network mynetwork \
  --dbid mydb
```

### Full Workflow (Combined)

For a complete rebuild including RAFT initialization:

```bash
clojure -M dev/ledger_toolkit/rebuild/full_rebuild.clj \
  --base-path /path/to/output \
  --network mynetwork \
  --dbid mydb \
  --blocks-source-path /path/to/source/ledger/mynetwork/mydb/block
```

## REPL Usage

### Compress Blocks

```clojure
(load-file "dev/ledger_toolkit/compress_blocks.clj")

(ledger-toolkit.compress-blocks/compress!
  {:source-base-path "/path/to/original"
   :output-base-path "/path/to/compressed"
   :network "mynetwork"
   :dbid "mydb"
   :flakes-per-block 50000})
```

### Rebuild Indexes

```clojure
(load-file "dev/ledger_toolkit/rebuild/index_rebuild.clj")

(rebuild.index-rebuild/rebuild!
  {:base-path "/path/to/data"
   :network "mynetwork"
   :dbid "mydb"
   :max-mb 200
   :parallelism 4})
```

### Register Ledger in RAFT

After rebuilding, register the ledger in RAFT state:

```clojure
(load-file "dev/ledger_toolkit/rebuild/register_ledger_in_raft.clj")

(rebuild.register-ledger-in-raft/register-ledger!
  {:base-path "/path/to/data"
   :network "mynetwork"
   :dbid "mydb"})
```

### Analyze RAFT State

For debugging RAFT issues:

```clojure
(load-file "dev/ledger_toolkit/rebuild/analyze_raft.clj")

(rebuild.analyze-raft/analyze-group-directory "/path/to/data/group")
```

## Block Compression Details

The compression engine (`block_processor/`) works as follows:

1. **Reads blocks incrementally** from disk using `incremental_reader.clj`
2. **Tracks state** in `optimized_state.clj`:
   - `committed-facts`: Facts from previously written blocks
   - `current-batch-assertions`: Assertions being accumulated
   - `current-batch-retractions`: Retractions being accumulated
3. **Eliminates pairs**: If an assertion and its retraction both occur in the current batch, both are dropped
4. **Accumulates until threshold**: Collects flakes until reaching `flakes-per-block`
5. **Writes consolidated blocks** via `generator.clj`

Statistics are tracked including:
- Total source flakes vs output flakes
- Compression ratio
- Retractions eliminated (assertion+retraction pairs removed)

## Path Conventions

- `base-path`: Parent directory containing `ledger/` and `group/`
- Block files: `<base-path>/ledger/<network>/<dbid>/block/`
- Index files: `<base-path>/ledger/<network>/<dbid>/index/`
- Root files: `<base-path>/ledger/<network>/<dbid>/root/`
- RAFT state: `<base-path>/group/`

## Testing

Generate test data for validation:

```bash
# Small test dataset (7 blocks)
clojure -M dev/ledger_toolkit/rebuild/create_test_data.clj \
  --base-path /tmp/fluree-test \
  --network test \
  --dbid small

# Large test dataset (100+ blocks)
clojure -M dev/ledger_toolkit/rebuild/create_large_test_data.clj \
  --base-path /tmp/fluree-test \
  --network test \
  --dbid large
```

See `rebuild/TESTING_GUIDE.md` for detailed testing instructions.
