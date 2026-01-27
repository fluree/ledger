# Index Rebuild Tool

This tool rebuilds Fluree ledger indexes from raw block files, with configurable memory thresholds to avoid excessive memory usage.

## Files

- `index_rebuild.clj` - Main rebuild logic with configurable thresholds
- `run_rebuild.clj` - Command-line runner script
- `test_rebuild.clj` - Testing and validation utilities

## Usage

### From REPL

```clojure
;; Load the namespace
(load-file "dev/ledger_toolkit/rebuild/index_rebuild.clj")

;; Basic rebuild with default 200MB threshold
(rebuild.index-rebuild/rebuild!
  {:base-path "/path/to/data"
   :network "mynetwork"
   :dbid "mydb"})

;; With custom settings
(rebuild.index-rebuild/rebuild!
  {:base-path "/path/to/data"
   :network "mynetwork"
   :dbid "mydb"
   :max-mb 500           ; 500MB threshold
   :parallelism 8        ; More parallel processing
   :validate true})      ; Validate block sequence
```

### From Command Line

```bash
# Basic usage
clojure -M dev/ledger_toolkit/rebuild/run_rebuild.clj \
  --base-path /path/to/data \
  --network mynetwork \
  --dbid mydb

# With options
clojure -M dev/ledger_toolkit/rebuild/run_rebuild.clj \
  --base-path /path/to/data \
  --network mynetwork \
  --dbid mydb \
  --max-mb 500 \
  --parallelism 8 \
  --no-validate

# Show help
clojure -M dev/ledger_toolkit/rebuild/run_rebuild.clj --help
```

### Testing/Validation

```clojure
;; Load test utilities
(load-file "dev/ledger_toolkit/rebuild/test_rebuild.clj")

;; Test rebuild
(rebuild.test-rebuild/test-rebuild "/path/to/data" "mynetwork" "mydb")

;; Just validate without rebuilding
(rebuild.test-rebuild/dry-run "/path/to/data" "mynetwork" "mydb")
```

## Configuration Options

- **base-path** (required): Root directory containing block files
  - Expected structure: `<base-path>/ledger/<network>/<dbid>/block/`
- **network** (required): Network name
- **dbid** (required): Database ID
- **max-mb**: Maximum MB of novelty before triggering index flush (default: 200)
- **parallelism**: Number of parallel threads for processing (default: 4)
- **validate**: Whether to validate block sequence for gaps (default: true)
- **use-file-threshold**: Use file size threshold instead of novelty bytes (experimental)

## How It Works

1. **Configuration**: Sets up a Fluree server with file storage pointing to your block directory
2. **Validation**: Checks that all block files exist without gaps in the sequence
3. **Reindexing**: Uses Fluree's built-in `reindex` function with configured novelty thresholds
4. **Memory Management**: When novelty (accumulated changes) exceeds threshold, indexes are flushed to disk
5. **Completion**: Ensures the ledger is queryable after rebuild

## Memory Tuning

The tool uses two thresholds:
- **fdb-memory-reindex**: Minimum novelty before considering an index (80% of max-mb)
- **fdb-memory-reindex-max**: Maximum novelty before forcing an index (100% of max-mb)

For a 200MB threshold:
- Minimum: ~160MB
- Maximum: 200MB

Adjust `--max-mb` based on your available memory and block file sizes.

## Troubleshooting

### Nil Value Errors
If you encounter errors about nil values in blocks:
- This is a known issue with some block files
- Wait for regenerated blocks without nil values and proper sort order

### Missing Blocks
The tool will detect and report any gaps in block numbering:
```
Error: Missing block files detected
Missing blocks: [5, 12, 23]
```

### Memory Issues
If running out of memory:
- Reduce `--max-mb` to trigger more frequent index flushes
- Reduce `--parallelism` to limit concurrent operations

## Output

The rebuilt ledger will have:
- Index files in: `<base-path>/ledger/<network>/<dbid>/index/`
- Root files in: `<base-path>/ledger/<network>/<dbid>/root/`
- Group state updated for immediate server loading

## Example Output

```
[2024-01-10T15:30:00] === Starting Index Rebuild ===
[2024-01-10T15:30:00] Base path: /path/to/data
[2024-01-10T15:30:00] Network: mynetwork, DB: mydb
[2024-01-10T15:30:00] Max MB per index: 200
[2024-01-10T15:30:00] Found 1000 block files
[2024-01-10T15:30:00] Total size: 1500.00 MB
[2024-01-10T15:30:01] Starting built-in reindex with max novelty 200 MB
[2024-01-10T15:31:45] Built-in reindex completed
[2024-01-10T15:31:45] === Rebuild Complete ===
```
