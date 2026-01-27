#!/bin/bash
# Start a test Fluree server

DATA_DIR="${1:-/tmp/fluree-test-data}"
PORT="${2:-8090}"

echo "Starting Fluree server..."
echo "Data directory: $DATA_DIR"
echo "API Port: $PORT"

# Set environment variables
export fdb_storage_file_root="$DATA_DIR"
export fdb_api_port="$PORT"
export fdb_mode="dev"
export fdb_group_this_server="testserver"
export fdb_group_servers="testserver@localhost:11001"
export fdb_group_port="11001"
export fdb_memory_reindex="5mb"
export fdb_memory_reindex_max="10mb"

# Run the server
java -jar target/fluree-ledger.standalone.jar
