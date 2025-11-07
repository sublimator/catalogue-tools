#!/bin/bash -u
set -ex

# Build the project
ninja -C build src/utils-v1/catl1-to-nudb

# Parameters for testing
INPUT_FILE=${INPUT_FILE:-"$HOME/projects/xahau-history/cat.4500000-5000000.compression-0.catl"}
END_LEDGER=${END_LEDGER:-5000000}
LOG_LEVEL=${LOG_LEVEL:-"info"}
NUDB_PATH=${NUDB_PATH:-"./test-nudb"}
NUDB_FACTOR=${NUDB_FACTOR:-""} # NuDB load factor (0.0-1.0) - lower = faster, higher = more space efficient. Default: 0.5
NUDB_MOCK=${NUDB_MOCK:-""}     # Mock mode: "noop"/"memory" (no I/O), "disk" (buffered append-only file)
# NOTE: Single-threaded (1) often performs better than multi-threaded due to overhead
HASHER_THREADS=${HASHER_THREADS:-1}                    # Default: 1 thread (best performance, avoids thread coordination overhead)
COMPRESSOR_THREADS=${COMPRESSOR_THREADS:-2}            # Default: 2 threads (compression worker threads)
VERIFY_KEYS=${VERIFY_KEYS:-""}                         # Set to any value to enable key verification after import
DEDUPE_STRATEGY=${DEDUPE_STRATEGY:-"cuckoo-rocks"}     # Deduplication strategy: none, cuckoo-rocks (default), nudb, memory-full, memory-xxhash
USE_DEDUPE_THREAD=${USE_DEDUPE_THREAD:-""}             # Set to any value to run deduplication in a separate parallel thread
DASHBOARD=${DASHBOARD:-""}                             # Set to any value to enable real-time FTXUI dashboard
ENABLE_DEBUG_PARTITIONS=${ENABLE_DEBUG_PARTITIONS:-""} # Set to any value to enable debug log partitions
WALK_NODES_LEDGER=${WALK_NODES_LEDGER:-""}             # Set to a ledger number to enable WALK_NODES logging for that specific ledger
WALK_NODES_DEBUG_KEY=${WALK_NODES_DEBUG_KEY:-""}       # Set to a hex key prefix to print detailed info for matching keys

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
  echo "❌ Error: Input file does not exist: $INPUT_FILE"
  echo "Please check the file path and try again."
  exit 1
fi

# Run with actual input file - process first $END_LEDGER ledgers
echo "Testing catl1-to-nudb tool - processing first $END_LEDGER ledgers"

# Prepare command
CMD="./build/src/utils-v1/catl1-to-nudb"

# Build debug partitions flag if enabled
DEBUG_FLAG=""
if [ -n "$ENABLE_DEBUG_PARTITIONS" ]; then
  echo "🐞 Debug log partitions ENABLED (MAP_OPS, WALK_NODES, VERSION_TRACK, PIPE_VERSION)"
  DEBUG_FLAG="--enable-debug-partitions"
fi

# Build walk-nodes-ledger flag if specified
WALK_NODES_FLAG=""
if [ -n "$WALK_NODES_LEDGER" ]; then
  echo "🔍 WALK_NODES logging enabled for ledger: $WALK_NODES_LEDGER"
  WALK_NODES_FLAG="--walk-nodes-ledger $WALK_NODES_LEDGER"
fi

# Build walk-nodes-debug-key flag if specified
WALK_NODES_DEBUG_KEY_FLAG=""
if [ -n "$WALK_NODES_DEBUG_KEY" ]; then
  echo "🔍 WALK_NODES debug key enabled: $WALK_NODES_DEBUG_KEY"
  WALK_NODES_DEBUG_KEY_FLAG="--walk-nodes-debug-key $WALK_NODES_DEBUG_KEY"
fi

# Build nudb-factor flag if specified
NUDB_FACTOR_FLAG=""
if [ -n "$NUDB_FACTOR" ]; then
  echo "⚙️  NuDB load factor: $NUDB_FACTOR"
  NUDB_FACTOR_FLAG="--nudb-factor $NUDB_FACTOR"
fi

# Build nudb-mock flag if specified
NUDB_MOCK_FLAG=""
if [ -n "$NUDB_MOCK" ]; then
  echo "🧪 NuDB mock mode: $NUDB_MOCK"
  NUDB_MOCK_FLAG="--nudb-mock $NUDB_MOCK"
fi

# Build verify-keys flag if specified
VERIFY_KEYS_FLAG=""
if [ -n "$VERIFY_KEYS" ]; then
  echo "🔑 Key verification enabled"
  VERIFY_KEYS_FLAG="--verify-keys"
fi

# Build dedupe-strategy flag
DEDUPE_STRATEGY_FLAG="--dedupe-strategy $DEDUPE_STRATEGY"
echo "📊 Deduplication strategy: $DEDUPE_STRATEGY"

# Build use-dedupe-thread flag if specified
USE_DEDUPE_THREAD_FLAG=""
if [ -n "$USE_DEDUPE_THREAD" ]; then
  echo "🔀 Parallel dedupe thread ENABLED (RocksDB I/O runs in separate thread)"
  USE_DEDUPE_THREAD_FLAG="--use-dedupe-thread"
fi

# Build dashboard flag if specified
DASHBOARD_FLAG=""
if [ -n "$DASHBOARD" ]; then
  echo "🎨 Real-time dashboard ENABLED (press 'q' to quit)"
  DASHBOARD_FLAG="--dashboard"
fi

# Check if we're in test snapshot mode
if [ -n "${TEST_SNAPSHOTS:-}" ]; then
  echo "🧪 Running in snapshot test mode..."
  echo "This will test memory usage of snapshots without the pipeline."
  if [ -n "${LLDB:-}" ]; then
    echo "Running under lldb..."
    lldb -o run -- "$CMD" --input "$INPUT_FILE" --test-snapshots --end-ledger $END_LEDGER --log-level "$LOG_LEVEL" --hasher-threads $HASHER_THREADS --compressor-threads $COMPRESSOR_THREADS $DEBUG_FLAG $WALK_NODES_FLAG $WALK_NODES_DEBUG_KEY_FLAG $NUDB_FACTOR_FLAG $NUDB_MOCK_FLAG $VERIFY_KEYS_FLAG $DEDUPE_STRATEGY_FLAG $USE_DEDUPE_THREAD_FLAG $DASHBOARD_FLAG
  else
    "$CMD" --input "$INPUT_FILE" --test-snapshots --end-ledger $END_LEDGER --log-level "$LOG_LEVEL" --hasher-threads $HASHER_THREADS --compressor-threads $COMPRESSOR_THREADS $DEBUG_FLAG $WALK_NODES_FLAG $WALK_NODES_DEBUG_KEY_FLAG $NUDB_FACTOR_FLAG $NUDB_MOCK_FLAG $VERIFY_KEYS_FLAG $DEDUPE_STRATEGY_FLAG $USE_DEDUPE_THREAD_FLAG $DASHBOARD_FLAG
  fi
# Normal pipeline mode
else
  echo "🚀 Running with $HASHER_THREADS hasher threads and $COMPRESSOR_THREADS compressor threads..."
  # Run with lldb if LLDB env var is set
  if [ -n "${LLDB:-}" ]; then
    echo "Running under lldb..."
    lldb -o run -- "$CMD" --input "$INPUT_FILE" --nudb-path "$NUDB_PATH" --end-ledger $END_LEDGER --log-level "$LOG_LEVEL" --hasher-threads $HASHER_THREADS --compressor-threads $COMPRESSOR_THREADS $DEBUG_FLAG $WALK_NODES_FLAG $WALK_NODES_DEBUG_KEY_FLAG $NUDB_FACTOR_FLAG $NUDB_MOCK_FLAG $VERIFY_KEYS_FLAG $DEDUPE_STRATEGY_FLAG $USE_DEDUPE_THREAD_FLAG $DASHBOARD_FLAG
  else
    "$CMD" --input "$INPUT_FILE" --nudb-path "$NUDB_PATH" --end-ledger $END_LEDGER --log-level "$LOG_LEVEL" --hasher-threads $HASHER_THREADS --compressor-threads $COMPRESSOR_THREADS $DEBUG_FLAG $WALK_NODES_FLAG $WALK_NODES_DEBUG_KEY_FLAG $NUDB_FACTOR_FLAG $NUDB_MOCK_FLAG $VERIFY_KEYS_FLAG $DEDUPE_STRATEGY_FLAG $USE_DEDUPE_THREAD_FLAG $DASHBOARD_FLAG
  fi
fi
