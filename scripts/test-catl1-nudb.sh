#!/bin/bash -u
set -ex

# Build the project
ninja -C build src/utils-v1/catl1-to-nudb

# Parameters for testing
INPUT_FILE=${INPUT_FILE:-"$HOME/projects/xahau-history/cat.4500000-5000000.compression-0.catl"}
END_LEDGER=${END_LEDGER:-5000000}
LOG_LEVEL=${LOG_LEVEL:-"info"}
NUDB_PATH=${NUDB_PATH:-"./test-nudb"}

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

# Check if we're in test snapshot mode
if [ -n "${TEST_SNAPSHOTS:-}" ]; then
  echo "🧪 Running in snapshot test mode..."
  echo "This will test memory usage of snapshots without the pipeline."
  if [ -n "${LLDB:-}" ]; then
    echo "Running under lldb..."
    lldb -o run -- "$CMD" --input "$INPUT_FILE" --test-snapshots --end-ledger $END_LEDGER --log-level "$LOG_LEVEL"
  else
    "$CMD" --input "$INPUT_FILE" --test-snapshots --end-ledger $END_LEDGER --log-level "$LOG_LEVEL"
  fi
# Normal pipeline mode
else
  # Run with lldb if LLDB env var is set
  if [ -n "${LLDB:-}" ]; then
    echo "Running under lldb..."
    lldb -o run -- "$CMD" --input "$INPUT_FILE" --nudb-path "$NUDB_PATH" --end-ledger $END_LEDGER --log-level "$LOG_LEVEL"
  else
    "$CMD" --input "$INPUT_FILE" --nudb-path "$NUDB_PATH" --end-ledger $END_LEDGER --log-level "$LOG_LEVEL"
  fi
fi
