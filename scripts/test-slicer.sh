#!/bin/bash -u
set -ex

# Parse command line arguments
CLEANUP=false
while [[ $# -gt 0 ]]; do
  case $1 in
  --cleanup)
    CLEANUP=true
    shift
    ;;
  *)
    echo "Unknown option: $1"
    echo "Usage: $0 [--cleanup]"
    exit 1
    ;;
  esac
done

set -ex

# Build the project
pushd build
ninja
popd

# Parameters for testing
INPUT_FILE="/Users/nicholasdudfield/projects/xahau-history/cat.1-5000000"
START_LEDGER=1
SLICE_SIZE=10000
END_LEDGER=$SLICE_SIZE
SLICE_FILE="./test-slice-$START_LEDGER-$END_LEDGER.catl"
SNAPSHOT_DIR="./catl_snapshots"
OUTPUT_COMPRESSION="0" # Uncompressed for testing

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
  echo "❌ Error: Input file does not exist: $INPUT_FILE"
  echo "Please check the file path and try again."
  exit 1
fi

# Create a temporary directory for snapshots
mkdir -p "$SNAPSHOT_DIR"

echo "Testing slice creation from $START_LEDGER to $END_LEDGER"
./build/src/utils-v1/catl1-slice \
  --input "$INPUT_FILE" \
  --output "$SLICE_FILE" \
  --start-ledger "$START_LEDGER" \
  --end-ledger "$END_LEDGER" \
  --snapshots-path "$SNAPSHOT_DIR" \
  --compression-level "$OUTPUT_COMPRESSION" \
  --force-overwrite \
  --log-level info

## Validate the slice file
echo "Validating slice file"
./build/src/utils-v1/catl1-validator "$SLICE_FILE"
./build/src/hasher-v1/catl1-hasher \
  $SLICE_FILE --level=info

# Check if the snapshot was created
NEXT_LEDGER=$((END_LEDGER + 1))
SNAPSHOT_FILE="$SNAPSHOT_DIR/state_snapshot_for_ledger_$NEXT_LEDGER.dat.zst"
if [ -f "$SNAPSHOT_FILE" ]; then
  echo "✅ Snapshot created for ledger $NEXT_LEDGER: $SNAPSHOT_FILE"
  SNAPSHOT_SIZE=$(stat -f "%z" "$SNAPSHOT_FILE")
  echo "Snapshot size: $SNAPSHOT_SIZE bytes"
else
  echo "⚠️ Warning: No snapshot found for ledger $NEXT_LEDGER"
  exit 1
fi

## Create a second slice using the snapshot
SECOND_START=$NEXT_LEDGER
SECOND_END=$((SECOND_START + $SLICE_SIZE - 1))
SECOND_SLICE="./test-slice-$SECOND_START-$SECOND_END.catl"
#
echo "Testing second slice creation from $SECOND_START to $SECOND_END using snapshot"
./build/src/utils-v1/catl1-slice \
  --input "$INPUT_FILE" \
  --output "$SECOND_SLICE" \
  --start-ledger "$SECOND_START" \
  --end-ledger "$SECOND_END" \
  --snapshots-path "$SNAPSHOT_DIR" \
  --compression-level "$OUTPUT_COMPRESSION" \
  --force-overwrite \
  --log-level info

# Validate the second slice file
echo "Validating second slice file"
./build/src/utils-v1/catl1-validator "$SECOND_SLICE"

./build/src/hasher-v1/catl1-hasher \
  $SECOND_SLICE --level=info
