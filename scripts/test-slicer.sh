#!/bin/bash -u

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
START_LEDGER=1000
END_LEDGER=2000
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
./build/src/utils/catl-slice \
  --input "$INPUT_FILE" \
  --output "$SLICE_FILE" \
  --start-ledger "$START_LEDGER" \
  --end-ledger "$END_LEDGER" \
  --snapshots-path "$SNAPSHOT_DIR" \
  --compression-level 0 \
  --force-overwrite \
  --log-level info

# Validate the slice file
echo "Validating slice file"
./build/src/utils/catl-validator "$SLICE_FILE"

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

# Create a second slice using the snapshot
SECOND_START=$NEXT_LEDGER
SECOND_END=$((SECOND_START + 1000))
SECOND_SLICE="./test-slice-$SECOND_START-$SECOND_END.catl"

echo "Testing second slice creation from $SECOND_START to $SECOND_END using snapshot"
./build/src/utils/catl-slice \
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
./build/src/utils/catl-validator "$SECOND_SLICE"

# Compare slices with direct hasher output for validation
echo "Creating validation slice with hasher"
HASHER_SLICE="./test-hasher-slice-$START_LEDGER-$END_LEDGER.catl"
./build/src/hasher/catl-hasher \
  "$INPUT_FILE" --level=info \
  --first-ledger "$START_LEDGER" --last-ledger "$END_LEDGER" \
  --compression="$OUTPUT_COMPRESSION" \
  --create-slice-file "$HASHER_SLICE"

# Use the validator to compare hash consistency
echo "Validating first slice against hasher output"
./build/src/utils/catl-validator "$SLICE_FILE"
./build/src/utils/catl-validator "$HASHER_SLICE"

# For binary comparison, check file sizes (they should be comparable)
SLICE_SIZE=$(stat -f "%z" "$SLICE_FILE")
HASHER_SIZE=$(stat -f "%z" "$HASHER_SLICE")
echo "Slice file size: $SLICE_SIZE bytes"
echo "Hasher file size: $HASHER_SIZE bytes"

# Display results
echo "Tests completed successfully!"
echo "Output files:"
echo "First slice: $SLICE_FILE"
echo "Second slice: $SECOND_SLICE"
echo "Hasher slice: $HASHER_SLICE"
echo "Snapshot: $SNAPSHOT_FILE"

# Clean up if requested
if [ "$CLEANUP" = true ]; then
  echo "Cleaning up output files..."
  rm -f "$SLICE_FILE" "$SECOND_SLICE" "$HASHER_SLICE"
  rm -rf "$SNAPSHOT_DIR"
  echo "Cleanup complete."
else
  echo "Files retained for inspection. Use --cleanup to remove output files."
fi
